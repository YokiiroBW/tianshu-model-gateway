"""Durable, bounded, single-owner JSONL runtime-event files.

One process owns one set of segment files. Segment names embed the process-unique
``instance_id`` and are created with an exclusive create, so a second process can never
append into the first process's file and a restart can never rewrite an earlier sequence.

Everything here is a *side channel*: a failure to persist a record never rolls back a
receipt, never aborts bytes already delivered to a caller, never changes an ``unknown``
outcome into a retryable one and never triggers an upstream retry. What a failure does do is
turn the process's logging state to ``unavailable``, which is visible to readiness and makes
new business refuse before any upstream side effect.

The sink never deletes a file: this batch has no collection confirmation, so a sealed segment
stays exactly as written. When the configured directory budget is reached the sink refuses
the *new* record instead of overwriting an old one.
"""

import asyncio
import os
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path

from . import events

# Fixed, contract-level bounds. They are not deployment knobs: the frozen package states a
# segment size and a bounded in-memory hand-off, so the product does not let an operator
# raise them.
SEGMENT_BYTES = 64 * 1024 * 1024
QUEUE_ENTRIES = 1024
QUEUE_BYTES = 8 * 1024 * 1024
MIN_DIRECTORY_BYTES = 32 * 1024 * 1024
MAX_DIRECTORY_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_DIRECTORY_BYTES = 1024 * 1024 * 1024
MAX_SEGMENTS = 100000
SEGMENT_PREFIX = "gateway-"
SEGMENT_SUFFIX = ".jsonl"
SHUTDOWN_TIMEOUT_SECONDS = 2.0
BATCH_LIMIT = 256

STATE_OK = "ok"
STATE_NON_DURABLE = "non_durable"
STATE_UNAVAILABLE = "unavailable"

REASON_IO = "log_unavailable"
REASON_CAPACITY = "log_capacity_exceeded"

STATE_FOR_CHECK = {
    STATE_OK: "ok",
    STATE_NON_DURABLE: "non_durable",
    STATE_UNAVAILABLE: "failed",
}


class RuntimeIdentity:
    """The one allocator of instance, sequence and event identity.

    ``instance_id`` is per process. ``sequence`` is allocated under a real lock and is
    therefore unique and monotonic even if a caller records from more than one thread, and
    ``event_id`` is an independent UUID so a record can be reconciled without inventing an
    end-to-end trace. A re-transmitted record keeps all three values.
    """

    def __init__(self, instance_id=None, clock=time.monotonic):
        self.instance_id = str(instance_id or uuid.uuid4())
        self.clock = clock
        self._sequence = 0
        self._lock = threading.Lock()

    def next_sequence(self):
        with self._lock:
            self._sequence += 1
            return self._sequence

    def record(self, name, *, outcome=None, correlation_id=None, duration_ms=None, error_code=None):
        """Build one closed record with freshly allocated identity."""
        return events.build_record(
            name,
            instance_id=self.instance_id,
            sequence=self.next_sequence(),
            event_id=str(uuid.uuid4()),
            outcome=outcome,
            correlation_id=correlation_id,
            duration_ms=duration_ms,
            error_code=error_code,
        )

    def elapsed_ms(self, started, finished=None):
        return events.duration_ms(started, finished, self.clock)


class _Item:
    """One queued line and, for a durable record, the waiter that must observe its fsync."""

    __slots__ = ("line", "ack", "persisted")

    def __init__(self, line, ack=None):
        self.line = line
        self.ack = ack
        self.persisted = False


_CLOSE = object()


class RuntimeLog:
    """Bounded queue in front of one durable file writer.

    ``directory`` is an explicit absolute path. ``directory=None`` selects the development
    fallback: records go to stderr and the logging check reports ``non_durable``, which can
    never be ready.
    """

    def __init__(
        self,
        directory,
        *,
        instance_id=None,
        max_directory_bytes=DEFAULT_DIRECTORY_BYTES,
        clock=time.monotonic,
        stderr=None,
    ):
        self.directory = None if directory is None else str(Path(directory))
        self.identity = RuntimeIdentity(instance_id, clock)
        self.clock = clock
        self.max_directory_bytes = max_directory_bytes
        self.stderr = stderr if stderr is not None else sys.stderr
        self._state = STATE_NON_DURABLE if directory is None else STATE_OK
        self._reason = None
        self._warned = False
        self._handle = None
        self._segment_index = 0
        self._segment_bytes = 0
        self._directory_bytes = 0
        self._queue = deque()
        self._queued_bytes = 0
        self._signal = None
        self._writer = None
        self._closing = False
        self._closed = False
        self._dropped = 0
        self._overflowed = 0
        self._written = 0

    # -- assembly -----------------------------------------------------------------------

    @property
    def instance_id(self):
        return self.identity.instance_id

    def open(self):
        """Create the directory and the first segment. Never raises for an unusable path."""
        if self.directory is None:
            self.submit("log.non_durable")
            return self
        try:
            os.makedirs(self.directory, exist_ok=True)
            self._directory_bytes = self._scan_directory()
            self._open_segment()
        except OSError:
            self._degrade(REASON_IO)
        return self

    async def start(self):
        """Start the writer and hand it whatever was queued before the event loop existed."""
        if self._closed or self.directory is None:
            return self
        if self._signal is None:
            self._signal = asyncio.Event()
        if self._writer is None:
            self._writer = asyncio.create_task(self._run())
        self._signal.set()
        return self

    async def shutdown(self, timeout=SHUTDOWN_TIMEOUT_SECONDS):
        """Bounded drain: a stuck disk must never hold the process open indefinitely.

        Whatever could not be persisted stays a detectable hole: its sequence number was
        already allocated, so the missing line is visible in the numbering rather than being
        silently renumbered or overwritten.
        """
        if self._closed:
            return
        self._closed = True
        if self._writer is not None:
            self._queue.append(_CLOSE)
            self._signal.set()
            try:
                async with asyncio.timeout(timeout):
                    await asyncio.shield(self._writer)
            except (TimeoutError, asyncio.CancelledError):
                self._writer.cancel()
            self._writer = None
        self._close_handle()

    def close_sync(self):
        """Deliver the queue and close the handle in the calling thread.

        Used on the start-up path, before an event loop exists, and on a failure path where an
        awaiting task may already be gone.
        """
        if self._closed:
            return
        self._closed = True
        if self.directory is not None:
            self._deliver(self._drain_queue())
            self._sync()
            self._close_handle()

    def _close_handle(self):
        if self._handle is None:
            return
        handle, self._handle = self._handle, None
        try:
            handle.flush()
            os.fsync(handle.fileno())
        except OSError:
            pass
        finally:
            try:
                handle.close()
            except OSError:
                pass

    def _scan_directory(self):
        try:
            entries = list(Path(self.directory).iterdir())
        except OSError:
            # An unreadable directory is not an empty one: the previous reading is kept rather
            # than telling the caller the budget is free.
            return self._directory_bytes
        total = 0
        for entry in entries:
            try:
                if entry.is_file():
                    total += entry.stat().st_size
            except OSError:
                continue
        return total

    def _open_segment(self):
        while True:
            path = Path(self.directory) / (
                f"{SEGMENT_PREFIX}{self.identity.instance_id}-{self._segment_index:06d}"
                f"{SEGMENT_SUFFIX}"
            )
            try:
                # Exclusive create: this process never appends into a file another process
                # may own, so one logical record can never be written twice.
                self._handle = open(path, "xb")
            except FileExistsError:
                self._segment_index += 1
                if self._segment_index > MAX_SEGMENTS:
                    raise OSError("no free log segment name") from None
                continue
            self._segment_bytes = 0
            return

    # -- record submission --------------------------------------------------------------

    def record(self, name, **fields):
        return self.identity.record(name, **fields)

    def encode(self, name, **fields):
        return events.encode_record(self.record(name, **fields))

    def submit(self, name, **fields):
        """Best-effort record. Returns whether it was accepted for persistence."""
        try:
            line = self.encode(name, **fields)
        except ValueError:
            return False
        return self._handoff(line)

    def submit_record(self, record):
        """Queue a record that already exists, keeping its instance/sequence/event identity.

        This is the re-transmission path: a collected record is written unchanged, so a retry
        can never masquerade as a new fact.
        """
        return self._handoff(events.encode_record(events.validate_record(record)))

    async def submit_durable(self, name, **fields):
        """Record and wait for flush+fsync. ``False`` means the caller must not proceed.

        The await happens with no scheduler lock, no database transaction and no forward write
        held, so a slow disk delays this attempt only.
        """
        try:
            line = self.encode(name, **fields)
        except ValueError:
            return False
        if self.directory is None:
            self._emergency(line)
            return True
        if self._closed:
            return False
        ack = asyncio.get_running_loop().create_future()
        if self._handoff(line, ack) is None:
            return False
        return await ack

    def _handoff(self, line, ack=None):
        """Accept one line into the bounded hand-off, or refuse it visibly.

        Nothing is evicted to make room: a refused record is *not* written and the process
        stops claiming to log in full, which is what the caller observes.
        """
        if self.directory is None:
            self._emergency(line)
            return True
        if self._closed:
            return None
        if len(self._queue) >= QUEUE_ENTRIES or self._queued_bytes + len(line) > QUEUE_BYTES:
            self._overflowed += 1
            self._dropped += 1
            self._degrade(REASON_IO)
            return None
        item = _Item(line, ack)
        self._queue.append(item)
        self._queued_bytes += len(line)
        if self._signal is not None:
            self._signal.set()
        return item

    # -- writer -------------------------------------------------------------------------

    def _drain_queue(self, limit=None):
        items = []
        while self._queue and (limit is None or len(items) < limit):
            item = self._queue.popleft()
            if item is _CLOSE:
                self._closing = True
                continue
            self._queued_bytes -= len(item.line)
            items.append(item)
        return items

    async def _run(self):
        while True:
            await self._signal.wait()
            self._signal.clear()
            while self._queue:
                items = self._drain_queue(BATCH_LIMIT)
                if not items:
                    continue
                self._deliver(items)
                if not self._sync():
                    for item in items:
                        item.persisted = False
                for item in items:
                    if item.ack is not None and not item.ack.done():
                        item.ack.set_result(item.persisted)
            if self._closing:
                return

    def _deliver(self, items):
        for item in items:
            if self._state == STATE_UNAVAILABLE and self._reason == REASON_CAPACITY:
                # A capacity refusal is a policy decision, not an IO error: the record is not
                # attempted, so the budget can never be exceeded and no old file is touched.
                self._dropped += 1
                continue
            item.persisted = self._write(item.line)

    def _write(self, line):
        """Write one line, sealing a full segment first. Never raises, never overwrites."""
        try:
            if self._handle is None and not self._reopen_segment():
                return False
            if self._segment_bytes and self._segment_bytes + len(line) > SEGMENT_BYTES:
                seal = self._seal_segment()
                if seal is None:
                    return False
                self._append(seal)
            if self._directory_bytes + len(line) > self.max_directory_bytes:
                self._degrade(REASON_CAPACITY)
                self._dropped += 1
                return False
            self._append(line)
        except OSError:
            self._degrade(REASON_IO)
            self._dropped += 1
            return False
        self._written += 1
        if self._state == STATE_UNAVAILABLE:
            # A real, successful write is the only proof of recovery; a readiness probe
            # cannot produce one because it is read-only.
            self._recover()
        return True

    def _reopen_segment(self):
        """Try to obtain a fresh segment after an earlier one failed to open."""
        try:
            self._open_segment()
            return True
        except OSError:
            return False

    def _append(self, line):
        self._handle.write(line)
        self._segment_bytes += len(line)
        self._directory_bytes += len(line)

    def _seal_segment(self):
        """Close the full segment, start the next and return the seal record for it."""
        handle, self._handle = self._handle, None
        if handle is None:
            return None
        try:
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
            self._segment_index += 1
            self._open_segment()
            return events.encode_record(self.record("log.segment_sealed"))
        except OSError:
            self._degrade(REASON_IO)
            return None

    def _sync(self):
        """flush+fsync the current segment. A failure invalidates the batch just written."""
        if self._handle is None:
            return False
        try:
            self._handle.flush()
            os.fsync(self._handle.fileno())
            return True
        except OSError:
            self._degrade(REASON_IO)
            return False

    # -- degradation and recovery -------------------------------------------------------

    def _degrade(self, reason):
        """Enter the unavailable state and raise exactly one fixed emergency line.

        The failed file cannot carry the news of its own failure, so the record goes to the
        emergency channel, once per process. Nothing recurses: a failure while reporting a
        failure is swallowed.
        """
        self._reason = reason
        if self._state == STATE_UNAVAILABLE:
            return
        self._state = STATE_UNAVAILABLE
        if self._warned:
            return
        self._warned = True
        name = "log.capacity_exceeded" if reason == REASON_CAPACITY else "log.unavailable"
        try:
            line = events.encode_record(self.record(name, error_code=reason))
        except ValueError:  # pragma: no cover - both events are registered above
            return
        self._emergency(line)

    def _recover(self):
        line = events.encode_record(self.record("log.recovered"))
        if self._write_quiet(line):
            self._state = STATE_OK
            self._reason = None

    def _write_quiet(self, line):
        try:
            if self._handle is None:
                return False
            self._append(line)
            return True
        except OSError:
            return False

    def _emergency(self, line):
        """The single safe emergency channel: always available, never the failed file.

        Two callers use it and no other channel exists: :meth:`_degrade` writes the one fixed
        record that announces its own failure, and the non-durable mode writes the records
        themselves because there is no configured file to write them to. A write that fails
        here is swallowed: a failure while reporting a failure must never recurse.
        """
        try:
            self.stderr.write(line.decode("utf-8"))
            self.stderr.flush()
        except Exception:
            return

    def probe_write(self):
        """Explicit maintenance action: prove the sink with a real, successful write.

        This is not reachable from a health probe. It writes one registered
        ``maintenance.log_recovery_check`` record, reports the result and, when it succeeds,
        leaves the sink in the healthy state.

        The directory is measured again first: an operator who freed space or repaired the mount
        expects the command to observe the filesystem as it is now, not the reading taken when
        the process started. Re-reading a directory is not a write, and nothing is deleted,
        truncated or overwritten to make room.
        """
        if self.directory is None:
            return False
        self._directory_bytes = self._scan_directory()
        ok = self._write(events.encode_record(self.record("maintenance.log_recovery_check")))
        if not ok:
            failed = self.record(
                "maintenance.log_recovery_check", outcome="failed", error_code=REASON_IO
            )
            self._emergency(events.encode_record(failed))
            return False
        return self._sync()

    # -- read-only inspection -----------------------------------------------------------

    @property
    def state(self):
        return self._state

    @property
    def reason(self):
        return self._reason

    def check_status(self):
        return STATE_FOR_CHECK[self._state]

    def accepts_new_work(self):
        """Whether a new business side effect may be attempted at all."""
        return self._state != STATE_UNAVAILABLE

    def stats(self):
        return {
            "state": self._state,
            "reason": self._reason,
            "queued": len(self._queue),
            "queued_bytes": self._queued_bytes,
            "written": self._written,
            "dropped": self._dropped,
            "overflowed": self._overflowed,
            "segment_bytes": self._segment_bytes,
            "directory_bytes": self._directory_bytes,
            "max_directory_bytes": self.max_directory_bytes,
            "segments": self._segment_index + 1,
            "instance_id": self.identity.instance_id,
        }


def segment_paths(directory):
    """Every stored segment, in the order it was written."""
    return sorted(
        path
        for path in Path(directory).iterdir()
        if path.is_file() and path.name.endswith(SEGMENT_SUFFIX)
    )


def read_records(directory):
    """Decode every stored record. Used by verification and by the next batch's collector."""
    records = []
    for path in segment_paths(directory):
        with open(path, "rb") as handle:
            for raw in handle:
                if raw.strip():
                    records.append(events.decode_line(raw))
    return records


def sequence_gaps(records):
    """Sequences that are missing for one instance, so a loss is detectable, not silent."""
    seen = {}
    for record in records:
        seen.setdefault(record["instance_id"], set()).add(record["sequence"])
    gaps = {}
    for instance, sequences in seen.items():
        missing = sorted(set(range(1, max(sequences) + 1)) - sequences)
        if missing:
            gaps[instance] = missing
    return gaps
