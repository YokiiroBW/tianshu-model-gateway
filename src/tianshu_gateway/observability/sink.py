"""Durable, bounded, single-owner JSONL runtime-event files.

One process owns one set of segment files. Segment names embed the process-unique
``instance_id`` and are created with an exclusive create, so a second process can never
append into the first process's file and a restart can never rewrite an earlier sequence.

**Disk IO never runs on the event loop.** A dedicated owner thread holds the file handle and
performs every ``write``/``flush``/``fsync``/``close``; the loop only appends a line to a
bounded hand-off and awaits a completion signal. That is what keeps a slow disk from stalling
liveness, cancellation, queue deadlines and every other request, and it is why a durable
record can be awaited without holding a scheduler lock, a database transaction or a forward
write. The hand-off is bounded by count and by bytes; nothing is ever evicted to make room,
and no unbounded pool of tasks is created in place of the one owner.

**Every stored byte is metered by the same reservation.** A line, the ``log.segment_sealed``
record that opens the next segment and the ``log.recovered`` record that may follow it are all
counted *before* the first byte is written, against both the segment bound and the directory
budget. A record that does not fit is refused as a whole: the budget is never exceeded, an old
file is never deleted, truncated or overwritten to make room, and the sink reports itself
unusable instead of quietly writing past its own limit. Recovery to the healthy state is only
declared after the recovery record itself has been flushed and fsynced inside the budget.

Everything here is a *side channel*: a failure to persist a record never rolls back a receipt,
never aborts bytes already delivered to a caller, never changes an ``unknown`` outcome into a
retryable one and never triggers an upstream retry. What a failure does do is turn the
process's logging state to ``unavailable``, which is visible to readiness and makes new
business refuse before any upstream side effect.

The sink never deletes a file: this batch has no collection confirmation, so a sealed segment
stays exactly as written. When the configured directory budget is reached the sink refuses the
*new* record instead of overwriting an old one.
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
WRITER_NAME = "tianshu-log-writer"
# How often a bounded shutdown wait looks at the owner thread. The wait itself is bounded by
# the caller's deadline and yields to the loop on every step, so it never stalls liveness.
JOIN_POLL_SECONDS = 0.005
# A sequence wider than any sequence a process can reach, used only to measure the exact
# encoded length of a record that has not been allocated yet. Measuring with it makes the
# reservation an upper bound, so a reserved record can never be longer than what was reserved.
MEASURE_SEQUENCE = 10**18

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

    def measure(self, name, **fields):
        """Encoded length of one record without consuming identity.

        The measurement uses the widest sequence this process could ever hold, so the value is
        an upper bound of the real record and a reservation made with it is always sufficient.
        """
        return len(
            events.encode_record(
                events.build_record(
                    name,
                    instance_id=self.instance_id,
                    sequence=MEASURE_SEQUENCE,
                    event_id=str(uuid.uuid4()),
                    **fields,
                )
            )
        )

    def elapsed_ms(self, started, finished=None):
        return events.duration_ms(started, finished, self.clock)


class _Item:
    """One queued line and the caller that must observe its durable result.

    ``future`` is the event-loop waiter of a durable submission, ``ack`` the thread-side waiter
    of the synchronous maintenance action. At most one of them is set. A ``barrier`` item
    carries no record: it is resolved once every record queued before it is durable, which is
    how a caller (or a test) waits for the sink to catch up without guessing at timing.
    """

    __slots__ = ("line", "future", "ack", "maintenance", "barrier", "epoch", "persisted")

    def __init__(self, line, future=None, ack=None, maintenance=False, barrier=False, epoch=0):
        self.line = line
        self.future = future
        self.ack = ack
        self.maintenance = maintenance
        self.barrier = barrier
        # The degradation this item was submitted under. Only a record submitted at or after the
        # current degradation can prove that the sink works again: flushing a record that was
        # already queued when the disk failed says nothing about the disk now.
        self.epoch = epoch
        self.persisted = False


class _Ack:
    """A one-shot completion signal for a caller that is not on the event loop."""

    __slots__ = ("_event", "_result")

    def __init__(self):
        self._event = threading.Event()
        self._result = False

    def set(self, result):
        self._result = bool(result)
        self._event.set()

    def wait(self, timeout):
        return self._event.wait(timeout)

    @property
    def result(self):
        return self._result


def _resolve(future, result):
    if future is not None and not future.done():
        future.set_result(result)


class RuntimeLog:
    """Bounded hand-off in front of one durable file writer owned by one thread.

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
        self._condition = threading.Condition()
        self._lock = threading.Lock()
        self._pending_emergency = []
        self._thread = None
        self._loop = None
        self._in_flight = ()
        self._closing = False
        self._closed = False
        self._abandoned = False
        self._epoch = 0
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
        """Start the owner thread and hand it whatever was queued before the loop existed."""
        if self._closed:
            return self
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:  # pragma: no cover - start() is only awaited inside a loop
                self._loop = None
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name=WRITER_NAME, daemon=True)
            self._thread.start()
        return self

    async def shutdown(self, timeout=SHUTDOWN_TIMEOUT_SECONDS):
        """Bounded stop: a stuck disk must never hold the process open indefinitely.

        The wait is bounded by the caller's deadline and yields on every step, so it cannot
        stall the loop. A write that has not returned by the deadline is **not** cancelled and
        its result is unknown, so this method never touches the handle the owner thread still
        holds and never reports the write as failed; it refuses everything still queued so no
        caller is left waiting for an answer, and the owner thread stays the only writer.
        """
        if self._closed:
            return
        self._closed = True
        deadline = time.monotonic() + max(0.0, timeout)
        thread = self._thread
        if thread is not None:
            with self._condition:
                self._closing = True
                self._condition.notify_all()
            while thread.is_alive() and time.monotonic() < deadline:
                await asyncio.sleep(JOIN_POLL_SECONDS)
            if thread.is_alive():
                self._abandon()
                return
            self._thread = None
        else:
            self._commit(self._drain_queue())
            self._flush_emergency()
        self._close_handle()

    def close_sync(self):
        """Deliver the queue and close the handle in the calling thread.

        Used on the start-up path, before an event loop exists, and on a failure path where an
        awaiting task may already be gone. When the owner thread exists it is joined first: the
        calling thread never becomes a second writer.
        """
        if self._closed:
            return
        self._closed = True
        thread = self._thread
        if thread is not None:
            with self._condition:
                self._closing = True
                self._condition.notify_all()
            thread.join(SHUTDOWN_TIMEOUT_SECONDS)
            if thread.is_alive():
                self._abandon()
                return
            self._thread = None
        self._commit(self._drain_queue())
        self._flush_emergency()
        self._close_handle()

    def _abandon(self):
        """Refuse everything still queued after the deadline without touching the handle."""
        self._abandoned = True
        for item in tuple(self._in_flight) + tuple(self._drain_queue()):
            item.persisted = False
            self._settle(item, False)

    def _close_handle(self):
        if self._handle is None:
            return
        handle, self._handle = self._handle, None
        self._segment_bytes = 0
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
        return self._handoff(line) is not None

    def submit_record(self, record):
        """Queue a record that already exists, keeping its instance/sequence/event identity.

        This is the re-transmission path: a collected record is written unchanged, so a retry
        can never masquerade as a new fact.
        """
        return self._handoff(events.encode_record(events.validate_record(record))) is not None

    async def flush(self):
        """Wait until every record submitted before this call is on durable storage.

        This is a barrier, not a record: it writes nothing and allocates no identity. It is what
        makes "the probes stored nothing" and "the batch before shutdown is durable" checkable
        without guessing at timing.
        """
        if self.directory is None or self._closed:
            return False
        loop = asyncio.get_running_loop()
        self._loop = loop
        future = loop.create_future()
        if self._handoff(b"", future=future, barrier=True) is None:
            return False
        return await future

    async def submit_durable(self, name, **fields):
        """Record and wait for flush+fsync. ``False`` means the caller must not proceed.

        The await happens with no scheduler lock, no database transaction and no forward write
        held, and the disk IO happens on the owner thread, so a slow disk delays this attempt
        without delaying anything else. The wait has no timeout of its own: waiting for a real
        durable acknowledgement is the whole point of the gate, and a caller that is cancelled
        stops waiting without pretending the record is durable.
        """
        try:
            line = self.encode(name, **fields)
        except ValueError:
            return False
        if self.directory is None:
            self._handoff(line)
            return True
        if self._closed:
            return False
        loop = asyncio.get_running_loop()
        self._loop = loop
        future = loop.create_future()
        if self._handoff(line, future=future) is None:
            return False
        return await future

    def _handoff(self, line, future=None, ack=None, maintenance=False, barrier=False):
        """Accept one line into the bounded hand-off, or refuse it visibly.

        Nothing is evicted to make room: a refused record is *not* written and the process
        stops claiming to log in full, which is what the caller observes. Nothing blocking
        happens here: the caller only appends under a short lock and wakes the owner thread.
        """
        if self.directory is None and not maintenance:
            if self._thread is None:
                self._emergency(line)
                return True
        if self._closed:
            return None
        with self._condition:
            if len(self._queue) >= QUEUE_ENTRIES or self._queued_bytes + len(line) > QUEUE_BYTES:
                overflowed = True
            else:
                overflowed = False
                item = _Item(
                    line,
                    future=future,
                    ack=ack,
                    maintenance=maintenance,
                    barrier=barrier,
                    epoch=self._epoch,
                )
                self._queue.append(item)
                self._queued_bytes += len(line)
                self._condition.notify_all()
        if overflowed:
            self._overflowed += 1
            self._dropped += 1
            self._degrade(REASON_IO)
            return None
        return item

    # -- owner thread -------------------------------------------------------------------

    def _run(self):
        """The single writer. Every disk operation in this module happens on this thread."""
        while True:
            items, stop = self._take_batch()
            if items:
                self._commit(items)
            self._flush_emergency()
            if stop:
                break
        self._flush_emergency()
        self._close_handle()

    def _take_batch(self):
        """Block until there is work or a stop request, then take one bounded batch.

        The batch is published as in-flight while the queue lock is still held, so a stop that
        times out can always see -- and answer -- every caller whose line has left the queue.
        """
        with self._condition:
            while not self._queue and not self._closing:
                self._condition.wait()
            items = []
            while self._queue and len(items) < BATCH_LIMIT:
                item = self._queue.popleft()
                self._queued_bytes -= len(item.line)
                items.append(item)
            self._in_flight = tuple(items)
            # Only stop once everything that arrived before the stop request is committed.
            return items, self._closing and not self._queue

    def _drain_queue(self, limit=None):
        with self._condition:
            items = []
            while self._queue and (limit is None or len(items) < limit):
                item = self._queue.popleft()
                self._queued_bytes -= len(item.line)
                items.append(item)
            return items

    def _commit(self, items):
        """Write, flush and fsync one batch on the owner thread, then answer its callers."""
        if not items:
            return
        if self.directory is None:
            # The explicit development fallback: there is no file to be durable in, so every
            # record goes to the one emergency channel, on the owner thread rather than on the
            # loop, and the caller is told what this mode is worth (never "durable").
            for item in items:
                self._emergency(item.line)
                item.persisted = True
            for item in items:
                self._settle(item, True)
            return
        self._in_flight = tuple(items)
        try:
            recovering = self._state == STATE_UNAVAILABLE
            epoch = self._epoch
            for item in items:
                if item.maintenance:
                    # The maintenance action re-reads the directory, so an operator who freed
                    # space is observed as the filesystem is now, not as it was at start-up.
                    self._directory_bytes = self._scan_directory()
            wrote = False
            proved = False
            for item in items:
                if item.barrier:
                    continue
                item.persisted = self._write(item.line, recovering=recovering)
                wrote = wrote or item.persisted
                proved = proved or (item.persisted and item.epoch >= epoch)
            durable = self._sync() if wrote else True
            if not durable:
                for item in items:
                    item.persisted = False
            else:
                for item in items:
                    if item.barrier:
                        # Every record queued before the barrier is on durable storage.
                        item.persisted = True
                if recovering and proved:
                    # Recovery is a stored fact like any other: it is written inside the same
                    # reservation and the healthy state is only declared once it is durable.
                    # Only a record submitted after the failure counts as proof.
                    self._recover()
            for item in items:
                if item.maintenance:
                    # The maintenance action proves the sink only if its own record is durable
                    # and the process is actually logging again afterwards.
                    item.persisted = item.persisted and durable and self._state == STATE_OK
        finally:
            # Every caller is answered before the batch stops being in-flight, so a stop that
            # times out can never find a line that belongs to nobody.
            for item in items:
                self._settle(item, item.persisted)
            self._in_flight = ()

    def _settle(self, item, result):
        if item.future is not None:
            loop = self._loop
            if loop is None or loop.is_closed():
                return
            try:
                loop.call_soon_threadsafe(_resolve, item.future, result)
            except RuntimeError:  # pragma: no cover - loop closed during teardown
                return
        elif item.ack is not None:
            item.ack.set(result)

    # -- the write path (owner thread only) ---------------------------------------------

    def _write(self, line, *, recovering=None):
        """Plan, then write one line, sealing a full segment first. Never raises.

        The plan reserves the line, the seal record that opens the next segment and the
        recovery record that may follow it, all against the segment bound and the directory
        budget. Nothing is written until the whole reservation fits, so a refused record leaves
        the stored bytes exactly as they were.
        """
        if recovering is None:
            recovering = self._state == STATE_UNAVAILABLE
        plan = self._plan(line, recovering)
        if plan is None:
            self._dropped += 1
            self._degrade(REASON_CAPACITY)
            return False
        seal, _tail = plan
        try:
            if self._handle is None and not self._reopen_segment():
                return False
            if seal:
                if not self._seal_segment():
                    self._dropped += 1
                    return False
                self._append(events.encode_record(self.record("log.segment_sealed")))
            self._append(line)
        except OSError:
            self._degrade(REASON_IO)
            self._dropped += 1
            return False
        self._written += 1
        return True

    def _plan(self, line, recovering):
        """The exact reservation for one write, or ``None`` when it cannot fit.

        Returns ``(seal_bytes, tail_bytes)``. ``seal_bytes`` is what the ``log.segment_sealed``
        record will occupy when the current segment must be rotated; ``tail_bytes`` is the line
        plus, while the sink is degraded, the ``log.recovered`` record that follows it.
        """
        tail = len(line) + (self.identity.measure("log.recovered") if recovering else 0)
        seal = 0
        if (
            self._handle is not None
            and self._segment_bytes
            and self._segment_bytes + tail > SEGMENT_BYTES
        ):
            seal = self.identity.measure("log.segment_sealed")
        if seal + tail > SEGMENT_BYTES:
            # A single record must fit one segment; the line budget already guarantees it, and
            # this keeps the guarantee independent of the caller.
            return None
        if self._directory_bytes + seal + tail > self.max_directory_bytes:
            return None
        return seal, tail

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
        """Close the full segment and start the next one. The seal record is metered by the plan."""
        handle, self._handle = self._handle, None
        self._segment_bytes = 0
        if handle is None:
            return False
        try:
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
            self._segment_index += 1
            self._open_segment()
            return True
        except OSError:
            # The full segment is given up, not truncated and not rewritten: its bytes stay as
            # they were, and the next segment gets a name of its own.
            try:
                handle.close()
            except (OSError, ValueError):  # pragma: no cover - already closed is not a failure
                pass
            self._degrade(REASON_IO)
            return False

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
        emergency channel, once per process. The first cause wins: a later symptom never
        rewrites why the sink stopped being usable. Nothing recurses: a failure while reporting
        a failure is swallowed.
        """
        if self._state != STATE_UNAVAILABLE:
            self._state = STATE_UNAVAILABLE
            self._reason = reason
            with self._condition:
                # Every record submitted from now on is a candidate for proving recovery. The
                # counter moves under the same lock a submission reads it under, so a line can
                # never be filed under a degradation it was not submitted in.
                self._epoch += 1
        if self._warned:
            return
        self._warned = True
        name = "log.capacity_exceeded" if reason == REASON_CAPACITY else "log.unavailable"
        try:
            line = events.encode_record(self.record(name, error_code=reason))
        except ValueError:  # pragma: no cover - both events are registered above
            return
        self._queue_emergency(line)

    def _recover(self):
        """Declare the sink healthy again only after the recovery record itself is durable."""
        line = events.encode_record(self.record("log.recovered"))
        if not self._write(line, recovering=False):
            return False
        if not self._sync():
            return False
        self._state = STATE_OK
        self._reason = None
        return True

    def _queue_emergency(self, line):
        """Hand the emergency line to the owner thread, or write it here when there is none.

        The event loop never writes: a caller on the loop only appends to the pending list.
        """
        if self._thread is not None and self._thread.is_alive():
            if threading.current_thread() is self._thread:
                self._emergency(line)
                return
            with self._lock:
                self._pending_emergency.append(line)
            return
        self._emergency(line)

    def _flush_emergency(self):
        with self._lock:
            pending, self._pending_emergency = self._pending_emergency, []
        for line in pending:
            self._emergency(line)

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

    # -- explicit maintenance -----------------------------------------------------------

    def probe_write(self, timeout=SHUTDOWN_TIMEOUT_SECONDS):
        """Explicit maintenance action: prove the sink with a real, successful write.

        This is not reachable from a health probe. It writes one registered
        ``maintenance.log_recovery_check`` record, reports the result and, when it succeeds,
        leaves the sink in the healthy state. With a running owner thread the action is handed
        to that thread rather than opening a second writer; the caller waits on a thread event,
        which is safe because this entry point is the local maintenance command, not a request.

        The directory is measured again first: an operator who freed space or repaired the mount
        expects the command to observe the filesystem as it is now, not the reading taken when
        the process started. Re-reading a directory is not a write, and nothing is deleted,
        truncated or overwritten to make room.
        """
        if self.directory is None:
            return False
        line = events.encode_record(self.record("maintenance.log_recovery_check"))
        thread = self._thread
        if thread is not None and thread.is_alive() and threading.current_thread() is not thread:
            ack = _Ack()
            if self._handoff(line, ack=ack, maintenance=True) is None:
                return False
            if not ack.wait(timeout):
                return False
            return ack.result
        self._directory_bytes = self._scan_directory()
        ok = self._write(line)
        if not ok or not self._sync():
            failed = self.record(
                "maintenance.log_recovery_check", outcome="failed", error_code=REASON_IO
            )
            self._emergency(events.encode_record(failed))
            return False
        if self._state == STATE_UNAVAILABLE and not self._recover():
            return False
        return True

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
        """Whether a new business side effect may be attempted at all.

        A sink that has been asked to stop accepts nothing, even when a write that had not
        returned was left running: the write's result is unknown, and an unknown durability is
        never reported as a usable one.
        """
        return self._state != STATE_UNAVAILABLE and not self._closed

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
            "abandoned": self._abandoned,
            "writer_alive": self._thread is not None and self._thread.is_alive(),
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
