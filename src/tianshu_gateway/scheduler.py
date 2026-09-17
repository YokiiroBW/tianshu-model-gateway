"""Bounded single-process admission scheduling for interactive and background work.

This module owns exactly one rule set: admission, the bounded wait queues, the global and
per-provider capacity ledger, the interactive reserve, wait deadlines, cancellation cleanup
and the acquire/release of one permit. It deliberately does not parse a Chat or Responses
payload, read a request header, read the model-configuration library, contact an upstream or
write diagnostics: every input arrives already verified from the caller (an authenticated
service, the deployment-bound class, a launch key, a deadline and the provider that the
pinned configuration selected).

Class is a deployment fact, not a caller preference. The caller derives it from the
protected gateway deployment configuration; the ``declared_workload`` value this module
receives is used only to notice a mismatch for the operator and can never move a request
into another class.

The ledger is process-local and in-memory on purpose:

* a restart drops the queue and the counters; waiting orders are gone and their callers see
  a closed connection, never a delayed send from the previous process. There is no
  persistent queue and no cross-process coordination, so only the existing receipt of an
  attempt that had already started survives;
* every permit is released through one authoritative path (the adapter's guarded
  ``release``), so cancellation, timeout, disconnect and exception cannot free a permit
  twice or leave one behind;
* the caller re-verifies identity, configuration validity and idempotency after the permit
  is granted, so nothing queued here is ever sent upstream by this module.

Interactive work keeps reserved capacity and background work cannot be starved: background
may never hold more than ``max_in_flight - interactive_reserve`` slots globally, nor more than
``max_provider_in_flight - interactive_reserve`` slots on one provider, which leaves the
reserve reachable for interactive work even while a full pool of background orders runs.
Within one class the queue is FIFO, and interactive waiters are always considered before
background ones.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict

LOG = logging.getLogger("tianshu_gateway")

# The two deployment classes of this slice. They are local runtime policy, not part of any
# published contract, and no protocol field, model name or header maps onto them.
INTERACTIVE = "interactive"
BACKGROUND = "background"
CLASSES = (INTERACTIVE, BACKGROUND)

# Private admission outcomes of one bounded-pool request. ``admitted`` means the caller may
# proceed to re-verify and forward; the others never reached an upstream send. A waiter whose
# consumer disappeared without a decision carries ``cancelled`` and no wait duration.
ADMITTED = "admitted"
QUEUE_FULL = "queue_full"
DEADLINE_EXCEEDED = "deadline_exceeded"
CANCELLED = "cancelled"
DUPLICATE = "duplicate"


class AdmissionRefused(Exception):
    """This request may not be admitted; ``reason`` is a fixed local observation value.

    Binding one of these to an HTTP status is the caller's job: the scheduler owns capacity
    decisions, not transport semantics.
    """

    def __init__(self, reason, status):
        self.reason = reason
        self.status = status
        super().__init__(reason)


class QueueFull(AdmissionRefused):
    """The bounded queue cannot accept another waiter."""

    def __init__(self):
        super().__init__(QUEUE_FULL, 429)


class DuplicateRequest(AdmissionRefused):
    """One launch key is already waiting or running: it must not hold two slots."""

    def __init__(self):
        super().__init__(DUPLICATE, 400)


class Waiter:
    """One admitted-or-waiting request. Never a second source of capacity truth.

    The three flags are the whole observable state of a permit: ``globally_admitted`` for the
    global slot, ``released_provider`` for the provider slot, and ``_done`` for the admission
    decision. ``live()`` is what the caller must re-check before sending anything upstream.
    """

    __slots__ = (
        "key",
        "service",
        "workload_class",
        "declared_workload",
        "provider_id",
        "queued_at",
        "released_provider",
        "globally_admitted",
        "outcome",
        "wait_ms",
        "_clock",
        "_future",
        "_done",
    )

    def __init__(self, key, service, workload_class, declared_workload, provider_id, clock):
        self.key = key
        self.service = service
        self.workload_class = workload_class
        self.declared_workload = declared_workload
        self.provider_id = provider_id
        self.queued_at = clock()
        self.released_provider = None
        self.globally_admitted = False
        self.outcome = None
        self.wait_ms = None
        self._clock = clock
        self._future = asyncio.get_running_loop().create_future()
        self._done = False

    def live(self):
        """True while this permit still authorizes sending: the caller re-checks it."""
        return (
            self.outcome == ADMITTED
            and self.globally_admitted
            and self.released_provider is not None
        )

    @property
    def done(self):
        """True once this waiter has an admission decision (or was abandoned)."""
        return self._done

    def wait(self):
        """Await an admission decision; a lost consumer is an abandoned waiter."""
        if self._done:
            if self.outcome == ADMITTED:
                return self
            raise self._refused()
        if self._future.cancelled():
            raise self._refused()
        return self._future

    def _refused(self):
        if self.outcome == DEADLINE_EXCEEDED:
            return TimeoutError()
        return DuplicateRequest() if self.outcome == DUPLICATE else QueueFull()

    def _settle(self, outcome):
        if self._future.done():
            return False
        self._done = True
        self.outcome = outcome
        self.wait_ms = max(0, int((self._clock() - self.queued_at) * 1000))
        self._future.set_result(self)
        return True

    def _abandon(self, outcome):
        """Give up without an admission decision; the consumer is gone."""
        if self._done:
            return False
        self._done = True
        self.outcome = outcome
        self.wait_ms = None
        self._future.cancel()
        return True


class BoundedAdmissionScheduler:
    """Owns in-flight capacity, per-provider capacity and the bounded wait queues.

    One request is enrolled once, with every input already verified by the caller: the
    authenticated service, the deployment-bound class, the provider that the pinned
    configuration selected, a launch key and a deadline. It receives a permit only when both
    its own provider limit and the global limit can be granted together, so a busy provider
    *waits* instead of failing -- which is the point of a bounded queue.
    """

    def __init__(self, policy, diagnostics=None, clock=time.monotonic):
        policy.validate()
        if not callable(clock):
            raise ValueError("callable clock required")
        self.policy = policy
        self.diagnostics = diagnostics
        self.clock = clock
        self.lock = asyncio.Lock()
        self._pending = {}
        # One queue per class per service; a service is created on its first enrollment, so
        # the ledger accepts any authenticated registration without a fixed roster.
        self._queues = defaultdict(lambda: {INTERACTIVE: [], BACKGROUND: []})
        self._inflight = 0
        self._provider_active = {}
        self._background_active = 0
        self._background_provider_active = {}
        self._interactive_claimed = 0
        self._interactive_claimed_provider = {}

    # -- read-only inspection (verification and operator observation) -------------------

    @property
    def inflight(self):
        return self._inflight

    @property
    def provider_active(self):
        return {name: count for name, count in self._provider_active.items() if count}

    @property
    def waiting(self):
        return sum(
            len(queues[INTERACTIVE]) + len(queues[BACKGROUND]) for queues in self._queues.values()
        )

    @property
    def interactive_claimed(self):
        return self._interactive_claimed

    @property
    def background_claimed(self):
        """How much of the pool background work holds: never above ``background_capacity``."""
        return self._background_active

    def waiting_keys(self):
        return set(self._pending)

    def background_capacity(self):
        """Slots background work may use: the global limit minus the interactive reserve."""
        return self.policy.max_in_flight - self.policy.interactive_reserve

    def provider_background_capacity(self, limit=None):
        limit = self.policy.max_provider_in_flight if limit is None else limit
        return limit - self.policy.interactive_reserve

    # -- admission ----------------------------------------------------------------------

    async def acquire(
        self, service, workload_class, declared_workload, key, provider_id, timeout_ms
    ):
        """Block until this request holds a permit, or raise without sending anything.

        ``provider_id`` is the provider the pinned configuration of this attempt selected; it
        is never taken from the client, so the provider limit this request waits for can never
        be retargeted while it waits.
        """
        waiter = await self._enroll(
            service, workload_class, declared_workload, key, provider_id, timeout_ms
        )
        await self._pump()
        if waiter.done:
            return waiter
        return await self._wait(waiter, timeout_ms)

    async def release(self, waiter):
        """Free everything this waiter holds; the adapter guards against a second call."""
        async with self.lock:
            self._release_locked(waiter)
            while self._pump_locked():
                pass

    async def _enroll(
        self, service, workload_class, declared_workload, key, provider_id, timeout_ms
    ):
        if workload_class not in CLASSES:
            raise ValueError("invalid workload class")
        if not isinstance(provider_id, str) or not provider_id:
            raise ValueError("verified provider required")
        key = str(uuid.uuid4()) if key is None else key
        async with self.lock:
            if key in self._pending:
                # One launch key may hold at most one permit; otherwise a repeated request
                # could eventually hold two upstream slots for one logical call.
                self._observe(service, key, workload_class, DUPLICATE, None)
                raise DuplicateRequest()
            if self._waiting_locked() >= self.policy.max_queue_length:
                self._observe(service, key, workload_class, QUEUE_FULL, None)
                raise QueueFull()
            waiter = Waiter(
                key, service, workload_class, declared_workload, provider_id, self.clock
            )
            self._pending[key] = waiter
            self._queues[service][workload_class].append(waiter)
            if declared_workload is not None and declared_workload != workload_class:
                # Observation only: a caller-declared workload never moves a caller between
                # classes, it only tells the operator that the caller and the deployment
                # binding disagree about what the request is doing.
                LOG.info("declared_workload_differs_from_binding")
            return waiter

    async def _wait(self, waiter, timeout_ms):
        """Wait for the deadline, the admission decision, or the client giving up.

        A cancelled wait is the client disconnect the gateway already handles, so it is never
        turned into a deadline here: the caller keeps its own cancellation semantics. Either
        way the waiter leaves the queue and its capacity, and nothing is sent.
        """
        try:
            await asyncio.wait_for(asyncio.shield(waiter.wait()), timeout_ms / 1000)
        except asyncio.TimeoutError:
            await self._remove(waiter)
            self._abandon(waiter, DEADLINE_EXCEEDED)
            raise TimeoutError() from None
        except asyncio.CancelledError:
            current = asyncio.current_task()
            cancelled = current is not None and current.cancelling()
            await self._remove(waiter)
            self._abandon(waiter, CANCELLED)
            if cancelled:
                raise
        return waiter

    async def _remove(self, waiter):
        """Detach an undecided waiter from its queue; a held slot is freed by ``release``."""
        async with self.lock:
            if not waiter.done:
                self._detach_locked(waiter)

    async def _pump(self):
        async with self.lock:
            while self._pump_locked():
                pass

    def _pump_locked(self):
        served = False
        for name in (INTERACTIVE, BACKGROUND):
            # One queue per class, so a waiting background request can never sit in front of
            # an interactive one, and interactive is always considered first.
            for service, queues in self._queues.items():
                for waiter in tuple(queues[name]):
                    if not self._can_serve_locked(waiter):
                        continue
                    self._serve_locked(waiter)
                    served = True
                    break
        return served

    def _can_serve_locked(self, waiter):
        """Two hard limits, in the order that makes the reserve meaningful.

        A per-provider limit and the global limit are independent: either one being full means
        this request waits. Background work may additionally never hold more than
        ``limit - interactive_reserve`` slots, globally and per provider, so the reserve always
        remains available to interactive work -- while background work itself can still reach
        its own capacity no matter how much interactive work is running.
        """
        if waiter.done:
            return False
        if self._inflight >= self.policy.max_in_flight:
            return False
        if self._provider_active.get(waiter.provider_id, 0) >= self.policy.max_provider_in_flight:
            return False
        if waiter.workload_class == BACKGROUND:
            if self._background_active >= self.background_capacity():
                return False
            if (
                self._background_provider_active.get(waiter.provider_id, 0)
                >= self.provider_background_capacity()
            ):
                return False
        return True

    def _serve_locked(self, waiter):
        """Grant the global and provider slots together: only a full permit is admitted."""
        provider = waiter.provider_id
        waiter.globally_admitted = True
        waiter.released_provider = provider
        self._inflight += 1
        self._provider_active[provider] = self._provider_active.get(provider, 0) + 1
        if waiter.workload_class == BACKGROUND:
            self._background_active += 1
            self._background_provider_active[provider] = (
                self._background_provider_active.get(provider, 0) + 1
            )
        else:
            self._interactive_claimed += 1
            self._interactive_claimed_provider[provider] = (
                self._interactive_claimed_provider.get(provider, 0) + 1
            )
        self._detach_locked(waiter)
        self._observe(
            waiter.service,
            waiter.key,
            waiter.workload_class,
            ADMITTED,
            max(0, int((self.clock() - waiter.queued_at) * 1000)),
        )
        if not waiter._settle(ADMITTED):
            # The consumer gave up in the same step; its capacity is not left behind.
            self._release_locked(waiter)

    def _detach_locked(self, waiter):
        self._pending.pop(waiter.key, None)
        for queues in self._queues.values():
            for queue in queues.values():
                if waiter in queue:
                    queue.remove(waiter)

    def _release_locked(self, waiter):
        if waiter.globally_admitted:
            waiter.globally_admitted = False
            self._inflight -= 1
            if waiter.workload_class == INTERACTIVE:
                self._interactive_claimed -= 1
            else:
                self._background_active -= 1
        provider = waiter.released_provider
        if provider is None:
            return
        waiter.released_provider = None
        self._provider_active[provider] -= 1
        if not self._provider_active[provider]:
            del self._provider_active[provider]
        ledger = (
            self._interactive_claimed_provider
            if waiter.workload_class == INTERACTIVE
            else self._background_provider_active
        )
        claimed = ledger[provider] - 1
        if claimed:
            ledger[provider] = claimed
        else:
            del ledger[provider]

    def _abandon(self, waiter, outcome):
        if not waiter._abandon(outcome):
            return
        self._observe(waiter.service, waiter.key, waiter.workload_class, outcome, None)

    def _waiting_locked(self):
        return sum(len(queue) for queues in self._queues.values() for queue in queues.values())

    def _observe(self, service, request_id, workload_class, outcome, wait_ms):
        """Hand one bounded fact to the injected diagnostics port; failures only degrade.

        Diagnostics is a side channel: this never raises, never retries, and never influences
        the admission decision that follows or precedes it.
        """
        if self.diagnostics is None:
            return
        try:
            self.diagnostics.record_admission(
                service=service,
                request_id=request_id,
                workload_class=workload_class,
                outcome=outcome,
                wait_ms=wait_ms,
            )
        except Exception:
            LOG.warning("admission_metric_degraded outcome=%s", outcome)


def bind(scheduler, service, workload_class, declared_workload, provider_id, key=None):
    """Create the per-request permit adapter the caller holds for the whole attempt."""
    return _Permit(scheduler, service, workload_class, declared_workload, provider_id, key)


class _Permit:
    """One attempt's permit: enter to be admitted, re-check with ``live``, release once."""

    __slots__ = (
        "scheduler",
        "service",
        "workload_class",
        "declared_workload",
        "provider_id",
        "_key",
        "waiter",
        "_done",
    )

    def __init__(self, scheduler, service, workload_class, declared_workload, provider_id, key):
        self.scheduler = scheduler
        self.service = service
        self.workload_class = workload_class
        self.declared_workload = declared_workload
        self.provider_id = provider_id
        self._key = key
        self.waiter = None
        self._done = False

    async def __aenter__(self):
        self.waiter = await self.scheduler.acquire(
            self.service,
            self.workload_class,
            self.declared_workload,
            self._key,
            self.provider_id,
            self.scheduler.policy.wait_timeout_ms,
        )
        return self

    async def __aexit__(self, *_):
        await self.release()
        return False

    def live(self):
        """Whether this permit still authorizes an upstream send; the caller re-checks it."""
        return self.waiter is not None and self.waiter.live()

    async def release(self):
        if self._done or self.waiter is None:
            return
        self._done = True
        await self.scheduler.release(self.waiter)
