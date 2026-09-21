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
  twice or leave one behind -- including a cancellation that lands *after* the decision but
  *before* the caller has taken delivery of it;
* the caller re-verifies identity, credential, configuration validity and idempotency after
  the permit is granted, so nothing queued here is ever sent upstream by this module.

Fairness is bounded and provable rather than "interactive always first". Interactive work
keeps reserved capacity (background may never hold more than
``max_in_flight - interactive_reserve`` slots globally, nor more than
``max_provider_in_flight - interactive_reserve`` on one provider), and on top of that the
two classes alternate: once a class is served, the other class is offered the next free slot
first. So while a background waiter is queued it can be overtaken by at most one interactive
service, and a sustained interactive stream can no longer starve it. A caller-declared
workload value is observation only and never reorders this.

Observation is a side channel owned by the caller. This module only *forms* bounded facts
inside its lock and hands them out afterwards through ``drain``; it performs no logging, IO
or diagnostics call while holding the lock, and nothing it hands out can change an admission
decision.

TS-103 adds a second, equally narrow observation port next to the private ledger sink:
``observe`` receives the very same bounded facts and a caller may turn them into registered
runtime events. It is the same object, formed in the same place, under the same rule -- no
decision reads it back, and one fact about *waiting* (``queued``) is marked as observation
only so the private admission table keeps its existing four outcomes.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict

LOG = logging.getLogger("tianshu_gateway")


def _ignore(fact):
    """The default fact sink: this module records nothing on its own."""
    del fact
    return None


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
# Formed only for the event side channel: a request really did have to wait. It is not an
# admission decision and never reaches the private admission table.
QUEUED = "queued"


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


class AdmissionFact:
    """One bounded observation produced under the lock and emitted after it is released.

    Plain data with no reference to a store, a connection or the ledger: the module that owns
    the private tables decides how to record it. ``observable_only`` marks a fact that exists
    for the runtime-event side channel alone -- it was still formed under the lock and can
    still change nothing.
    """

    __slots__ = ("service", "request_id", "workload_class", "outcome", "wait_ms", "observable_only")

    def __init__(
        self, service, request_id, workload_class, outcome, wait_ms, observable_only=False
    ):
        self.service = service
        self.request_id = request_id
        self.workload_class = workload_class
        self.outcome = outcome
        self.wait_ms = wait_ms
        self.observable_only = observable_only

    def as_row(self):
        """The field names the private ledger uses, so the caller needs no knowledge of it."""
        return {
            "service": self.service,
            "request_id": self.request_id,
            "workload_class": self.workload_class,
            "outcome": self.outcome,
            "wait_ms": self.wait_ms,
        }


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
        "overtaken",
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
        self.overtaken = 0
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

    def __init__(self, policy, clock=time.monotonic):
        policy.validate()
        if not callable(clock):
            raise ValueError("callable clock required")
        self.policy = policy
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
        # Facts formed under the lock, emitted only after it is released.
        self._facts = []
        self._sink = _ignore
        self._observer = _ignore
        # Fairness state: the class that must be offered the next free slot first. It starts
        # with interactive so an idle pool keeps interactive latency, and flips on every
        # granted permit, which is what bounds how often one class can overtake the other.
        self._preferred = INTERACTIVE

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

    def waiting_overtakes(self, workload_class):
        """How often queued work of one class was passed over for a granted permit.

        Exposed so verification can assert the fairness bound instead of trusting the queue
        order: while any background waiter is queued, the interactive allowance is one.
        """
        return sum(
            waiter.overtaken
            for queues in self._queues.values()
            for waiter in queues[workload_class]
        )

    def background_capacity(self):
        """Slots background work may use: the global limit minus the interactive reserve."""
        return self.policy.max_in_flight - self.policy.interactive_reserve

    def provider_background_capacity(self, limit=None):
        limit = self.policy.max_provider_in_flight if limit is None else limit
        return limit - self.policy.interactive_reserve

    # -- observation port (side channel only) ------------------------------------------

    def drain(self):
        """Take the facts formed since the last call; the caller emits them outside the lock.

        Nothing here can change an admission decision: the caller decides what to do with a
        fact, and a caller that ignores, delays or fails on one cannot affect capacity.
        """
        facts, self._facts = self._facts, []
        return facts

    def _form(self, service, request_id, workload_class, outcome, wait_ms, observable_only=False):
        """Record one bounded fact. Called with the lock held and does no IO."""
        self._facts.append(
            AdmissionFact(service, request_id, workload_class, outcome, wait_ms, observable_only)
        )

    # -- admission ----------------------------------------------------------------------

    async def acquire(
        self, service, workload_class, declared_workload, key, provider_id, timeout_ms
    ):
        """Block until this request holds a permit, or raise without sending anything.

        ``provider_id`` is the provider the pinned configuration of this attempt selected; it
        is never taken from the client, so the provider limit this request waits for can never
        be retargeted while it waits.
        """
        try:
            waiter = await self._enroll(
                service, workload_class, declared_workload, key, provider_id
            )
            await self._pump()
            if waiter.done:
                return waiter
            async with self.lock:
                # The request really is leaving for the queue: this is the only place a
                # "queued" fact is formed, so a request served immediately never claims to
                # have waited.
                self._form(service, waiter.key, workload_class, QUEUED, None, True)
            return await self._wait(waiter, timeout_ms)
        finally:
            self.flush()

    async def release(self, waiter):
        """Free everything this waiter holds; the adapter guards against a second call."""
        try:
            async with self.lock:
                if self._release_locked(waiter):
                    self._advance_locked()
        finally:
            self.flush()

    def flush(self):
        """Emit formed facts outside the lock; a failing sink only degrades.

        The ledger sink receives the ledger's field names, so the caller can wire the private
        recording call straight in without an adapter of its own. The event observer receives
        the fact itself, including the observation-only one the ledger deliberately does not
        know about. Both are best effort: neither can change an admission decision, and a
        failure in either is a bounded local warning rather than an exception on this path.
        """
        for fact in self.drain():
            if not fact.observable_only:
                try:
                    self._sink(**fact.as_row())
                except Exception:
                    LOG.warning("admission_metric_degraded outcome=%s", fact.outcome)
            try:
                self._observer(fact)
            except Exception:
                LOG.warning("admission_observation_degraded outcome=%s", fact.outcome)

    def record(self, sink):
        """Wire the narrow fact sink; it is called with no lock held.

        The default sink does nothing, so a deployment that wires nothing keeps capacity
        truthful and simply records no private admission row. A slow or failing sink can only
        delay fact emission, never an admission decision or a forwarded byte.
        """
        self._sink = sink if callable(sink) else _ignore
        return self

    def observe(self, observer):
        """Wire the event observer for the same facts; it is called with no lock held.

        Kept separate from :meth:`record` so the private admission projection keeps exactly its
        own outcome vocabulary: an event-only fact can never widen the ledger's table.
        """
        self._observer = observer if callable(observer) else _ignore
        return self

    async def _enroll(self, service, workload_class, declared_workload, key, provider_id):
        if workload_class not in CLASSES:
            raise ValueError("invalid workload class")
        if not isinstance(provider_id, str) or not provider_id:
            raise ValueError("verified provider required")
        key = str(uuid.uuid4()) if key is None else key
        async with self.lock:
            if key in self._pending:
                # One launch key may hold at most one permit; otherwise a repeated request
                # could eventually hold two upstream slots for one logical call.
                self._form(service, key, workload_class, DUPLICATE, None)
                raise DuplicateRequest()
            if self._waiting_locked() >= self.policy.max_queue_length:
                self._form(service, key, workload_class, QUEUE_FULL, None)
                raise QueueFull()
            waiter = Waiter(
                key, service, workload_class, declared_workload, provider_id, self.clock
            )
            self._pending[key] = waiter
            self._queues[service][workload_class].append(waiter)
            return waiter

    async def _wait(self, waiter, timeout_ms):
        """Wait for the deadline, the admission decision, or the client giving up.

        A cancelled wait is the client disconnect the gateway already handles, so it is never
        turned into a deadline here: the caller keeps its own cancellation semantics. Either
        way the waiter leaves the queue and its capacity, and nothing is sent.

        The decision can land in the same event-loop step as the cancellation or the deadline,
        after which the caller never reaches the permit at all. That permit is handed back
        here, exactly once, so a lost consumer can never strand global or provider capacity.
        """
        try:
            await asyncio.wait_for(asyncio.shield(waiter.wait()), timeout_ms / 1000)
        except asyncio.TimeoutError:
            await self._discard(waiter, DEADLINE_EXCEEDED)
            raise TimeoutError() from None
        except asyncio.CancelledError:
            current = asyncio.current_task()
            cancelled = current is not None and current.cancelling()
            await self._discard(waiter, CANCELLED)
            if cancelled:
                raise
        return waiter

    async def _discard(self, waiter, outcome):
        """Drop a waiter the caller can no longer take delivery from, then advance the queue.

        One path for every way a wait ends without a usable permit: an undecided waiter leaves
        its queue, and a waiter that was granted in the same step hands the grant back. Both
        cases free capacity at most once because the ledger move is idempotent, and both must
        advance the queue: the slot this waiter did not use -- whether it was reclaimed from a
        grant or simply never taken -- is available to whoever is already waiting. A successor
        may therefore never be left until its own deadline merely because the release it was
        waiting for came from a cleanup instead of from a completed attempt.
        """
        async with self.lock:
            if waiter.done:
                self._reclaim_locked(waiter)
            else:
                self._detach_locked(waiter)
                self._abandon_locked(waiter, outcome)
            if self._has_free_capacity_locked():
                self._advance_locked()

    def _has_free_capacity_locked(self):
        """Whether some waiting request could be served right now, on the global limit alone.

        Cheap and conservative on purpose: it decides whether advancing is worth a pass, while
        the per-provider and background limits stay where they belong, inside ``_can_serve``.
        """
        return self._waiting_locked() > 0 and self._inflight < self.policy.max_in_flight

    def _reclaim_locked(self, waiter):
        """Hand back everything a waiter holds, if it holds anything, exactly once.

        Returns whether capacity was actually freed -- the condition for advancing the queue.
        Every caller that can free capacity goes through here, which is what keeps the release
        and wake-up paths from drifting apart.
        """
        if not waiter.done or waiter.outcome != ADMITTED:
            return False
        return self._release_locked(waiter)

    def _advance_locked(self):
        """Grant every permit the freed capacity allows. Called with the lock held."""
        while self._pump_locked():
            pass

    async def _pump(self):
        async with self.lock:
            self._advance_locked()

    def _pump_locked(self):
        """Grant as many permits as capacity allows, alternating between the two classes.

        Each pass serves at most one waiter, in the class that must be offered the slot first,
        and then flips that preference. Serving one interactive request can therefore never be
        followed by another while background work is ready, which bounds how often a queued
        background request is overtaken and removes the starvation a fixed interactive-first
        order allows.
        """
        served = False
        while True:
            order = (self._preferred, BACKGROUND if self._preferred == INTERACTIVE else INTERACTIVE)
            chosen = None
            for name in order:
                chosen = self._next_ready_locked(name)
                if chosen is not None:
                    break
            if chosen is None:
                return served
            self._serve_locked(chosen)
            served = True

    def _next_ready_locked(self, name):
        """The oldest ready waiter of one class, or None; also counts what it passed over."""
        for service, queues in self._queues.items():
            for waiter in queues[name]:
                if not self._can_serve_locked(waiter):
                    continue
                # Only work that was ready and got passed over is counted, so the counter
                # measures overtaking rather than ordinary queue depth.
                for other in CLASSES:
                    if other == name:
                        continue
                    for skipped in self._queues[service][other]:
                        if skipped is not waiter and self._can_serve_locked(skipped):
                            skipped.overtaken += 1
                return waiter
        return None

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
        if waiter.workload_class == BACKGROUND:
            if self._background_active >= self.background_capacity():
                return False
            if (
                self._background_provider_active.get(waiter.provider_id, 0)
                >= self.provider_background_capacity()
            ):
                return False
        return self._has_capacity_locked(waiter.provider_id)

    def _has_capacity_locked(self, provider_id):
        if self._inflight >= self.policy.max_in_flight:
            return False
        return self._provider_active.get(provider_id, 0) < self.policy.max_provider_in_flight

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
        self._preferred = BACKGROUND if waiter.workload_class == INTERACTIVE else INTERACTIVE
        self._detach_locked(waiter)
        self._form(
            waiter.service,
            waiter.key,
            waiter.workload_class,
            ADMITTED,
            max(0, int((self.clock() - waiter.queued_at) * 1000)),
        )
        if not waiter._settle(ADMITTED):
            # The consumer gave up in the same step; its capacity is not left behind, and the
            # slot it briefly held is offered to whoever is already waiting -- the grant loop
            # that called this method may have no further pass left.
            if self._reclaim_locked(waiter):
                self._advance_locked()

    def _detach_locked(self, waiter):
        self._pending.pop(waiter.key, None)
        for queues in self._queues.values():
            for queue in queues.values():
                if waiter in queue:
                    queue.remove(waiter)

    def _abandon_locked(self, waiter, outcome):
        if not waiter._abandon(outcome):
            return
        self._form(waiter.service, waiter.key, waiter.workload_class, outcome, None)

    def _release_locked(self, waiter):
        """Give back this waiter's slots. Returns whether it freed anything at all.

        The return value is the single wake-up condition: only a call that really moved a
        ledger can make room for a waiting request, so a repeated or empty release neither
        changes capacity nor advances the queue.
        """
        freed = False
        if waiter.globally_admitted:
            waiter.globally_admitted = False
            freed = True
            self._inflight -= 1
            if waiter.workload_class == INTERACTIVE:
                self._interactive_claimed -= 1
            else:
                self._background_active -= 1
        provider = waiter.released_provider
        if provider is None:
            return freed
        waiter.released_provider = None
        freed = True
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
        return freed

    def _waiting_locked(self):
        return sum(len(queue) for queues in self._queues.values() for queue in queues.values())


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
