"""Read-only liveness and readiness.

A probe answers with what the running process already knows. It creates no directory, writes
no probe file, applies no migration, runs no DDL, ticks no worker, flushes nothing, revokes
nothing, creates no receipt, repairs no database, claims or cancels no queue entry and calls
no model. It does not refresh the configuration cache and does not open a new database
connection. Two consequences follow and are the point of this module:

* liveness says only that the event loop answered. It is not a statement about the model,
  the platform or the database, and a container health check uses it -- never readiness --
  because readiness failing must not turn into a restart storm for the whole dependency
  chain;
* a request to either endpoint produces no durable observation at all. The result is recorded
  by whatever external monitor asked, not by the gateway.

Readiness is a conjunction of the checks this deployment actually depends on, and every check
is a value the process can read without side effects. A dependency nobody measured stays
``not_verified``: a configured address is not evidence that the peer is reachable, so
"reachable" is never reported merely because a configuration entry exists.
"""

import hmac
import json
import time
from dataclasses import dataclass
from typing import Callable

SERVICE = "gateway"
LIVE = "alive"
READY = "ready"
NOT_READY = "not_ready"

# The closed check vocabulary of the frozen package. A value outside it is never returned.
CHECK_NAMES = (
    "configuration",
    "contracts",
    "runtime",
    "ledger",
    "logging",
    "platform",
    "model",
    "native",
)
# The checks a deployment cannot serve without. Native and the two remote dependencies are
# deliberately outside this tuple: an optional capability or an unmeasured peer must not make
# basic chat unavailable.
REQUIRED_CHECKS = ("configuration", "contracts", "runtime", "ledger", "logging")
CHECK_VALUES = ("ok", "failed", "not_configured", "not_verified", "non_durable")
FAILED = "failed"

LIVE_BODY = {"status": LIVE}
MAX_BODY_BYTES = 16384
PROBE_CONCURRENCY = 2
PROBE_BUDGET_MS = 1000
STATUS_OK = 200
STATUS_UNAUTHORIZED = 401
STATUS_UNAVAILABLE = 503
STATUS_TOO_MANY = 429


@dataclass(frozen=True)
class CheckProviders:
    """The eight fixed checks, each a synchronous read of state the process already has.

    Every provider returns a value from :data:`CHECK_VALUES`. None of them performs IO with a
    side effect, so assembling them here cannot become a hidden probe-time write.
    """

    configuration: Callable[[], str]
    contracts: Callable[[], str]
    runtime: Callable[[], str]
    ledger: Callable[[], str]
    logging: Callable[[], str]
    platform: Callable[[], str]
    model: Callable[[], str]
    native: Callable[[], str]

    def collect(self):
        """Read every check in the fixed order; one broken check is only that check."""
        checks = {}
        for name in CHECK_NAMES:
            try:
                value = getattr(self, name)()
            except Exception:
                value = FAILED
            checks[name] = value if value in CHECK_VALUES else FAILED
        return checks


class ObservationWindow:
    """A dependency reads ``ok`` only while a real success is recent enough.

    The gateway records a success when it actually completed one -- a platform snapshot that
    answered and verified, or an upstream model call that finished. Nothing in this class
    contacts anything, and an expired observation degrades to ``not_verified`` rather than
    being refreshed by the probe.
    """

    def __init__(self, validity_seconds, clock=time.monotonic):
        self.validity_seconds = validity_seconds
        self.clock = clock
        self._observed = None

    def observed(self):
        self._observed = self.clock()

    def forget(self):
        self._observed = None

    def status(self):
        if self._observed is None or self.validity_seconds <= 0:
            return "not_verified"
        if self.clock() - self._observed > self.validity_seconds:
            return "not_verified"
        return "ok"

    @property
    def last_observed(self):
        """In-memory reading for verification only; never part of a response body."""
        return self._observed


def ready_status(checks):
    return READY if all(checks[name] == "ok" for name in REQUIRED_CHECKS) else NOT_READY


def status_code(status):
    return STATUS_OK if status == READY else STATUS_UNAVAILABLE


def ready_body(checks):
    """The closed readiness body: exactly ``status``, ``service`` and ``checks``.

    There is no ``checked_at``, no counter, no path, no environment value and no business
    identifier: a caller learns whether this instance is ready and which local check says no,
    and nothing about how the deployment is configured.
    """
    return {"status": ready_status(checks), "service": SERVICE, "checks": checks}


def encode_body(body):
    """Render a closed body; a body that somehow exceeded its budget degrades, never grows."""
    raw = json.dumps(body, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    if len(raw) > MAX_BODY_BYTES:  # pragma: no cover - the shape is fixed and tiny
        return json.dumps(
            {"status": NOT_READY, "service": SERVICE, "checks": dict.fromkeys(CHECK_NAMES, FAILED)},
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
    return raw


class ReadinessProbe:
    """Bounded, read-only readiness evaluation for one process."""

    def __init__(self, budget_ms=PROBE_BUDGET_MS, concurrency=PROBE_CONCURRENCY):
        self.budget_ms = budget_ms
        self.concurrency = concurrency
        self._active = 0

    @property
    def active(self):
        return self._active

    def busy(self):
        return self._active >= self.concurrency

    def evaluate(self, providers):
        """Return ``(http_status, body)`` for one readiness request.

        Two probes may be in flight at once; a third is refused with an explicit status rather
        than queued behind them. The whole evaluation is a bounded, in-memory read, and a run
        that overruns the budget is reported as not ready instead of being answered anyway.
        """
        if self.busy():
            return STATUS_TOO_MANY, ready_body(dict.fromkeys(CHECK_NAMES, FAILED))
        self._active += 1
        try:
            started = time.monotonic()
            checks = providers.collect()
            over_budget = (time.monotonic() - started) * 1000 > self.budget_ms
        finally:
            self._active -= 1
        if over_budget:
            return STATUS_UNAVAILABLE, ready_body(dict.fromkeys(CHECK_NAMES, FAILED))
        body = ready_body(checks)
        return status_code(body["status"]), body


def probe_authorization(header, expected):
    """The readiness endpoint's own credential decision.

    ``TIANSHU_DIAGNOSTICS_TOKEN`` is independent of every business, administrator and upstream
    token, so one of those can never open the readiness detail. A server with no configured
    token refuses with ``503``: an unconfigured probe is not a probe that everyone may read.
    An absent or malformed credential is ``401``. The comparison is constant time and the
    presented value is never echoed, logged or stored.
    """
    if not expected:
        return STATUS_UNAVAILABLE
    if not isinstance(header, str) or not header.startswith("Bearer "):
        return STATUS_UNAUTHORIZED
    token = header[len("Bearer ") :]
    if not token or not hmac.compare_digest(token.encode(), expected.encode()):
        return STATUS_UNAUTHORIZED
    return STATUS_OK
