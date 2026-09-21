"""Independent observation adapter: runtime events, the durable sink and health probes.

The gateway's domain modules import this package; this package imports none of them. It owns
no business rule, no receipt, no capacity decision and no queue: it is given facts that were
already formed elsewhere and turns them into the frozen wire format, or answers a read-only
health question. That direction is the whole point -- logging can never become a second
business database, and a logging failure can never change what a caller receives.

Layering:

* :mod:`~tianshu_gateway.observability.events` is the static catalogue and the closed record;
* :mod:`~tianshu_gateway.observability.sink` is the bounded, durable, single-owner file sink;
* :mod:`~tianshu_gateway.observability.health` is the read-only liveness/readiness surface;
* this module is the assembly point the entry point wires up.
"""

import os
import time
from dataclasses import dataclass
from pathlib import Path

from . import events, health, sink
from .events import (
    CORRELATION_HEADER,
    REGISTRY,
    SCHEMA_VERSION,
    SERVICE,
    bind_correlation,
    current_correlation,
    load_contract,
    new_correlation,
    normalize_correlation,
    reset_correlation,
)
from .health import CheckProviders, ObservationWindow, ReadinessProbe

__all__ = [
    "CORRELATION_HEADER",
    "CheckProviders",
    "DEFAULT_OBSERVATION_VALIDITY_SECONDS",
    "DIAGNOSTICS_TOKEN_ENV",
    "Observability",
    "ObservabilitySettings",
    "REGISTRY",
    "SCHEMA_VERSION",
    "SERVICE",
    "bind_correlation",
    "current_correlation",
    "events",
    "health",
    "load_contract",
    "new_correlation",
    "normalize_correlation",
    "reset_correlation",
    "sink",
    "time",
]

DIAGNOSTICS_TOKEN_ENV = "TIANSHU_DIAGNOSTICS_TOKEN"
DEFAULT_OBSERVATION_VALIDITY_SECONDS = 60.0
MAX_PROBE_BUDGET_MS = 10000


@dataclass(frozen=True)
class ObservabilitySettings:
    """Deployment input for this adapter. Every field is validated before the server starts.

    ``log_directory`` is an explicit absolute path (the deployment mounts it at
    ``/var/log/tianshu``). Leaving it unset is the development fallback: records go to stderr,
    the logging check reports ``non_durable`` and readiness can never be ready, so an
    unconfigured directory cannot be mistaken for a durable production log.
    """

    log_directory: str | None = None
    max_directory_bytes: int = sink.DEFAULT_DIRECTORY_BYTES
    probe_token_env: str = DIAGNOSTICS_TOKEN_ENV
    probe_budget_ms: int = health.PROBE_BUDGET_MS
    observation_validity_seconds: float = DEFAULT_OBSERVATION_VALIDITY_SECONDS

    def validate(self):
        if self.log_directory is not None:
            if not isinstance(self.log_directory, str) or not self.log_directory:
                raise ValueError("log directory must be an explicit path")
            if not Path(self.log_directory).is_absolute():
                raise ValueError("absolute log directory required")
        if (
            type(self.max_directory_bytes) is not int
            or not sink.MIN_DIRECTORY_BYTES <= self.max_directory_bytes <= sink.MAX_DIRECTORY_BYTES
        ):
            raise ValueError("log directory budget outside the supported range")
        if not isinstance(self.probe_token_env, str) or not self.probe_token_env:
            raise ValueError("diagnostics token reference required")
        if (
            type(self.probe_budget_ms) is not int
            or not 1 <= self.probe_budget_ms <= MAX_PROBE_BUDGET_MS
        ):
            raise ValueError("invalid probe budget")
        if (
            type(self.observation_validity_seconds) not in (int, float)
            or self.observation_validity_seconds < 0
        ):
            raise ValueError("invalid observation validity")
        return self


class Observability:
    """One process's observation adapter: identity, sink, probe and dependency windows."""

    def __init__(
        self,
        settings,
        *,
        contract=None,
        instance_id=None,
        stderr=None,
        clock=time.monotonic,
    ):
        self.settings = settings
        self.contract = contract
        self.clock = clock
        self.log = sink.RuntimeLog(
            settings.log_directory,
            instance_id=instance_id,
            max_directory_bytes=settings.max_directory_bytes,
            clock=clock,
            stderr=stderr,
        )
        self.probe = ReadinessProbe(settings.probe_budget_ms)
        self.platform_observation = ObservationWindow(settings.observation_validity_seconds, clock)
        self.model_observation = ObservationWindow(settings.observation_validity_seconds, clock)
        self._assembled = False
        self._closed = False
        self._readiness = None

    # -- assembly -----------------------------------------------------------------------

    def open(self):
        self.log.open()
        return self

    async def start(self):
        # Opening the directory and the first segment is part of starting: a caller that starts
        # the adapter ends up with a usable sink or with an explicit degradation, never with a
        # sink that quietly has nowhere to write.
        self.open()
        await self.log.start()
        self._assembled = True
        return self

    async def shutdown(self, timeout=sink.SHUTDOWN_TIMEOUT_SECONDS):
        self._closed = True
        self.platform_observation.forget()
        self.model_observation.forget()
        await self.log.shutdown(timeout)

    async def flush(self):
        """Wait until every event submitted before this call is on durable storage."""
        return await self.log.flush()

    def close_sync(self):
        self._closed = True
        self.log.close_sync()

    @property
    def assembled(self):
        return self._assembled and not self._closed

    # -- identity and read-only inspection ----------------------------------------------

    @property
    def instance_id(self):
        return self.log.instance_id

    @property
    def state(self):
        return self.log.state

    @property
    def reason(self):
        return self.log.reason

    def stats(self):
        return self.log.stats()

    def logging_status(self):
        """The ``logging`` readiness value; unassembled logging is not configured at all."""
        if not self.assembled:
            return "not_configured"
        return self.log.check_status()

    def accepts_new_work(self):
        """Whether a new business side effect may be attempted under the current sink state."""
        return self.log.accepts_new_work()

    def contracts_status(self):
        return "ok" if self.contract is not None else "not_configured"

    def probe_token(self):
        """The readiness credential, read at probe time so a rotation needs no restart."""
        return os.environ.get(self.settings.probe_token_env, "")

    @property
    def readiness(self):
        """The last readiness this process determined itself; ``None`` before it ever did."""
        return self._readiness

    @property
    def readiness_ok(self):
        return self._readiness == health.READY

    def note_readiness(self, status):
        """Record a readiness transition this process observed on a real path.

        A readiness *probe* never reaches this method: the probe is read-only and writes no
        event of its own, so the transition is reported by assembly or by a business path that
        actually looked at the checks. A repeated value writes nothing.
        """
        if status == self._readiness:
            return False
        self._readiness = status
        return self.log.submit(
            "health.readiness_changed",
            outcome="succeeded" if status == health.READY else "degraded",
        )

    # -- events -------------------------------------------------------------------------

    def _correlation(self, correlation_id):
        if correlation_id is not None:
            return correlation_id
        return current_correlation()

    def event(self, name, *, correlation_id=None, duration_ms=None, error_code=None, outcome=None):
        """Best-effort registered event. Never raises and never blocks the caller."""
        return self.log.submit(
            name,
            outcome=outcome,
            correlation_id=self._correlation(correlation_id),
            duration_ms=duration_ms,
            error_code=error_code,
        )

    async def durable_event(
        self, name, *, correlation_id=None, duration_ms=None, error_code=None, outcome=None
    ):
        """Registered event that must be flushed and fsynced before the caller proceeds.

        Called only outside a scheduler lock, a database transaction and an upstream write.
        ``False`` means the record is not durable, and the caller must refuse the new business
        side effect it was about to cause instead of pretending the attempt was logged.
        """
        return await self.log.submit_durable(
            name,
            outcome=outcome,
            correlation_id=self._correlation(correlation_id),
            duration_ms=duration_ms,
            error_code=error_code,
        )

    def timer(self):
        return self.clock()

    def elapsed_ms(self, started):
        return self.log.identity.elapsed_ms(started)

    def resend(self, record):
        """Write an already-numbered record unchanged, for the collection batch that follows.

        The instance, sequence and event identity are preserved: re-transmission can never
        present an old fact as a new one.
        """
        return self.log.submit_record(record)

    # -- dependency observation (never a probe-time network call) -----------------------

    def observe_platform(self):
        self.platform_observation.observed()

    def observe_model(self):
        self.model_observation.observed()
