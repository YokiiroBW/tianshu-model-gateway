"""Actual aiohttp transport with one attempt, cancellation and bounded streaming."""

import asyncio
import hmac
import logging
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field

import aiohttp
from aiohttp import web

from .config import (
    Classification,
    ClientGrant,
    ConfigCache,
    EnvSecrets,
    HttpConfigSource,
    RegisteredTargets,
    SchedulingPolicy,
    default_policy,
    read_limited,
    utcnow,
)
from .contracts import (
    NATIVE_CONTRACT,
    NATIVE_RECEIPT_PATH,
    NATIVE_ROUTE_PATH,
    NATIVE_VERSION_HEADER,
    Contracts,
    Rejected,
    loads,
)
from .diagnostics import NATIVE_IDENTITY_FIELDS, Diagnostics, attempt_metrics, redact
from .native import (
    NativeConfigCache,
    NativeConfigSource,
    NativeGrant,
    authorize,
    requested_version,
    route_context,
    route_receipt,
    select_native_route,
)
from .observability import (
    CORRELATION_HEADER,
    CheckProviders,
    ObservabilitySettings,
    bind_correlation,
    reset_correlation,
)
from .observability import events as observation_events
from .observability import health as probe_health
from .observability.events import load_contract
from .origin_renewal import (
    CurrentCache,
    OriginRenewal,
    validate_settings as validate_origin_renewal,
)
from .responses import send_responses, validate_request
from .routing import PROTOCOL, SecretGuard, StreamObserver, apply_fields, prepare, record_usage
from .scheduler import AdmissionRefused, BoundedAdmissionScheduler, bind
from .usage import build_report

LOG = logging.getLogger("tianshu_gateway")
INTERNAL_HEADERS = {"x-tianshu-config-version", "x-tianshu-workload", "x-tianshu-turn-id"}
NATIVE_HEADERS = {
    NATIVE_VERSION_HEADER.lower(),
    "x-tianshu-turn-id",
}
NATIVE_ROUTE_HEADERS = {"x-request-id"} | NATIVE_HEADERS | {"authorization"}
# Private, authenticated operations reads. They are not part of either published contract
# and return no message body, tool argument, credential or provider URL.
USAGE_PATH = "/internal/v1/model-usage"
NATIVE_USAGE_PATH = "/internal/v1/native-model-usage"
REQUEST_ID = web.RequestKey("request_id", str)
FORWARD_STARTED = web.RequestKey("forward_started", bool)
STREAM_RESPONSE = web.RequestKey("stream_response", web.StreamResponse)
# TS-103 observation state of one in-flight request. Both are per-request facts about what has
# already been recorded, so "one terminal per request" is enforced rather than hoped for.
ACCEPTED_RECORDED = web.RequestKey("accepted_recorded", bool)
TERMINAL_RECORDED = web.RequestKey("terminal_recorded", bool)
FIRST_OUTPUT_RECORDED = web.RequestKey("first_output_recorded", bool)
WORK_STARTED = web.RequestKey("work_started", float)
# The two read-only probes. They are read from the URL path before routing so the request
# middleware can promise they record nothing at all, whichever route answers them.
LIVE_PATH = "/health/live"
READY_PATH = "/health/ready"
PROBE_PATHS = frozenset({LIVE_PATH, READY_PATH})
NO_STORE = {"Cache-Control": "no-store"}
JSON_TYPE = "application/json"

# Exactly one of these is recorded per request, and the first one wins. The rest of the
# catalogue describes a stage of the same request and never ends it.
TERMINAL_EVENTS = frozenset(
    {
        "request.finished",
        "request.rejected",
        "request.unauthenticated",
        "request.revoked",
        "request.route_unmatched",
        "request.disconnected",
        "request.cancelled",
        "request.queue_full",
        "request.queue_expired",
        "request.duplicate",
    }
)

# The terminal observation of one upstream attempt, keyed by the attempt reason the transport
# already uses. Nothing here is a new decision: each entry is the fixed event vocabulary for a
# reason the forwarding path already produced, and both tables below are asserted against the
# registered catalogue by the tests.
ATTEMPT_TERMINALS = {
    "completed": ("succeeded", None),
    "response_completed": ("succeeded", None),
    "response_failed": ("failed", "upstream_http_error"),
    "response_incomplete": ("unknown", "result_unknown"),
    "observation_incomplete": ("unknown", "result_unknown"),
    "upstream_http_error": ("failed", "upstream_http_error"),
    "incomplete_stream": ("unknown", "result_unknown"),
    "transport_unknown": ("unknown", "result_unknown"),
    "timeout_unknown": ("unknown", "timeout"),
    "cancelled_unknown": ("cancelled", None),
}
UNKNOWN_ATTEMPT_TERMINAL = ("unknown", "result_unknown")
# The same fixed pairs, addressed by the outcome the authoritative receipt already carries.
OUTCOME_TERMINALS = {
    "succeeded": ("succeeded", None),
    "failed": ("failed", "upstream_http_error"),
    "unknown": ("unknown", "result_unknown"),
    "cancelled": ("cancelled", None),
}
ROUTE_UNMATCHED_STATUSES = frozenset({404, 405})
HTTP_STATUS_CODES = {
    400: "invalid_input",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    408: "timeout",
    409: "version_conflict",
    413: "payload_too_large",
    429: "queue_full",
    501: "unsupported_operation",
    502: "result_unknown",
    503: "dependency_unavailable",
}

# The request being served, so a fact another module forms on this task can be attributed to
# it without that module ever seeing an HTTP request.
_CURRENT_REQUEST = ContextVar("tianshu_current_request", default=None)


@dataclass
class Settings:
    contract_directory: str
    diagnostics_path: str
    platform_base_url: str
    platform_credential_ref: str
    platform_origin_env: str
    secret_references: dict[str, str]
    targets: list[dict]
    clients: list[ClientGrant]
    native_enabled: bool = False
    native_contract_directory: str | None = None
    native_clients: list[NativeGrant] = field(default_factory=list)
    revoked_native_versions: list[int] = field(default_factory=list)
    max_request_bytes: int = 1_048_576
    max_response_bytes: int = 8_388_608
    max_concurrent: int = 16
    max_provider_concurrent: int = 4
    max_timeout_ms: int = 180_000
    request_read_timeout: float = 5.0
    config_refresh_seconds: float = 30.0
    revoked_versions: list[int] = field(default_factory=list)
    # TS-044 local runtime policy. Both are absent from an existing deployment document, and
    # their defaults reproduce the previous behaviour exactly (no waiting, immediate refusal),
    # so an old deployment keeps its exact semantics without being edited.
    scheduling: SchedulingPolicy | None = None
    workload_bindings: Classification | None = None
    # TS-103 observation input. Both are absent from an existing deployment document and both
    # defaults reproduce the previous behaviour exactly: no runtime event is emitted and no
    # health surface is configured, so an old document still starts unchanged.
    diagnostics_contract_directory: str | None = None
    observability: ObservabilitySettings | None = None
    platform_origin_renewal: bool = False

    @property
    def policy(self):
        """The effective scheduling policy; ``None`` fields mean the historical defaults."""
        return self.scheduling or default_policy(self.max_concurrent, self.max_provider_concurrent)

    @property
    def classification(self):
        return self.workload_bindings or Classification()

    def validate(self):
        validate_origin_renewal(self.platform_origin_renewal, self.platform_base_url)
        if not self.clients and not self.native_clients:
            raise ValueError("authenticated client registration required")
        if (
            min(
                self.max_request_bytes,
                self.max_response_bytes,
                self.max_concurrent,
                self.max_provider_concurrent,
                self.max_timeout_ms,
                self.request_read_timeout,
            )
            <= 0
        ):
            raise ValueError("positive limits required")
        if self.config_refresh_seconds < 0:
            raise ValueError("nonnegative refresh interval required")
        if self.diagnostics_contract_directory is not None and (
            not isinstance(self.diagnostics_contract_directory, str)
            or not self.diagnostics_contract_directory
        ):
            raise ValueError("explicit diagnostics contract directory required")
        if self.observability is not None:
            # An unusable observation configuration is refused before the server binds: a
            # relative log directory or an out-of-range budget must not start and then claim
            # to be logging.
            self.observability.validate()
        if len({c.service for c in self.clients}) != len(self.clients):
            raise ValueError("one explicit grant per service required")
        if any(type(c.config_version) is not int or c.config_version < 1 for c in self.clients):
            raise ValueError("pinned configuration version required")
        if any(
            type(c.internal) is not bool
            or any(type(v) is not int or v < 1 for v in c.allowed_versions)
            for c in self.clients
        ):
            raise ValueError("invalid client grant")
        if type(self.native_enabled) is not bool:
            raise ValueError("invalid native switch")
        native_material = bool(
            self.native_clients
            or self.native_contract_directory is not None
            or self.revoked_native_versions
        )
        if native_material and not self.native_enabled:
            # Native stays off by default; half-configured material must not look absent.
            raise ValueError("native material requires explicit enable")
        if any(type(v) is not int or v < 1 for v in self.revoked_native_versions):
            raise ValueError("invalid revoked native version")
        # Scheduling is a local runtime policy; a half-valid policy must never start.
        policy = self.policy
        policy.validate()
        self.classification.validate()
        if policy.max_provider_in_flight > policy.max_in_flight:
            raise ValueError("provider limit above the global limit")
        if policy.interactive_reserve > policy.max_provider_in_flight:
            raise ValueError("interactive reserve above the provider limit")
        if not self.native_enabled:
            return
        if not self.native_contract_directory:
            raise ValueError("published native contract directory required")
        if not self.native_clients:
            raise ValueError("authenticated native client registration required")
        if len({g.service for g in self.native_clients}) != len(self.native_clients):
            raise ValueError("one explicit native grant per service required")
        for grant in self.native_clients:
            grant.validate()


class Gateway:
    def __init__(
        self, settings, contracts, targets, session, diagnostics, scheduler=None, observability=None
    ):
        self.settings, self.contracts, self.targets = settings, contracts, targets
        self.session, self.diagnostics = session, diagnostics
        self.scheduler = scheduler
        # The observation adapter is assembled by the entry point and may be absent: without it
        # this class emits no runtime event at all, which is exactly the pre-TS-103 behaviour.
        self.observability = observability
        self.contract_verified = False
        self.closed = False
        self.classification = settings.classification
        if scheduler is not None:
            # One narrow port, no adapter: the scheduler forms bounded facts with no lock held
            # and the private ledger records them. The scheduler holds no store of its own, and
            # a failing or slow ledger can only lose a private row.
            scheduler.record(self.diagnostics.record_admission)
            # The same bounded facts also feed the read-only runtime-event channel, through a
            # second port. Neither port can change an admission decision or a forwarded byte.
            scheduler.observe(self.admission_fact)
        # The ledger reports its own fixed persistence failures here, so the adapter never has
        # to read a row, a statement or an exception to observe one.
        self.diagnostics.observe(self.receipt_failure)
        self.secrets = EnvSecrets(settings.secret_references)
        self.origin_renewal = (
            OriginRenewal(
                session,
                targets,
                self.secrets,
                settings.platform_base_url,
                settings.platform_credential_ref,
                settings.platform_origin_env,
            )
            if settings.platform_origin_renewal
            else None
        )
        self.cache = ConfigCache(
            HttpConfigSource(
                session,
                contracts,
                targets,
                self.secrets,
                settings.platform_base_url,
                settings.platform_credential_ref,
                settings.platform_origin_env,
                # A verified snapshot is a real success of the platform dependency, so the
                # readiness window is refreshed here and never on a cache hit.
                on_success=self.note_platform,
            ),
            contracts,
            targets,
            diagnostics,
            settings.config_refresh_seconds,
        )
        # The native ledger is the same local database but a different key space.
        self.native_ledger = diagnostics
        self.native_cache = None
        if settings.native_enabled:
            self.native_cache = NativeConfigCache(
                NativeConfigSource(
                    session,
                    contracts,
                    targets,
                    self.secrets,
                    settings.platform_base_url,
                    settings.platform_credential_ref,
                    settings.platform_origin_env,
                ),
                contracts,
                targets,
                diagnostics,
                settings.config_refresh_seconds,
            )
        if self.origin_renewal is not None:
            self.cache = CurrentCache(self.cache, self.origin_renewal)
            if self.native_cache is not None:
                self.native_cache = CurrentCache(self.native_cache, self.origin_renewal)
        self.active = 0
        self.provider_active = {}

    @property
    def idle(self):
        """True when no attempt holds capacity and no request is still waiting."""
        if self.scheduler is not None:
            return self.scheduler.inflight == 0 and self.scheduler.waiting == 0
        return self.active == 0

    @property
    def busy_providers(self):
        if self.scheduler is not None:
            return self.scheduler.provider_active
        return {name: count for name, count in self.provider_active.items() if count}

    def class_for(self, service):
        """The deployment-bound class of one authenticated service.

        This is the only place where a caller is mapped to a workload class, and the only
        input is the authenticated registration. A request body, a model name, the protocol
        or the client-declared ``X-Tianshu-Workload`` value can never change it: the declared
        value is passed to the scheduler for observation only.
        """
        return self.classification.for_service(service)

    def declared_workload(self, request, internal):
        return request.headers.get("X-Tianshu-Workload") if internal else None

    def slot(self, request, service, internal, key, provider_id):
        """Capacity adapter for one attempt: the bounded pool or the historical counter.

        The bounded pool is active only when the deployment reserved interactive capacity. A
        reserve of zero means the previous behaviour, so such a deployment keeps the exact old
        accounting (a plain counter, an immediate ``queue_full`` when capacity is busy) instead
        of acquiring a queue it never asked for.
        """
        if self.pool_active:
            return bind(
                self.scheduler,
                service,
                self.class_for(service),
                self.declared_workload(request, internal),
                provider_id,
                key,
            )
        return _CounterSlot(self, request, service, provider_id)

    @property
    def pool_active(self):
        """True when this deployment runs the bounded pool instead of the plain counter."""
        return self.scheduler is not None and self.settings.policy.interactive_reserve > 0

    # -- observation (assembly seam; never a business rule) ------------------------------

    def record_event(self, request, name, **fields):
        """Record one registered runtime event, enforcing one terminal per request.

        A stage event is recorded every time it happens -- none of them is sampled, filtered by
        success or rate limited -- while a terminal event is recorded exactly once: the first
        terminal for a request wins and a later one is dropped instead of producing a second
        end for the same attempt.
        """
        if self.observability is None:
            return False
        if name in TERMINAL_EVENTS:
            if request is not None and request.get(TERMINAL_RECORDED):
                return False
            if request is not None:
                request[TERMINAL_RECORDED] = True
        return self.observability.event(name, **fields)

    def admission_fact(self, fact):
        """Turn one scheduler fact into its registered event; a pure observation port."""
        name = observation_events.QUEUE_FACT_EVENTS.get(fact.outcome)
        if name is None:
            return
        self.record_event(
            _CURRENT_REQUEST.get(),
            name,
            duration_ms=fact.wait_ms,
            error_code=observation_events.QUEUE_FACT_CODES.get(fact.outcome),
        )

    def receipt_failure(self, name):
        """The ledger's own fixed failure observation; the name is already registered."""
        if self.observability is None or name not in observation_events.REGISTRY:
            return
        self.observability.event(name, error_code=observation_events.UNKNOWN_ERROR_CODE)

    async def begin_upstream(self, request):
        """The durable record that must exist before this attempt may cause a side effect.

        Emitted with no scheduler lock, no database transaction and no forward write held. A
        record that is not on durable storage refuses the new business with the fixed 503: the
        attempt is not forwarded, no ledger row was created, and no earlier attempt is retried.
        """
        if self.observability is None:
            return True
        started = request.get(WORK_STARTED)
        durable = await self.observability.durable_event(
            "upstream.call_started",
            duration_ms=None if started is None else self.observability.elapsed_ms(started),
        )
        # A readiness change is reported here, by the path that just looked at the checks, and
        # never by a probe. The full evaluation runs only when something is wrong or when the
        # process had not established readiness yet, so the healthy path costs one attribute
        # read and the ledger is not queried on every request.
        if not durable or not self.observability.readiness_ok:
            self.sync_readiness()
        return durable

    def sync_readiness(self):
        """Determine readiness on a non-probe path and record the transition, if any."""
        if self.observability is None:
            return None
        status = probe_health.ready_status(self.check_providers().collect())
        self.observability.note_readiness(status)
        return status

    def note_platform(self):
        """A real, verified platform snapshot answered; not a cache hit and not a probe."""
        if self.observability is not None:
            self.observability.observe_platform()

    def attempt_terminal(self, reason):
        """The fixed (outcome, error_code) pair of an attempt reason; never a provider value."""
        return ATTEMPT_TERMINALS.get(reason, UNKNOWN_ATTEMPT_TERMINAL)

    def outcome_terminal(self, receipt):
        """The same fixed pair, read back from the receipt the transfer already produced."""
        return OUTCOME_TERMINALS.get(receipt.get("outcome"), UNKNOWN_ATTEMPT_TERMINAL)

    def finish_attempt(self, request, outcome, code, elapsed_ms, disconnected):
        """Close the observation of one attempt: its upstream terminal and the request terminal.

        Called after the authoritative receipt is written, so the runtime event never becomes
        the record of whether the attempt succeeded. The outcome is copied from the reason the
        transfer already produced; a logging failure here cannot change it, cannot retry it and
        cannot abort bytes already delivered.
        """
        if self.observability is None:
            return
        if outcome == "succeeded":
            self.observability.observe_model()
        self.observability.event(
            "upstream.call_finished", outcome=outcome, error_code=code, duration_ms=elapsed_ms
        )
        if disconnected:
            # The downstream connection broke after the headers were sent: that is the end of
            # this request, and it is reported as a disconnect rather than a completion.
            self.record_event(
                request, "request.disconnected", duration_ms=elapsed_ms, error_code="result_unknown"
            )
            return
        self.record_event(
            request, "request.finished", outcome=outcome, error_code=code, duration_ms=elapsed_ms
        )

    def note_first_output(self, request, latency_ms):
        """Record the first output of this attempt, once, with the transport's own timing."""
        if self.observability is None or request.get(FIRST_OUTPUT_RECORDED):
            return
        request[FIRST_OUTPUT_RECORDED] = True
        self.observability.event("upstream.first_output", duration_ms=latency_ms)

    # -- read-only readiness checks ------------------------------------------------------

    @property
    def assembled(self):
        """Whether this runtime is actually assembled and has not been closed."""
        return not self.closed

    def check_providers(self):
        """The eight fixed readiness checks, each a read of state this process already has."""
        return CheckProviders(
            configuration=self.configuration_check,
            contracts=self.contracts_check,
            runtime=self.runtime_check,
            ledger=self.ledger_check,
            logging=self.logging_check,
            platform=self.platform_check,
            model=self.model_check,
            native=self.native_check,
        )

    def configuration_check(self):
        """Required deployment inputs still resolve; nothing is written and nothing is fetched."""
        if not self.assembled:
            return "not_configured"
        try:
            for grant in (*self.settings.clients, *self.settings.native_clients):
                self.secrets.resolve(grant.credential_ref)
            self.secrets.resolve(self.settings.platform_credential_ref)
            if self.origin_renewal is not None:
                self.origin_renewal.check()
        except Rejected:
            return "failed"
        return "ok"

    def contracts_check(self):
        if not self.assembled:
            return "not_configured"
        if self.observability is None or not self.contract_verified:
            return "not_configured"
        return "ok"

    def runtime_check(self):
        return "ok" if self.assembled else "failed"

    def ledger_check(self):
        """Short read-only reachability of the ledger this process already has open."""
        if not self.assembled:
            return "failed"
        return "ok" if self.diagnostics.reachable() else "failed"

    def logging_check(self):
        if self.observability is None:
            return "not_configured"
        return self.observability.logging_status()

    def platform_check(self):
        """A remote dependency is ``ok`` only while a real success is recent enough."""
        if self.observability is None:
            return "not_verified"
        return self.observability.platform_observation.status()

    def model_check(self):
        if self.observability is None:
            return "not_verified"
        return self.observability.model_observation.status()

    def native_check(self):
        """Native is optional: switched off it is ``not_configured`` and blocks nothing."""
        if not self.settings.native_enabled:
            return "not_configured"
        if not self.assembled:
            return "failed"
        return "ok" if self.native_cache is not None else "failed"

    def requeue_reverify(self, request, grant, native, provider):
        """Re-read the caller's authority and credentials after the wait, before sending.

        A request that waited is not the same request it was when it joined the queue: the
        service credential can be withdrawn, a native grant can expire or be revoked, and the
        pinned configuration can lapse or be revoked. Every one of them is re-read here from
        the same authoritative sources the pre-queue path used -- the request's own bearer
        token is re-authenticated against the current registrations, and the provider
        credential reference is resolved again -- so a caller that lost its right to send is
        refused instead of being forwarded on authority it no longer holds.

        This never picks another provider, never retries and never substitutes a credential:
        it either confirms the attempt that was already selected or refuses it. The returned
        credential is the current value for the provider this attempt is pinned to.
        """
        current = self.authenticate_native(request) if native else self.authenticate(request)
        if grant_identity(current) != grant_identity(grant):
            # The token now belongs to a different registration: never send on the old one.
            raise Rejected("unauthorized", 401)
        if native:
            authorize(current)
        credential = self.secrets.resolve(provider["credential_ref"])
        self.targets.check(provider["base_url"])
        return credential

    def authenticate(self, request):
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if request.headers.get("Authorization", "") != "Bearer " + token or not token:
            raise Rejected("unauthorized", 401)
        matches = []
        for client in self.settings.clients:
            try:
                expected = self.secrets.resolve(client.credential_ref)
            except Rejected:
                continue  # Unset/revoked service credentials never authenticate.
            if hmac.compare_digest(token.encode(), expected.encode()):
                matches.append(client)
        if len(matches) != 1:
            raise Rejected("unauthorized", 401)
        return matches[0]

    def authenticate_native(self, request):
        """Native clients are their own registration; Chat credentials never open them."""
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if request.headers.get("Authorization", "") != "Bearer " + token or not token:
            raise Rejected("unauthorized", 401)
        matches = []
        for grant in self.settings.native_clients:
            try:
                expected = self.secrets.resolve(grant.credential_ref)
            except Rejected:
                continue  # Unset/revoked service credentials never authenticate.
            if hmac.compare_digest(token.encode(), expected.encode()):
                matches.append(grant)
        if len(matches) != 1:
            raise Rejected("unauthorized", 401)
        return matches[0]

    def native_context(self, request, grant):
        """Trusted request identity and native version selection for one native call.

        The Chat version header is rejected here and the native header is rejected on the
        Chat route, so equal integers can never mean the same configuration.
        """
        if any(len(request.headers.getall(h, [])) > 1 for h in NATIVE_ROUTE_HEADERS):
            raise Rejected()
        if any(h in request.headers for h in ("openai-organization", "openai-project")):
            raise Rejected("forbidden", 403)
        if "x-tianshu-config-version" in request.headers or "x-tianshu-workload" in request.headers:
            raise Rejected()
        if not grant.internal:
            if any(
                h.lower().startswith("x-tianshu-") or h.lower() == "x-request-id"
                for h in request.headers
            ):
                raise Rejected("forbidden", 403)
            request_id, turn_id = str(uuid.uuid4()), str(uuid.uuid4())
        else:
            if any(
                h.lower().startswith("x-tianshu-") and h.lower() not in NATIVE_HEADERS
                for h in request.headers
            ):
                raise Rejected()
            try:
                request_id = request.headers["x-request-id"]
                turn_id = request.headers["x-tianshu-turn-id"]
            except KeyError:
                raise Rejected() from None
            for value in (request_id, turn_id):
                self.contracts.validate("common#id", value)
                if any(secret in value for secret in self.secrets.known_values()):
                    raise Rejected()
        authorize(grant)
        version = requested_version(grant, request.headers.get(NATIVE_VERSION_HEADER))
        return request_id, version, turn_id

    def context(self, request, grant):
        if any(
            len(request.headers.getall(h, [])) > 1
            for h in INTERNAL_HEADERS | {"x-request-id", "authorization", "openai-beta"}
        ):
            raise Rejected()
        if any(h in request.headers for h in ("openai-organization", "openai-project")):
            raise Rejected("forbidden", 403)
        if not grant.internal:
            if any(
                h.lower().startswith("x-tianshu-") or h.lower() == "x-request-id"
                for h in request.headers
            ):
                raise Rejected("forbidden", 403)
            return str(uuid.uuid4()), grant.config_version, None
        try:
            version = request.headers["x-tianshu-config-version"]
            if not version.isascii() or not version.isdecimal() or str(int(version)) != version:
                raise ValueError()
            if NATIVE_VERSION_HEADER.lower() in request.headers:
                # Native versions are a different space; never reinterpret them as Chat.
                raise ValueError()
            context = {
                "request_id": request.headers["x-request-id"],
                "turn_id": request.headers["x-tianshu-turn-id"],
                "config_version": int(version),
                "workload": request.headers["x-tianshu-workload"],
                "protocol": PROTOCOL,
            }
        except (KeyError, ValueError):
            raise Rejected() from None
        self.contracts.validate("model#route_context", context)
        if any(secret in context["request_id"] for secret in self.secrets.known_values()):
            raise Rejected()
        if context["config_version"] not in (grant.config_version, *grant.allowed_versions):
            raise Rejected("forbidden", 403)
        return context["request_id"], context["config_version"], context["turn_id"]

    async def receipt(self, request):
        grant = self.authenticate(request)
        result = self.diagnostics.get(grant.service, request.match_info["request_id"])
        if result is None:
            raise Rejected("not_found", 404)
        return web.json_response(
            redact(result, self.secrets.known_values()), headers={"Cache-Control": "no-store"}
        )

    def usage_report(self, request, key_space, identity):
        """Read-only projection of the authenticated caller's own metric rows."""
        query = request.rel_url.query
        if any(len(query.getall(name)) != 1 for name in query):
            raise Rejected()
        return web.json_response(
            redact(
                build_report(
                    self.diagnostics.connection,
                    key_space,
                    identity,
                    {name: query[name] for name in query},
                ),
                self.secrets.known_values(),
            ),
            headers={"Cache-Control": "no-store"},
        )

    async def usage(self, request):
        grant = self.authenticate(request)
        return self.usage_report(request, "chat", {"service": grant.service})

    async def native_usage(self, request):
        """Only a currently authorized native identity may read its own history."""
        grant = self.authenticate_native(request)
        authorize(grant)
        identity = dict(zip(NATIVE_IDENTITY_FIELDS, grant.identity(), strict=True))
        return self.usage_report(request, "native", identity)

    async def chat(self, request):
        grant = self.authenticate(request)
        request_id, version, turn_id = self.context(request, grant)
        request[REQUEST_ID] = request_id
        if (
            request.query_string
            or request.content_type != "application/json"
            or request.headers.get("Content-Encoding")
        ):
            raise Rejected()
        beta = request.headers.get("OpenAI-Beta", "")
        if len(beta) > 4096 or any(ord(c) < 32 or ord(c) > 126 for c in beta):
            raise Rejected()
        # The request is parsed and the pinned configuration read before capacity is taken:
        # the provider is an input to admission, never something the scheduler guesses, and a
        # request that will be rejected never occupies a permit while it is being rejected. A
        # body that does not arrive inside the read deadline is still the client-visible 408.
        try:
            async with asyncio.timeout(self.settings.request_read_timeout):
                raw = await read_limited(request.content, self.settings.max_request_bytes)
        except TimeoutError:
            raise Rejected("timeout", 408) from None
        body = loads(raw)
        self.contracts.validate("model#native_request", body)
        config = await self.cache.get(version)
        effective, provider, binding, receipt = prepare(
            self.contracts, body, config, grant, request_id, grant.internal
        )
        credential = self.secrets.resolve(provider["credential_ref"])
        self.targets.check(provider["base_url"])
        # One launch key per logical client call: a repeated request ID may not occupy a
        # second queue slot, and the key never leaves this attempt.
        slot = self.slot(
            request, grant.service, grant.internal, request_id, provider["provider_id"]
        )
        try:
            async with slot:
                # Everything below is re-verified after the wait, immediately before sending:
                # the caller's permit, the caller's own authority and credential, the pinned
                # configuration's validity and revocation state, and the ledger outcome. A
                # queued request that loses any of them is refused here instead of being
                # forwarded on authority it no longer holds.
                if not slot.live():
                    raise Rejected("timeout", 408)
                # Re-read from the pinned version, then re-authenticate the caller: an expired
                # or revoked registration, or a withdrawn credential, must not reach upstream.
                await self.cache.get(version)
                credential = self.requeue_reverify(request, grant, False, provider)
                secrets = self.secrets.known_values()
                receipt = redact(receipt, secrets)
                self.contracts.validate("model#route_receipt", receipt)
                # The attempt that is about to cause a side effect is put on durable storage
                # first, with no scheduler lock, no transaction and no forward write held. A
                # record that is not durable refuses this new business before anything is sent:
                # no ledger row exists yet and no upstream call is made, so nothing is retried
                # and nothing already delivered is disturbed.
                if not await self.begin_upstream(request):
                    raise Rejected("dependency_unavailable", 503)
                # A queued request is never recorded as forwarded: the ledger row is created
                # after admission and only for an attempt that is about to be sent.
                self.diagnostics.begin(receipt, turn_id)
                request[FORWARD_STARTED] = True
                payload = apply_fields(raw, effective, receipt["applied_policies"])
                return await self.forward(
                    request, payload, effective, provider, binding, credential, receipt, secrets
                )
        except AdmissionRefused as exc:
            raise Rejected(exc.reason, exc.status) from None
        except TimeoutError:
            raise Rejected("timeout", 408) from None

    async def native_read(self, request):
        """Receipt read bound to the trusted identity, never to the request ID alone.

        A registration that is revoked, expired, unpermitted or carries no native version
        allowlist is not a usable identity, so it cannot read even its own history. Native
        *version* revocation is a different concept: it stops new routing and never erases
        the audit rows, so it is deliberately not consulted here.
        """
        grant = self.authenticate_native(request)
        authorize(grant)
        result = self.native_ledger.native_get(grant.identity(), request.match_info["request_id"])
        if result is None:
            raise Rejected("not_found", 404)
        return web.json_response(
            redact(result, self.secrets.known_values()), headers={"Cache-Control": "no-store"}
        )

    async def native_responses(self, request):
        grant = self.authenticate_native(request)
        request_id, version, turn_id = self.native_context(request, grant)
        request[REQUEST_ID] = request_id
        if (
            request.query_string
            or request.content_type != "application/json"
            or request.headers.get("Content-Encoding")
        ):
            raise Rejected()
        # Parsed before capacity for the same reason as Chat: the pinned configuration selects
        # the provider, and a request that fails validation must not hold a permit. A body that
        # does not arrive inside the read deadline is the same client-visible refusal as before.
        try:
            async with asyncio.timeout(self.settings.request_read_timeout):
                try:
                    raw = await read_limited(request.content, self.settings.max_request_bytes)
                except Rejected:
                    raise Rejected("payload_too_large", 413) from None
        except TimeoutError:
            raise Rejected("timeout", 408) from None
        # Native scope/state rejection precedes published shape checks.
        body = validate_request(raw)
        self.contracts.validate("native#native_request", body)
        config = await self.native_cache.get(grant, version)
        binding, provider = select_native_route(config, grant, body)
        selected_version = config["native_config_version"]
        credential = self.secrets.resolve(provider["credential_ref"])
        self.targets.check(provider["base_url"])
        slot = self.slot(
            request, grant.service, grant.internal, request_id, provider["provider_id"]
        )
        try:
            async with slot:
                # Re-verified after the wait, immediately before anything is sent: the permit,
                # the native grant (expiry, revocation, permission, allowlist), the provider
                # credential, the pinned native version's validity and the ledger outcome. A
                # grant that lapsed while this request waited must not reach an upstream.
                # Re-read the pinned native version (validity, revocation, caller allowlist)
                # and the caller's own authority before anything is sent. A grant that lapsed
                # while this request waited must not reach an upstream.
                if not slot.live():
                    raise Rejected("timeout", 408)
                await self.native_cache.get(grant, version)
                credential = self.requeue_reverify(request, grant, True, provider)
                # The ledger key space stays the version this attempt pinned before waiting.
                if self.native_ledger.native_is_revoked(grant.identity(), selected_version):
                    raise Rejected("forbidden", 403)
                secrets = self.secrets.known_values()
                context = route_context(grant, provider, request_id, turn_id, selected_version)
                self.contracts.validate("native#route_context", context)
                receipt = route_receipt(
                    grant,
                    provider,
                    body,
                    request_id,
                    selected_version,
                    utcnow().isoformat().replace("+00:00", "Z"),
                )
                receipt = redact(receipt, (*secrets, credential))
                self.contracts.validate("native#route_receipt", receipt)
                # Same durable gate as Chat, and for the same reason: the native attempt that is
                # about to cause a side effect is on durable storage before it is sent.
                if not await self.begin_upstream(request):
                    raise Rejected("dependency_unavailable", 503)
                # Only a caller-supplied turn can be pinned; an external caller's turn id is a
                # per-request correlation value, not a repeated client turn.
                self.native_ledger.native_begin(receipt, turn_id if grant.internal else None)
                request[FORWARD_STARTED] = True
                # preserve_client: the client's own native bytes are the upstream payload.
                return await self.forward_native(
                    request, raw, binding, provider, credential, receipt, secrets
                )
        except AdmissionRefused as exc:
            raise Rejected(exc.reason, exc.status) from None
        except TimeoutError:
            raise Rejected("timeout", 408) from None

    async def forward_native(
        self, request, payload, binding, provider, credential, receipt, secrets
    ):
        """Stream native bytes downstream; never rewrite, retry or fabricate a terminal."""
        state = {"response": None, "pending": None, "stream": False}
        started = time.monotonic()

        def first_output():
            """At most once per attempt, and only after the bytes already went downstream."""
            self.note_first_output(request, int((time.monotonic() - started) * 1000))

        async def start_response(status, content_type):
            headers = {
                "Content-Type": content_type,
                "Cache-Control": "no-store",
                "X-Request-ID": receipt["request_id"],
            }
            if content_type == "text/event-stream":
                response = web.StreamResponse(
                    status=status, headers={**headers, "X-Accel-Buffering": "no"}
                )
                state["stream"] = True
                state["response"] = response
                request[STREAM_RESPONSE] = response
                await response.prepare(request)
            else:
                state["pending"] = (status, headers)

        async def write(chunk):
            response = state["response"]
            if response is None:
                status, headers = state["pending"]
                state["response"] = web.Response(body=chunk, status=status, headers=headers)
                return
            await response.write(chunk)

        try:
            await send_responses(
                self.session,
                self.targets,
                self.native_ledger,
                payload=payload,
                base_url=provider["base_url"],
                credential=credential,
                receipt=receipt,
                start_response=start_response,
                write=write,
                timeout=min(binding["timeout_ms"], self.settings.max_timeout_ms) / 1000,
                max_response_bytes=self.settings.max_response_bytes,
                max_request_bytes=self.settings.max_request_bytes,
                secrets=secrets,
                validate_response=lambda document: self.contracts.validate(
                    "native#native_response", document
                ),
                validate_receipt=lambda document: self.contracts.validate(
                    "native#route_receipt", document
                ),
                # A fixed observation seam: no chunk, no count, no content, and a failure inside
                # it is confined to the event side channel.
                on_first_output=first_output,
            )
        except Rejected:
            response = state["response"]
            if response is not None and response.prepared:
                # Headers are already on the wire: no local error frame can be appended.
                if request.transport:
                    request.transport.abort()
                outcome, code = self.outcome_terminal(receipt)
                self.finish_attempt(
                    request, outcome, code, int((time.monotonic() - started) * 1000), True
                )
                return response
            outcome, code = self.outcome_terminal(receipt)
            self.finish_attempt(
                request, outcome, code, int((time.monotonic() - started) * 1000), False
            )
            raise
        response = state["response"]
        if state["stream"]:
            await response.write_eof()
        outcome, code = self.outcome_terminal(receipt)
        self.finish_attempt(request, outcome, code, int((time.monotonic() - started) * 1000), False)
        return response

    async def forward(
        self, request, payload, body, provider, binding, credential, receipt, secrets
    ):
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + credential,
            "Accept": "text/event-stream" if body.get("stream") else "application/json",
            "Accept-Encoding": "identity",
        }
        if "OpenAI-Beta" in request.headers:
            beta = request.headers["OpenAI-Beta"]
            headers["OpenAI-Beta"] = beta
        response = None
        upstream_status = None
        observer = None
        reason = "transport_unknown"
        # True when the downstream connection broke after this attempt had already sent headers,
        # which is what separates "the client went away" from "the attempt failed".
        downstream_broken = False
        timeout = min(binding["timeout_ms"], self.settings.max_timeout_ms) / 1000
        started = time.monotonic()
        # True once a complete upstream response has been read and examined, which is what
        # separates "the provider reported no usage" from "no usage could be observed".
        inspected = False
        try:
            # Includes reading and downstream backpressure; no retry or redirect middleware.
            async with asyncio.timeout(timeout):
                async with self.session.post(
                    provider["base_url"] + "/chat/completions",
                    data=payload,
                    headers=headers,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as upstream:
                    upstream_status = upstream.status
                    upstream_id = upstream.headers.get("x-request-id")
                    if upstream_id and len(upstream_id) <= 256:
                        receipt["upstream_request_id"] = redact(upstream_id, secrets)
                    if not 200 <= upstream.status < 300:
                        receipt["outcome"] = "failed" if 400 <= upstream.status < 500 else "unknown"
                        reason = "upstream_http_error"
                        # Preserve status, never echo the raw provider error or headers.
                        return error_response(
                            Rejected("dependency_unavailable", upstream.status, "unknown"),
                            receipt["request_id"],
                        )
                    if upstream.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise Rejected("result_unknown", 502, "unknown")
                    content_type = upstream.content_type
                    if body.get("stream"):
                        if content_type != "text/event-stream":
                            raise Rejected("result_unknown", 502, "unknown")
                        observer = StreamObserver(body.get("n", 1), started=started)
                        guard = SecretGuard(secrets)
                        response = web.StreamResponse(
                            status=upstream.status,
                            headers={
                                "Content-Type": "text/event-stream",
                                "Cache-Control": "no-store",
                                "X-Accel-Buffering": "no",
                                "X-Request-ID": receipt["request_id"],
                            },
                        )
                        request[STREAM_RESPONSE] = response
                        await response.prepare(request)
                        async for chunk in upstream.content.iter_any():
                            observer.feed(chunk)
                            if observer.first_output_ms is not None:
                                self.note_first_output(request, observer.first_output_ms)
                            safe = guard.feed(chunk)
                            if safe:
                                await response.write(safe)
                        observer.end()
                        inspected = True
                        if not observer.complete:
                            reason = "incomplete_stream"
                            raise Rejected("result_unknown", 502, "unknown")
                        tail = guard.feed(b"", final=True)
                        if tail:
                            await response.write(tail)
                        await response.write_eof()
                        record_usage(receipt, observer.native_usage, True)
                    else:
                        if content_type != "application/json":
                            raise Rejected("result_unknown", 502, "unknown")
                        raw = await read_limited(upstream.content, self.settings.max_response_bytes)
                        SecretGuard(secrets).feed(raw, final=True)
                        native = loads(raw)
                        if isinstance(native, dict):
                            inspected = True
                        self.contracts.validate("model#native_response", native)
                        if "error" in native:
                            raise Rejected("result_unknown", 502, "unknown")
                        choices = native["choices"]
                        if (
                            len(choices) != body.get("n", 1)
                            or {c.get("index") for c in choices if type(c.get("index")) is int}
                            != set(range(body.get("n", 1)))
                            or any(
                                not isinstance(c.get("finish_reason"), str)
                                or not c["finish_reason"]
                                for c in choices
                            )
                        ):
                            raise Rejected("result_unknown", 502, "unknown")
                        response = web.Response(
                            body=raw,
                            status=upstream.status,
                            headers={
                                "Content-Type": "application/json",
                                "Cache-Control": "no-store",
                                "X-Request-ID": receipt["request_id"],
                            },
                        )
                        record_usage(receipt, native.get("usage"), True)
                        self.note_first_output(request, int((time.monotonic() - started) * 1000))
                    receipt["outcome"], reason = "succeeded", "completed"
                    return response
        except asyncio.CancelledError:
            reason = "cancelled_unknown"
            if observer:
                record_usage(receipt, observer.native_usage, False)
            raise
        except (aiohttp.ClientError, TimeoutError, ConnectionError, Rejected) as exc:
            if isinstance(exc, TimeoutError):
                reason = "timeout_unknown"
            if observer:
                record_usage(receipt, observer.native_usage, False)
            if response is not None and response.prepared:
                # Do not fabricate [DONE] or close a truncated SSE body as a successful HTTP message.
                downstream_broken = True
                if request.transport:
                    request.transport.abort()
                return response
            return error_response(Rejected("result_unknown", 502, "unknown"), receipt["request_id"])
        finally:
            receipt["observed_at"] = utcnow().isoformat().replace("+00:00", "Z")
            safe_receipt = redact(receipt, secrets)
            self.contracts.validate("model#route_receipt", safe_receipt)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            self.diagnostics.finish(
                safe_receipt,
                reason,
                elapsed_ms,
                upstream_status,
                attempt_metrics(
                    safe_receipt, observer, elapsed_ms, bool(body.get("stream")), inspected
                ),
            )
            LOG.info(
                "model_request outcome=%s elapsed_ms=%d",
                receipt["outcome"],
                elapsed_ms,
            )
            # Recorded after the authoritative receipt, from the reason the transfer already
            # produced: the runtime event reports the attempt, it never decides it.
            outcome, code = self.attempt_terminal(reason)
            self.finish_attempt(request, outcome, code, elapsed_ms, downstream_broken)


def grant_identity(grant):
    """The registered identity of either grant kind, for comparing a re-read authority.

    Chat registrations and native grants have different fields; the comparison only needs to
    answer "is this still the same registration", so each kind supplies its own tuple and the
    two are never compared across kinds.
    """
    identity = getattr(grant, "identity", None)
    if callable(identity):
        return identity()
    return (grant.service, grant.credential_ref, grant.provider_id, grant.config_version)


def error_response(exc, request_id, contract=None):
    """Chat errors use the shared envelope; native paths use the published native one."""
    body = {
        "schema_version": 1,
        "request_id": request_id,
        "code": exc.code,
        "execution_state": exc.state,
        "retryable": False,
    }
    if contract is not None:
        body["contract"] = contract
    return web.json_response(
        body,
        status=exc.status,
        headers={"Cache-Control": "no-store", "X-Request-ID": request_id},
    )


def native_route(request):
    return (
        request.path == NATIVE_ROUTE_PATH
        or request.path.startswith(NATIVE_RECEIPT_PATH)
        or request.path == NATIVE_USAGE_PATH
    )


def provenance_event(exc):
    """The terminal event of a refusal, chosen by the refusal's own fixed code."""
    if exc.code == "unauthorized":
        return "request.unauthenticated"
    if exc.code == "forbidden":
        return "request.revoked"
    return "request.rejected"


def proven_code(exc):
    """A refusal's own code when it is a registered log code, otherwise the fixed fallback."""
    if exc.code in observation_events.ERROR_CODES:
        return exc.code
    return observation_events.UNKNOWN_ERROR_CODE


@web.middleware
async def errors(request, handler):
    request[REQUEST_ID] = str(uuid.uuid4())
    gateway = request.app.get(GATEWAY)
    # The two read-only probes are structurally excluded from observation here, so no later
    # change can accidentally give them a correlation, a business event or a success record.
    probe = request.path in PROBE_PATHS
    native = native_route(request)
    contract = NATIVE_CONTRACT if native else None
    token = None
    if not probe and gateway is not None and gateway.observability is not None:
        # The correlation of this request. A missing, malformed or unknown header produces a
        # fresh value and the presented string is never kept, echoed, logged or forwarded.
        token = bind_correlation(request.headers.get(CORRELATION_HEADER))
    try:
        response = await handler(request)
    except Rejected as exc:
        if not probe and gateway is not None:
            gateway.record_event(request, provenance_event(exc), error_code=proven_code(exc))
        return error_response(exc, request[REQUEST_ID], contract)
    except web.HTTPException as exc:
        if not probe and gateway is not None:
            if exc.status in ROUTE_UNMATCHED_STATUSES:
                # A path or method this gateway never published: no attempt, no forward, no
                # capacity was consumed, so the end of the request is an unmatched route.
                gateway.record_event(request, "request.route_unmatched", error_code="not_found")
            else:
                gateway.record_event(
                    request,
                    "request.rejected",
                    error_code=HTTP_STATUS_CODES.get(
                        exc.status, observation_events.UNKNOWN_ERROR_CODE
                    ),
                )
        if native:
            # The published native table has no 404/405 pair; an unknown method or path on
            # a native interface stays a fixed not_found rather than an unmapped status.
            return error_response(Rejected("not_found", 404), request[REQUEST_ID], contract)
        return error_response(Rejected("not_found", exc.status), request[REQUEST_ID])
    except asyncio.CancelledError:
        if not probe and gateway is not None:
            gateway.record_event(request, "request.cancelled")
        raise
    except Exception:
        if not probe and gateway is not None:
            gateway.record_event(
                request,
                "request.finished",
                outcome="unknown",
                error_code=observation_events.UNKNOWN_ERROR_CODE,
            )
        # No exception text/stack/request URL: third-party errors can contain credentials or body.
        LOG.error("gateway_internal_error")
        stream = request.get(STREAM_RESPONSE)
        if stream is not None and stream.prepared:
            if request.transport:
                request.transport.abort()
            return stream
        if request.get(FORWARD_STARTED):
            if native:
                return error_response(
                    Rejected("result_unknown", 502, "unknown"), request[REQUEST_ID], contract
                )
            return error_response(Rejected("result_unknown", 503, "unknown"), request[REQUEST_ID])
        return error_response(
            Rejected("dependency_unavailable", 503), request[REQUEST_ID], contract
        )
    else:
        # A route that answered without raising and without forwarding anything ends here: the
        # receipt read, the usage report and every other non-forwarding success. The terminal
        # record belongs to *this* request's correlation, so it is written inside the bound
        # context and before the context is released by the finally block below. A forwarded
        # attempt already recorded its own terminal in the transfer path.
        if not probe and gateway is not None:
            if response.status >= 400:
                gateway.record_event(
                    request,
                    "request.rejected",
                    error_code=HTTP_STATUS_CODES.get(
                        response.status, observation_events.UNKNOWN_ERROR_CODE
                    ),
                )
            else:
                gateway.record_event(request, "request.finished", outcome="succeeded")
        return response
    finally:
        if token is not None:
            reset_correlation(token)


def observed(handler):
    """Record the entry of one published business route, then run it unchanged.

    This is assembly, not domain behaviour: the wrapper adds no decision and observes the path
    the router already selected. The two probes are never wrapped, which is what makes "a probe
    records nothing" structural rather than remembered.
    """

    async def wrapper(request):
        gateway = request.app[GATEWAY]
        gateway.record_event(request, "request.accepted")
        request[WORK_STARTED] = time.monotonic()
        token = _CURRENT_REQUEST.set(request)
        try:
            return await handler(request)
        finally:
            _CURRENT_REQUEST.reset(token)

    return wrapper


GATEWAY = web.AppKey("gateway", Gateway)


class _CounterSlot:
    """The bounded pool is off: capacity is the historical counter with a fast refusal.

    This is the exact pre-TS-044 behaviour, kept for a deployment whose scheduling reserve is
    zero. It performs no waiting, records no admission row and keeps the same
    ``queue_full``/429 answer when either limit is busy.
    """

    __slots__ = ("gateway", "service", "provider_id", "_active")

    def __init__(self, gateway, request, service, provider_id):
        self.gateway, self.service, self.provider_id = gateway, service, provider_id
        self._active = False

    async def __aenter__(self):
        # The historical order and status are preserved exactly: the global limit is checked
        # first, then this provider's limit, and either busy limit is an immediate refusal.
        if self.gateway.active >= self.gateway.settings.max_concurrent:
            raise Rejected("queue_full", 429)
        if (
            self.gateway.provider_active.get(self.provider_id, 0)
            >= self.gateway.settings.max_provider_concurrent
        ):
            raise Rejected("queue_full", 429)
        self.gateway.active += 1
        self.gateway.provider_active[self.provider_id] = (
            self.gateway.provider_active.get(self.provider_id, 0) + 1
        )
        self._active = True
        return self

    async def __aexit__(self, *_):
        self.release()
        return False

    def live(self):
        """This path has no waiting, so an entered attempt always still holds its capacity."""
        return True

    def release(self):
        if not self._active:
            return
        self._active = False
        self.gateway.active -= 1
        self.gateway.provider_active[self.provider_id] -= 1


def closed_probe(status, checks):
    """One closed probe answer: ``status`` and ``checks`` are the only fields either probe has."""
    body = {
        "status": probe_health.NOT_READY,
        "service": probe_health.SERVICE,
        "checks": checks,
    }
    return web.Response(
        body=probe_health.encode_body(body),
        status=status,
        content_type=JSON_TYPE,
        headers=dict(NO_STORE),
    )


def create_app(settings, observability=None):
    settings.validate()
    contract = None
    if settings.diagnostics_contract_directory is not None:
        # Read-only load with manifest hash verification: a package that disagrees with its own
        # manifest, or a closed record that disagrees with this product's static catalogue,
        # stops the start instead of producing records nobody agreed on.
        contract = load_contract(settings.diagnostics_contract_directory)
        if observability is not None:
            observability.contract = contract
    contracts = Contracts(
        settings.contract_directory,
        settings.native_contract_directory if settings.native_enabled else None,
    )
    for client in settings.clients:
        contracts.validate("common#id", client.service)
        contracts.validate("common#id", client.provider_id)
    for grant in settings.native_clients:
        for value in (*grant.provider_ids, grant.service, grant.principal_id):
            contracts.validate("common#id", value)
        contracts.validate("common#id", grant.credential_namespace)
    targets = RegisteredTargets(settings.targets)
    targets.check(settings.platform_base_url)
    app = web.Application(middlewares=[errors], client_max_size=settings.max_request_bytes)

    async def resources(app):
        diagnostics = Diagnostics(settings.diagnostics_path)
        try:
            for version in settings.revoked_versions:
                diagnostics.revoke(version)
            for grant in settings.native_clients:
                for version in settings.revoked_native_versions:
                    # Independent key space: a native revocation never touches Chat rows.
                    diagnostics.native_revoke(grant.identity(), version)
            # One in-memory pool for the whole process: Chat and native draw from it, so the
            # same provider quota is never multiplied by the number of protocols or models.
            # The private ledger is wired in as the scheduler's fact sink by the Gateway; the
            # scheduler itself holds no store and performs no IO under its lock.
            scheduler = (
                BoundedAdmissionScheduler(settings.policy)
                if settings.policy.interactive_reserve > 0
                else None
            )
            # One pooled connection per possible in-flight attempt, plus the platform-config
            # call. Kept at the same size as the admission limit so a full pool is refused or
            # queued by the scheduler itself instead of blocking inside the connector.
            pool = max(settings.max_concurrent, settings.policy.max_in_flight)
            connector = aiohttp.TCPConnector(resolver=targets, limit=pool + 1, ttl_dns_cache=0)
            async with aiohttp.ClientSession(
                connector=connector,
                trust_env=False,
                auto_decompress=False,
                cookie_jar=aiohttp.DummyCookieJar(),
            ) as session:
                gateway = Gateway(
                    settings, contracts, targets, session, diagnostics, scheduler, observability
                )
                gateway.contract_verified = contract is not None
                app[GATEWAY] = gateway
                try:
                    if observability is not None:
                        # The lifecycle owns the sink; a successful origin check must precede
                        # runtime.started so an invalid bootstrap cannot be logged as started.
                        await observability.start()
                        observability.event("runtime.starting")
                    if gateway.origin_renewal is not None:
                        await gateway.origin_renewal.start()
                    if observability is not None:
                        # The first readiness this process determines for itself. It is not a
                        # probe, and it is what makes a later change a change.
                        gateway.sync_readiness()
                        observability.event("runtime.started")
                except BaseException:
                    if observability is not None:
                        # Nothing is serving. The failure is recorded on whatever channel is
                        # still usable -- the sink when it opened, the fixed emergency channel
                        # when it did not -- and the start is abandoned rather than degraded
                        # into a runtime that silently logs nothing.
                        observability.event("runtime.startup_failed", error_code="internal_error")
                    if gateway.origin_renewal is not None:
                        await gateway.origin_renewal.close()
                    raise
                try:
                    yield
                finally:
                    if gateway.origin_renewal is not None:
                        await gateway.origin_renewal.close()
        finally:
            gateway = app.get(GATEWAY)
            if gateway is not None:
                gateway.closed = True
            if observability is not None:
                # A bounded graceful teardown, recorded before the sink closes so the last
                # events of this process are on durable storage.
                observability.event("runtime.stopping")
                observability.event("runtime.stopped")
                await observability.shutdown()
            diagnostics.close()

    async def chat(request):
        return await request.app[GATEWAY].chat(request)

    async def receipt(request):
        return await request.app[GATEWAY].receipt(request)

    async def native_responses(request):
        return await request.app[GATEWAY].native_responses(request)

    async def native_receipt(request):
        return await request.app[GATEWAY].native_read(request)

    async def usage(request):
        return await request.app[GATEWAY].usage(request)

    async def native_usage(request):
        return await request.app[GATEWAY].native_usage(request)

    async def unsupported(request):
        request.app[GATEWAY].authenticate(request)
        # The terminal is recorded here, with this refusal's own code, before the middleware's
        # status-based fallback could name a different one.
        request.app[GATEWAY].record_event(request, "request.rejected", error_code="invalid_input")
        return error_response(Rejected("invalid_input", 501), request[REQUEST_ID])

    async def native_disabled(request):
        request.app[GATEWAY].authenticate(request)
        request.app[GATEWAY].record_event(
            request, "request.rejected", error_code="unsupported_operation"
        )
        return error_response(
            Rejected("unsupported_operation", 501), request[REQUEST_ID], NATIVE_CONTRACT
        )

    async def live(request):
        """Liveness is this process being able to answer: no dependency is consulted."""
        return web.Response(
            body=probe_health.encode_body(probe_health.LIVE_BODY),
            status=probe_health.STATUS_OK,
            content_type=JSON_TYPE,
            headers=dict(NO_STORE),
        )

    async def ready(request):
        """Readiness: eight checks, no write, no file, no network, no event of its own."""
        observation = request.app[GATEWAY].observability
        if observation is None:
            # No adapter is assembled, so nothing about readiness can be verified here. The
            # answer is the closed body with every check unverified, never a hopeful "ready".
            return closed_probe(
                probe_health.STATUS_UNAVAILABLE,
                dict.fromkeys(probe_health.CHECK_NAMES, "not_verified"),
            )
        status = probe_health.probe_authorization(
            request.headers.get("Authorization"), observation.probe_token()
        )
        if status != probe_health.STATUS_OK:
            # A probe that is not authorized learns the closed body and nothing else: no check
            # name is ever distinguished for a caller that cannot read readiness.
            return closed_probe(status, dict.fromkeys(probe_health.CHECK_NAMES, "not_verified"))
        status, body = observation.probe.evaluate(request.app[GATEWAY].check_providers())
        return web.Response(
            body=probe_health.encode_body(body),
            status=status,
            content_type=JSON_TYPE,
            headers=dict(NO_STORE),
        )

    app.cleanup_ctx.append(resources)
    app.router.add_post("/v1/chat/completions", observed(chat))
    app.router.add_get("/internal/v1/model-requests/{request_id}", observed(receipt))
    app.router.add_get(USAGE_PATH, observed(usage))
    # Neither probe is wrapped: a probe writes no event, takes no capacity and touches no
    # dependency, and that is a property of the wiring rather than of the handler body.
    app.router.add_get(LIVE_PATH, live)
    app.router.add_get(READY_PATH, ready)
    for path in ("/v1/messages", "/v1/embeddings"):
        app.router.add_route("*", path, observed(unsupported))
    if settings.native_enabled:
        app.router.add_post(NATIVE_ROUTE_PATH, observed(native_responses))
        app.router.add_get(NATIVE_RECEIPT_PATH + "{request_id}", observed(native_receipt))
        app.router.add_get(NATIVE_USAGE_PATH, observed(native_usage))
    else:
        # Native stays closed until a deployment explicitly enables it.
        app.router.add_route("*", NATIVE_ROUTE_PATH, observed(native_disabled))
        app.router.add_route("*", NATIVE_USAGE_PATH, observed(native_disabled))
    return app


def load_settings(raw):
    """Parse one deployment document; both entry points use the same construction."""
    data = loads(raw)
    data["clients"] = [ClientGrant(**client) for client in data["clients"]]
    if data.get("native_clients") is not None:
        data["native_clients"] = [NativeGrant(**grant) for grant in data["native_clients"]]
    # Both scheduling fields are optional; an existing document keeps the historical
    # behaviour without being edited, and a document that carries them is validated here.
    if data.get("scheduling") is not None:
        data["scheduling"] = SchedulingPolicy(**data["scheduling"])
    if data.get("workload_bindings") is not None:
        bindings = data["workload_bindings"]
        data["workload_bindings"] = Classification(
            bindings.get("bindings") or {}, bindings.get("default_class", "interactive")
        )
    # The observation block is optional and heavily defaulted: an existing document keeps the
    # previous behaviour (no runtime event, no health surface) without being edited, and a
    # document that carries the block is validated by ``Settings.validate`` before the bind.
    if data.get("observability") is not None:
        data["observability"] = ObservabilitySettings(**data["observability"])
    return Settings(**data)
