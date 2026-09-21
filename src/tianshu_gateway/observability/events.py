"""The one place an event name, level, outcome or error code is spelled.

Every record this product writes is one line of the frozen
``contracts/diagnostics/v1`` wire format (version 1.0.0). The registry below is a *static*
catalogue: business modules can only call a registered event, a registered level and a
registered outcome, and an error code can only come from the fixed whitelist. There is no
free-form mapping, no ``message``, no ``extras``, no exception text and no arbitrary object:
:func:`build_record` accepts fixed keywords only, so untrusted data has no route into a log.

The catalogue is checked against the published schema at start-up by
:func:`load_contract`: the closed field list, the enums and the patterns must match exactly.
That turns "we believe we implement the contract" into an equality assertion, and it fails
the start instead of silently drifting if the frozen package changes under us.
"""

import hashlib
import json
import re
import uuid
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Frozen wire identity. ``service`` is this product; the schema fixes the rest.
SCHEMA_VERSION = "1.0.0"
SERVICE = "gateway"
TIMESTAMP_FORMAT = "date-time"
UUID_FORMAT = "uuid"
SEQUENCE_MINIMUM = 1
DURATION_MINIMUM = 0

# The closed record, in the published field order, so a record is byte-deterministic.
FIELDS = (
    "schema_version",
    "timestamp",
    "service",
    "instance_id",
    "sequence",
    "event_id",
    "level",
    "event",
    "outcome",
    "correlation_id",
    "duration_ms",
    "error_code",
)

LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
OUTCOMES = (
    "started",
    "succeeded",
    "failed",
    "cancelled",
    "unknown",
    "rejected",
    "degraded",
)

EVENT_PATTERN = r"^[a-z][a-z0-9_.]{0,63}$"
ERROR_CODE_PATTERN = r"^[a-z][a-z0-9_]{0,63}$"
CORRELATION_PATTERN = r"^[a-f0-9]{32}$"
EVENT_RE = re.compile(EVENT_PATTERN)
ERROR_CODE_RE = re.compile(ERROR_CODE_PATTERN)
CORRELATION_RE = re.compile(CORRELATION_PATTERN)

# One record plus its LF delimiter may never exceed this, per the frozen contract.
MAX_LINE_BYTES = 4096
CORRELATION_HEADER = "X-Tianshu-Correlation-Id"

# Fixed local error codes. A code from anywhere else (a provider string, a request value, an
# exception attribute) is never passed through: it becomes ``internal_error`` instead.
ERROR_CODES = frozenset(
    {
        "internal_error",
        "unauthorized",
        "forbidden",
        "not_found",
        "invalid_input",
        "payload_too_large",
        "unsupported_operation",
        "state_reference_unsupported",
        "version_conflict",
        "idempotency_conflict",
        "unsupported_version",
        "budget_exceeded",
        "queue_full",
        "duplicate",
        "timeout",
        "dependency_unavailable",
        "result_unknown",
        "upstream_http_error",
        "configuration_invalid",
        "contract_invalid",
        "log_unavailable",
        "log_capacity_exceeded",
    }
)
UNKNOWN_ERROR_CODE = "internal_error"

# Fixed codes of the gateway's own refusal table that are also a log error code. A refusal
# whose code is absent here is logged as ``internal_error`` rather than passed through.
REJECTION_CODES = frozenset(
    {
        "unauthorized",
        "forbidden",
        "not_found",
        "invalid_input",
        "payload_too_large",
        "unsupported_operation",
        "state_reference_unsupported",
        "version_conflict",
        "idempotency_conflict",
        "unsupported_version",
        "budget_exceeded",
        "queue_full",
        "duplicate",
        "timeout",
        "dependency_unavailable",
        "result_unknown",
    }
)

# The scheduler's own fact names, mapped to this product's registered events. ``queued`` is
# formed for the event channel only; the private admission table keeps its own four outcomes.
QUEUE_FACT_EVENTS = {
    "queued": "request.queued",
    "admitted": "request.admitted",
    "queue_full": "request.queue_full",
    "deadline_exceeded": "request.queue_expired",
    "cancelled": "request.cancelled",
    "duplicate": "request.duplicate",
}
QUEUE_FACT_CODES = {
    "queue_full": "queue_full",
    "deadline_exceeded": "timeout",
    "duplicate": "duplicate",
}


@dataclass(frozen=True)
class EventSpec:
    """One registered event: its fixed level, its allowed outcomes and its fixed codes.

    ``codes`` is ``None`` when the event never carries an error code, and a frozenset when it
    may carry one; either way the value must also be in :data:`ERROR_CODES`.
    """

    name: str
    level: str
    outcomes: tuple[str, ...]
    codes: frozenset[str] | None = None

    @property
    def outcome(self):
        """The outcome used when a call site does not name one."""
        return self.outcomes[0]


def _spec(name, level, outcomes, codes=None):
    if not EVENT_RE.match(name):
        raise ValueError("invalid registered event name")
    if level not in LEVELS:
        raise ValueError("invalid registered level")
    if not outcomes or any(outcome not in OUTCOMES for outcome in outcomes):
        raise ValueError("invalid registered outcome")
    if codes is not None and not codes <= ERROR_CODES:
        raise ValueError("invalid registered error code")
    return EventSpec(name, level, outcomes, codes)


# Startup, configuration and contract verification.
_RUNTIME = (
    _spec("runtime.starting", "INFO", ("started",)),
    _spec("runtime.started", "INFO", ("succeeded",)),
    _spec("runtime.startup_failed", "CRITICAL", ("failed",), frozenset({UNKNOWN_ERROR_CODE})),
    _spec("runtime.stopping", "INFO", ("started",)),
    _spec("runtime.stopped", "INFO", ("succeeded",)),
    _spec("configuration.loaded", "INFO", ("succeeded",)),
    _spec(
        "configuration.invalid",
        "ERROR",
        ("rejected",),
        frozenset({"configuration_invalid"}),
    ),
    _spec("contracts.verified", "INFO", ("succeeded",)),
    _spec("contracts.invalid", "ERROR", ("rejected",), frozenset({"contract_invalid"})),
)

# Readiness transitions. Emitted by assembly and explicit maintenance only: a readiness
# probe is read-only and never writes an event of its own.
_HEALTH = (_spec("health.readiness_changed", "INFO", ("succeeded", "degraded")),)

# One business request: entry, authentication, refusals, terminal.
_REQUEST = (
    _spec("request.accepted", "INFO", ("succeeded",)),
    _spec("request.authenticated", "INFO", ("succeeded",)),
    _spec("request.unauthenticated", "WARNING", ("rejected",), frozenset({"unauthorized"})),
    _spec(
        "request.rejected",
        "WARNING",
        ("rejected",),
        REJECTION_CODES | frozenset({UNKNOWN_ERROR_CODE}),
    ),
    _spec(
        "request.finished",
        "INFO",
        ("succeeded", "failed", "cancelled", "unknown", "rejected"),
        REJECTION_CODES | frozenset({"upstream_http_error", UNKNOWN_ERROR_CODE}),
    ),
    _spec("request.route_unmatched", "WARNING", ("rejected",), frozenset({"not_found"})),
    _spec("request.disconnected", "WARNING", ("cancelled",), frozenset({"result_unknown"})),
)

# Bounded admission. These mirror the scheduler's own facts; they are not new decisions.
_QUEUE = (
    _spec("request.queued", "INFO", ("started",)),
    _spec("request.admitted", "INFO", ("succeeded",)),
    _spec("request.queue_full", "WARNING", ("rejected",), frozenset({"queue_full"})),
    _spec("request.queue_expired", "WARNING", ("failed",), frozenset({"timeout"})),
    _spec("request.cancelled", "INFO", ("cancelled",)),
    _spec("request.duplicate", "WARNING", ("rejected",), frozenset({"duplicate"})),
    _spec("request.revoked", "WARNING", ("rejected",), frozenset({"forbidden"})),
)

# The single upstream attempt. No token, chunk, chunk count or provider body is an event.
_UPSTREAM = (
    _spec("upstream.call_started", "INFO", ("started",)),
    _spec("upstream.first_output", "INFO", ("started",)),
    _spec(
        "upstream.call_finished",
        "INFO",
        ("succeeded", "failed", "cancelled", "unknown"),
        REJECTION_CODES | frozenset({"upstream_http_error", UNKNOWN_ERROR_CODE}),
    ),
)

# The authoritative receipt is the business record; only its persistence *failure* is an event.
_RECEIPT = (_spec("receipt.persist_failed", "ERROR", ("failed",), frozenset({"internal_error"})),)

# The log adapter's own lifecycle. Degradations use the emergency channel, not the failed file.
_LOG = (
    _spec("log.unavailable", "ERROR", ("failed",), frozenset({"log_unavailable"})),
    _spec(
        "log.capacity_exceeded",
        "ERROR",
        ("failed",),
        frozenset({"log_capacity_exceeded"}),
    ),
    _spec("log.recovered", "INFO", ("succeeded",)),
    _spec("log.segment_sealed", "INFO", ("succeeded",)),
    _spec("log.non_durable", "WARNING", ("degraded",)),
    _spec(
        "maintenance.log_recovery_check",
        "INFO",
        ("succeeded", "failed"),
        frozenset({"log_unavailable"}),
    ),
)

CATALOGUE = _RUNTIME + _HEALTH + _REQUEST + _QUEUE + _UPSTREAM + _RECEIPT + _LOG
REGISTRY = {spec.name: spec for spec in CATALOGUE}
if len(REGISTRY) != len(CATALOGUE):
    raise ValueError("duplicate registered event name")


@dataclass(frozen=True)
class DiagnosticsContract:
    """The verified frozen package. Read-only: nothing here is a second authority.

    Only the four files the package's own manifest lists are read, each hash-checked, and the
    published schema is compared field-by-field with the product's static catalogue.
    """

    directory: str
    version: str
    status: str
    files: tuple[tuple[str, str], ...]
    manifest_sha256: str
    schema_sha256: str


def _hashed(path, expected):
    """Read one published file and verify it against the digest its own manifest pins.

    The published ``diagnostics/v1`` manifest digests the bytes as stored, so the raw content
    is tried first. A checkout that converted line endings would otherwise hash differently for
    a reason that has nothing to do with the agreement, so the LF-normalized form of the same
    bytes is accepted as well. Anything else is a mismatch and stops the start.
    """
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() == expected:
        return content
    normalized = content.replace(b"\r\n", b"\n")
    if hashlib.sha256(normalized).hexdigest() == expected:
        return normalized
    raise ValueError("published contract hash mismatch")


def load_contract(directory):
    """Load and verify ``contracts/diagnostics/v1`` against this product's catalogue.

    Raises ``ValueError`` when a digest, the release version or any part of the closed record
    disagrees with the static registry. An unpublished or edited package therefore fails the
    start instead of producing records nobody agreed on.
    """
    root = Path(directory).resolve()
    manifest_bytes = (root / "manifest.json").read_bytes().replace(b"\r\n", b"\n")
    manifest = json.loads(manifest_bytes)
    if manifest.get("version") != SCHEMA_VERSION:
        raise ValueError("unsupported contract release")
    entries = manifest.get("files")
    if not isinstance(entries, dict) or "event.schema.json" not in entries:
        raise ValueError("published contract manifest is incomplete")
    for name, expected in sorted(entries.items()):
        _hashed(root / name, expected)
    schema_bytes = _hashed(root / "event.schema.json", entries["event.schema.json"])
    schema = json.loads(schema_bytes)
    _verify_schema(schema)
    return DiagnosticsContract(
        directory=str(root),
        version=manifest["version"],
        status=str(manifest.get("status", "")),
        files=tuple(sorted(entries.items())),
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        schema_sha256=hashlib.sha256(schema_bytes).hexdigest(),
    )


def _verify_schema(schema):
    """The published schema and the static catalogue must agree exactly."""
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        raise ValueError("contract record is not closed")
    properties = schema.get("properties")
    if not isinstance(properties, dict) or tuple(properties) != FIELDS:
        raise ValueError("contract field set differs from the registered record")
    if tuple(schema.get("required", ())) != FIELDS:
        raise ValueError("contract required fields differ from the registered record")
    fixed = (
        ("schema_version", "const", SCHEMA_VERSION),
        ("timestamp", "format", TIMESTAMP_FORMAT),
        ("instance_id", "format", UUID_FORMAT),
        ("event_id", "format", UUID_FORMAT),
        ("event", "pattern", EVENT_PATTERN),
        ("correlation_id", "pattern", CORRELATION_PATTERN),
        ("error_code", "pattern", ERROR_CODE_PATTERN),
        ("sequence", "minimum", SEQUENCE_MINIMUM),
        ("duration_ms", "minimum", DURATION_MINIMUM),
    )
    for field, keyword, expected in fixed:
        if properties[field].get(keyword) != expected:
            raise ValueError("contract field disagrees with the registered record")
    if tuple(properties["level"].get("enum", ())) != LEVELS:
        raise ValueError("contract levels differ from the registered record")
    if tuple(properties["outcome"].get("enum", ())) != OUTCOMES:
        raise ValueError("contract outcomes differ from the registered record")
    if SERVICE not in properties["service"].get("enum", ()):
        raise ValueError("this service is not a registered contract service")
    for spec in CATALOGUE:
        if not EVENT_RE.match(spec.name):
            raise ValueError("registered event name is not a contract event name")
    for code in ERROR_CODES:
        if not ERROR_CODE_RE.match(code):
            raise ValueError("registered error code is not a contract error code")


def new_correlation():
    """A fresh opaque correlation value: 32 lowercase hex, never derived from an identity."""
    return uuid.uuid4().hex


def normalize_correlation(value):
    """The caller's header value when it is a valid correlation value, otherwise ``None``.

    Nothing else about the value is inspected, kept or echoed, so an invalid or hostile string
    cannot reach a record or a response.
    """
    if isinstance(value, str) and CORRELATION_RE.match(value):
        return value
    return None


_CORRELATION: ContextVar[str | None] = ContextVar("tianshu_correlation", default=None)


def current_correlation():
    """The correlation of the request being served, if any; used for peer propagation."""
    return _CORRELATION.get()


def bind_correlation(value):
    """Set the current correlation for the duration of one request; returns the token."""
    return _CORRELATION.set(normalize_correlation(value) or new_correlation())


def reset_correlation(token: Token):
    _CORRELATION.reset(token)


def timestamp(now=None):
    """UTC RFC 3339 wall clock, milliseconds; used for ordering only, never for a duration."""
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        raise ValueError("timezone-aware timestamp required")
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def duration_ms(started, finished, clock):
    """Nonnegative whole milliseconds between two monotonic readings.

    A clock that appears to move backwards can only produce ``0`` here: a duration is never
    reported as negative and never recomputed from the wall clock.
    """
    elapsed = (finished if finished is not None else clock()) - started
    if elapsed <= 0:
        return 0
    return int(elapsed * 1000)


def build_record(
    name,
    *,
    instance_id,
    sequence,
    event_id,
    now=None,
    outcome=None,
    correlation_id=None,
    duration_ms=None,
    error_code=None,
):
    """Build one closed contract record. Any disagreement is a local programming error.

    Only these keywords exist. A value that is not a registered event, level, outcome or code
    raises ``ValueError`` instead of being written, which is what keeps provider text, user
    values and exception detail out of the log by construction.
    """
    spec = REGISTRY.get(name)
    if spec is None:
        raise ValueError("unregistered event")
    record = {
        "schema_version": SCHEMA_VERSION,
        "timestamp": timestamp(now),
        "service": SERVICE,
        "instance_id": str(instance_id),
        "sequence": sequence,
        "event_id": str(event_id),
        "level": spec.level,
        "event": spec.name,
        "outcome": outcome if outcome is not None else spec.outcome,
        "correlation_id": correlation_id,
        "duration_ms": duration_ms,
        "error_code": error_code,
    }
    validate_record(record)
    return record


def validate_record(record):
    """Cheap structural check of one record against the frozen closed schema."""
    if not isinstance(record, dict) or tuple(record) != FIELDS:
        raise ValueError("record is not the closed contract record")
    spec = REGISTRY.get(record["event"])
    if spec is None:
        raise ValueError("unregistered event")
    if record["schema_version"] != SCHEMA_VERSION or record["service"] != SERVICE:
        raise ValueError("invalid record identity")
    # The level is fixed by the catalogue, so a record whose level disagrees with its own event
    # is not a record this catalogue could have produced.
    if record["level"] != spec.level:
        raise ValueError("level not registered for this event")
    if record["outcome"] not in spec.outcomes:
        raise ValueError("outcome not registered for this event")
    if not isinstance(record["instance_id"], str) or not isinstance(record["event_id"], str):
        raise ValueError("invalid record identifiers")
    sequence = record["sequence"]
    if type(sequence) is not int or sequence < SEQUENCE_MINIMUM:
        raise ValueError("invalid record sequence")
    correlation = record["correlation_id"]
    if correlation is not None and not CORRELATION_RE.match(correlation):
        raise ValueError("invalid correlation")
    duration = record["duration_ms"]
    if duration is not None and (type(duration) is not int or duration < DURATION_MINIMUM):
        raise ValueError("invalid duration")
    code = record["error_code"]
    # A code is optional everywhere; when present it must be a registered code *and* one this
    # particular event is allowed to carry. An event that never carries a code refuses one.
    if code is not None:
        if code not in ERROR_CODES or spec.codes is None or code not in spec.codes:
            raise ValueError("error code not registered for this event")
    if not isinstance(record["timestamp"], str) or not record["timestamp"].endswith("Z"):
        raise ValueError("invalid timestamp")
    return record


def encode_record(record):
    """One UTF-8 JSON line with LF. Non-finite numbers and oversized lines are refused."""
    line = (
        json.dumps(
            record, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=False
        ).encode("utf-8")
        + b"\n"
    )
    if len(line) > MAX_LINE_BYTES:
        raise ValueError("record exceeds the contract line budget")
    return line


def decode_line(raw):
    """Parse one stored line back into a record; used by verification and recovery."""
    record = json.loads(raw.decode("utf-8"))
    return validate_record(record)
