"""Small local SQLite ledger. No native bodies, credential values or exception text.

The Chat tables and the native tables are separate key spaces. A native version is never
written into the Chat ``configs``/``revoked`` chain, and native receipts are keyed by
contract plus trusted principal, caller service and credential namespace, so a request ID
alone never addresses another subject's record.

TS-043 adds two private metric tables for usage and latency diagnostics. They index only
facts the receipt chain cannot answer with a bounded SQL query: the wall-clock completion
instant, the three monotonic stream timings, the usage provenance and the normalized
token counts. Reason, upstream status and elapsed time are still read from the existing
rows, the vendor-native usage structure stays in the receipt, and no message body, tool
argument, credential or provider URL is duplicated here. The tables are added in place by
an explicit migration that first copies an existing deployment database, and the
pre-existing tables are never altered, so an older gateway build keeps opening the file.

The metric row is pure observation and is written in its own transaction, after the
authoritative terminal receipt. A failure to write it may never roll the receipt back,
abort bytes already delivered to a caller, or be retried: the attempt then simply has no
private row and the report counts it as unmetered instead of as zero.
"""

import hashlib
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass

from .contracts import Rejected

LOG = logging.getLogger("tianshu_gateway")


def _no_observation(name):
    """The default observation port: this module records no runtime event on its own."""
    del name
    return None


NATIVE_IDENTITY_FIELDS = ("contract", "principal_id", "caller_service", "credential_namespace")

CORE_TABLES = (
    "requests",
    "turns",
    "revoked",
    "configs",
    "native_configs",
    "native_revoked",
    "native_requests",
    "native_turns",
)
MIGRATION_VERSION = 1
# TS-044 adds the private admission table in a second, independent migration step. The
# ledger therefore records which steps were applied instead of inferring it from the newest
# table: a ledger migrated by the previous build keeps ``schema_migrations`` version 1 and
# only gains the new table, so the older build still opens the same file.
ADMISSION_MIGRATION = 2
BACKUP_SUFFIX = ".ts043-backup"
# Wait outcomes of one bounded-pool request. ``admitted`` means the caller proceeded to
# re-verify and forward; the others never reached an upstream send. A waiter whose consumer
# disappeared without a decision carries no outcome and no wait duration.
ADMISSION_OUTCOMES = ("admitted", "queue_full", "deadline_exceeded", "cancelled")
# Where an observed usage value came from. A missing value is never reported as zero, and
# an attempt that never inspected a complete upstream response is not "no usage reported".
USAGE_SOURCES = (
    "upstream_json_usage",
    "upstream_stream_usage",
    "not_reported",
    "unobserved",
)
# Largest normalized token count this private projection indexes (a thousand trillion
# tokens is already far beyond any real attempt). The bound keeps every stored value inside
# a SQLite signed 64-bit integer and keeps a bounded aggregate away from integer overflow:
# MAX_METRIC_TOKENS * MAX_LIMIT stays below 2**63. A larger upstream value is left in the
# authoritative receipt, is not indexed here, and leaves the projection explicitly
# incomplete instead of being truncated or invented.
MAX_METRIC_TOKENS = 10**15


def metric_tokens(value):
    """The token count this projection may index, or ``None`` for anything else.

    Values outside the documented bound are not clamped: they are not indexed at all, so a
    report can never present a truncated number as the value a provider reported.
    """
    if type(value) is not int or not 0 <= value <= MAX_METRIC_TOKENS:
        return None
    return value


def redact(value, secrets):
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[REDACTED]")
        return value
    if isinstance(value, list):
        return [redact(v, secrets) for v in value]
    if isinstance(value, dict):
        return {redact(k, secrets): redact(v, secrets) for k, v in value.items()}
    return value


def native_identity(document):
    """Trusted key tuple; every native ledger row is scoped to this identity."""
    return tuple(document[field] for field in NATIVE_IDENTITY_FIELDS)


@dataclass(frozen=True)
class AttemptMetrics:
    """New performance facts for one attempt; not a second copy of the receipt.

    Every timing is monotonic milliseconds since the attempt started and stays ``None``
    when it was not observed: a missing first-event latency is never recorded as zero and
    never stands in for model generation time.
    """

    first_upstream_byte_ms: int | None = None
    first_event_ms: int | None = None
    first_output_ms: int | None = None
    usage_source: str = "unobserved"
    input_tokens: int | None = None
    output_tokens: int | None = None
    usage_complete: bool = False

    def __post_init__(self):
        for name in ("first_upstream_byte_ms", "first_event_ms", "first_output_ms"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("invalid metric timing")
        if self.usage_source not in USAGE_SOURCES:
            raise ValueError("invalid usage source")
        if type(self.usage_complete) is not bool:
            raise ValueError("invalid usage completeness")
        for name in ("input_tokens", "output_tokens"):
            value = getattr(self, name)
            if value is not None and metric_tokens(value) is None:
                raise ValueError("invalid normalized token count")


UNOBSERVED = AttemptMetrics()


def attempt_metrics(receipt, observer, elapsed_ms, stream, inspected):
    """Project one finished attempt into private metrics; never raises.

    ``observer`` is the bounded SSE observer of this attempt, or ``None`` for a
    non-streaming call. ``inspected`` says whether a complete upstream response was read
    and examined, which is what separates "the provider reported no usage" from "no usage
    could be observed". The normalized input/output counts are read from the receipt's
    projection; the vendor-native structure is deliberately not copied or aggregated.

    A value the private projection cannot index (for example an upstream token count
    beyond the SQLite-safe bound) is not indexed here: it stays authoritative in the
    receipt, the row is marked incomplete, and no truncated number is ever presented as
    the reported one. This projection is observation only, so any unexpected failure
    degrades to :data:`UNOBSERVED` instead of reaching the transfer path.
    """
    try:
        return _project_metrics(receipt, observer, elapsed_ms, stream, inspected)
    except Exception:
        # Observation must never change what a caller receives or what the receipt says.
        LOG.warning("metric_projection_degraded")
        return UNOBSERVED


def _project_metrics(receipt, observer, elapsed_ms, stream, inspected):
    usage = receipt.get("usage")
    usage = usage if isinstance(usage, dict) else {}

    if receipt.get("native_usage") is not None:
        source = "upstream_stream_usage" if stream else "upstream_json_usage"
    else:
        source = "not_reported" if inspected else "unobserved"

    def timing(attribute):
        value = getattr(observer, attribute, None) if observer is not None else None
        if type(value) is not int or value < 0:
            return None
        # Both values come from the same monotonic start; the clamp only removes the
        # sub-millisecond ordering of two successive clock reads.
        return min(value, elapsed_ms)

    input_tokens = metric_tokens(usage.get("input_tokens"))
    output_tokens = metric_tokens(usage.get("output_tokens"))
    return AttemptMetrics(
        first_upstream_byte_ms=timing("first_upstream_byte_ms"),
        first_event_ms=timing("first_event_ms"),
        first_output_ms=timing("first_output_ms"),
        usage_source=source,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        # A row is complete only when it actually indexed both normalized values: an
        # upstream count that could not be indexed leaves the projection incomplete even
        # though the authoritative receipt still says the provider reported both.
        usage_complete=(
            receipt.get("usage_complete") is True
            and input_tokens is not None
            and output_tokens is not None
        ),
    )


PRIVATE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS request_metrics (
        service TEXT NOT NULL, request_id TEXT NOT NULL, completed_at_ms INTEGER NOT NULL,
        first_upstream_byte_ms INTEGER, first_event_ms INTEGER, first_output_ms INTEGER,
        usage_source TEXT NOT NULL, input_tokens INTEGER, output_tokens INTEGER,
        usage_complete INTEGER NOT NULL,
        PRIMARY KEY(service, request_id));
    CREATE INDEX IF NOT EXISTS request_metrics_window
        ON request_metrics(service, completed_at_ms, request_id);
    CREATE TABLE IF NOT EXISTS native_request_metrics (
        contract TEXT NOT NULL, principal_id TEXT NOT NULL, caller_service TEXT NOT NULL,
        credential_namespace TEXT NOT NULL, request_id TEXT NOT NULL,
        completed_at_ms INTEGER NOT NULL,
        first_upstream_byte_ms INTEGER, first_event_ms INTEGER, first_output_ms INTEGER,
        usage_source TEXT NOT NULL, input_tokens INTEGER, output_tokens INTEGER,
        usage_complete INTEGER NOT NULL,
        PRIMARY KEY(contract, principal_id, caller_service, credential_namespace, request_id));
    CREATE INDEX IF NOT EXISTS native_request_metrics_window
        ON native_request_metrics(contract, principal_id, caller_service, credential_namespace,
                                  completed_at_ms, request_id);
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
"""

PRIVATE_TABLES = ("request_metrics", "native_request_metrics")

# The admission projection answers one question the receipt chain cannot: how long a request
# waited inside the gateway's own bounded pool before capacity was granted, and what class
# the deployment had bound it to. It stores no upstream fact, no request content and no
# credential, and it is written by the scheduler, never by the forward path.
ADMISSION_SCHEMA = """
    CREATE TABLE IF NOT EXISTS admission_metrics (
        service TEXT NOT NULL, request_id TEXT NOT NULL, workload_class TEXT NOT NULL,
        outcome TEXT NOT NULL, wait_ms INTEGER,
        PRIMARY KEY(service, request_id));
"""


def table_names(connection):
    return {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


class Diagnostics:
    def __init__(self, path):
        self.path = str(path)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=1000")
        # The observation port is injected by the caller and defaults to silence, so this
        # module keeps owning only its tables and its transaction rules.
        self._observer = _no_observation
        # Read the table set before creating anything, so a fresh database is not
        # mistaken for an existing deployment that needs an upgrade copy.
        deployed = table_names(self.connection)
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS requests (
                service TEXT NOT NULL, request_id TEXT NOT NULL, receipt TEXT NOT NULL,
                reason TEXT NOT NULL, elapsed_ms INTEGER, upstream_status INTEGER,
                PRIMARY KEY(service, request_id));
            CREATE TABLE IF NOT EXISTS turns (
                service TEXT NOT NULL, turn_id TEXT NOT NULL, version INTEGER NOT NULL,
                PRIMARY KEY(service, turn_id));
            CREATE TABLE IF NOT EXISTS revoked (version INTEGER PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS configs (version INTEGER PRIMARY KEY, digest TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS native_configs (
                contract TEXT NOT NULL, principal_id TEXT NOT NULL, caller_service TEXT NOT NULL,
                credential_namespace TEXT NOT NULL, native_config_version INTEGER NOT NULL,
                digest TEXT NOT NULL,
                PRIMARY KEY(contract, principal_id, caller_service, credential_namespace,
                            native_config_version));
            CREATE TABLE IF NOT EXISTS native_revoked (
                contract TEXT NOT NULL, principal_id TEXT NOT NULL, caller_service TEXT NOT NULL,
                credential_namespace TEXT NOT NULL, native_config_version INTEGER NOT NULL,
                PRIMARY KEY(contract, principal_id, caller_service, credential_namespace,
                            native_config_version));
            CREATE TABLE IF NOT EXISTS native_requests (
                contract TEXT NOT NULL, principal_id TEXT NOT NULL, caller_service TEXT NOT NULL,
                credential_namespace TEXT NOT NULL, request_id TEXT NOT NULL, receipt TEXT NOT NULL,
                reason TEXT NOT NULL, elapsed_ms INTEGER, upstream_status INTEGER,
                PRIMARY KEY(contract, principal_id, caller_service, credential_namespace,
                            request_id));
            CREATE TABLE IF NOT EXISTS native_turns (
                contract TEXT NOT NULL, principal_id TEXT NOT NULL, caller_service TEXT NOT NULL,
                credential_namespace TEXT NOT NULL, turn_id TEXT NOT NULL,
                native_config_version INTEGER NOT NULL,
                PRIMARY KEY(contract, principal_id, caller_service, credential_namespace, turn_id));
        """)
        self.migration_backup = self.migrate(deployed)
        self.recover_executions()

    def recover_executions(self):
        """A previous process cannot still own an active task; never replay its call."""
        with self.connection:
            for table in ("requests", "native_requests"):
                rows = self.connection.execute(
                    f"SELECT rowid, receipt FROM {table} WHERE reason='in_flight'"
                ).fetchall()
                for row_id, raw in rows:
                    receipt = json.loads(raw)
                    execution = receipt.get("execution")
                    if isinstance(execution, dict):
                        execution.update(state="unknown", error_code="interrupted")
                    receipt["outcome"] = "unknown"
                    receipt["usage_complete"] = False
                    self.connection.execute(
                        f"UPDATE {table} SET receipt=?,reason='interrupted_unknown' WHERE rowid=?",
                        (json.dumps(receipt), row_id),
                    )

    def progress(self, receipt):
        self._progress("requests", (receipt["caller_service"], receipt["request_id"]), receipt)

    def native_progress(self, receipt):
        self._progress(
            "native_requests", (*native_identity(receipt), receipt["request_id"]), receipt
        )

    def _progress(self, table, identity, receipt):
        # Terminal rows are immutable to checkpoints or a racing cancellation request.
        columns = (
            ("service", "request_id")
            if table == "requests"
            else (*NATIVE_IDENTITY_FIELDS, "request_id")
        )
        where = " AND ".join(f"{column}=?" for column in columns)
        try:
            with self.connection:
                self.connection.execute(
                    f"UPDATE {table} SET receipt=? WHERE {where} AND reason='in_flight'",
                    (json.dumps(receipt), *identity),
                )
        except sqlite3.Error:
            # A checkpoint is observation, not permission to repeat or discard an attempt.
            # The final receipt still uses the existing mandatory terminal write.
            LOG.warning("execution_checkpoint_degraded")

    def migrate(self, deployed):
        """Apply the private-schema steps in order, copying an existing database first.

        Only new tables are created: the pre-existing tables keep their exact shape, so a
        deployment can move between this build and the previous one on the same file. An
        already populated database is copied once, before the private tables appear, to
        ``<path>{suffix}``. Each step is recorded in ``schema_migrations``, so a ledger that
        was already migrated by an earlier build is recognised instead of being migrated
        again, and a step that failed leaves no version row behind.
        """
        current = table_names(self.connection)
        steps = []
        if not set(PRIVATE_TABLES) <= current:
            steps.append((MIGRATION_VERSION, PRIVATE_SCHEMA))
        if "admission_metrics" not in current:
            steps.append((ADMISSION_MIGRATION, ADMISSION_SCHEMA))
        if not steps:
            return None
        backup = None
        if set(deployed) & set(CORE_TABLES) and self.path != ":memory:":
            candidate = self.path + BACKUP_SUFFIX
            if not os.path.exists(candidate):
                target = sqlite3.connect(candidate)
                try:
                    self.connection.backup(target)
                finally:
                    target.close()
                backup = candidate
        for version, script in steps:
            self.connection.executescript(script)
            with self.connection:
                self.connection.execute(
                    "INSERT OR REPLACE INTO schema_migrations VALUES (?,?)",
                    (version, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
                )
        return backup

    def applied_migrations(self):
        try:
            rows = self.connection.execute("SELECT version FROM schema_migrations").fetchall()
        except sqlite3.Error:
            # A ledger from before the private tables existed has no records at all.
            return set()
        return {row[0] for row in rows}

    def record_admission(self, service, request_id, workload_class, outcome, wait_ms):
        """Best-effort private projection of one admission decision.

        This is pure observation: it runs before the slot is granted (or after a refusal)
        and a failure here never changes the decision, never rolls back anything and never
        reaches the caller. A bounded, value-free warning is the only trace, and a request
        that could not be recorded simply has no admission row.
        """
        if outcome not in ADMISSION_OUTCOMES:
            raise ValueError("invalid admission outcome")
        if wait_ms is not None and (type(wait_ms) is not int or wait_ms < 0):
            raise ValueError("invalid admission wait")
        self.project_metric(
            "admission_metrics",
            "INSERT OR REPLACE INTO admission_metrics "
            "(service, request_id, workload_class, outcome, wait_ms) VALUES (?,?,?,?,?)",
            (service, request_id, workload_class, outcome, wait_ms),
        )

    def admission(self, service, request_id):
        row = self.connection.execute(
            "SELECT workload_class, outcome, wait_ms FROM admission_metrics "
            "WHERE service=? AND request_id=?",
            (service, request_id),
        ).fetchone()
        return None if row is None else tuple(row)

    def remember_config(self, version, stable):
        digest = hashlib.sha256(
            json.dumps(stable, sort_keys=True, ensure_ascii=True).encode()
        ).hexdigest()
        row = self.connection.execute(
            "SELECT digest FROM configs WHERE version=?", (version,)
        ).fetchone()
        if row and row[0] != digest:
            raise Rejected("version_conflict", 409)
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO configs VALUES (?,?)", (version, digest))

    def revoke(self, version):
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO revoked VALUES (?)", (version,))

    def is_revoked(self, version):
        return bool(
            self.connection.execute("SELECT 1 FROM revoked WHERE version=?", (version,)).fetchone()
        )

    def begin(self, receipt, turn_id):
        service, request_id = receipt["caller_service"], receipt["request_id"]
        try:
            with self.connection:
                if turn_id is not None:
                    row = self.connection.execute(
                        "SELECT version FROM turns WHERE service=? AND turn_id=?",
                        (service, turn_id),
                    ).fetchone()
                    if row and row[0] != receipt["config_version"]:
                        raise Rejected("version_conflict", 409)
                    self.connection.execute(
                        "INSERT OR IGNORE INTO turns VALUES (?,?,?)",
                        (service, turn_id, receipt["config_version"]),
                    )
                self.connection.execute(
                    "INSERT INTO requests (service,request_id,receipt,reason) VALUES (?,?,?,?)",
                    (service, request_id, json.dumps(receipt), "in_flight"),
                )
        except sqlite3.IntegrityError:
            raise Rejected("idempotency_conflict", 409) from None

    def finish(self, receipt, reason, elapsed_ms, upstream_status, metrics=UNOBSERVED):
        """Persist the authoritative terminal receipt, then the private metric row.

        The two writes are separate transactions on purpose. The receipt is the
        authoritative record of the attempt, so a failure to write it must still surface.
        The metric row is pure observation: a binding, constraint or disk failure there
        leaves the receipt authoritative, counts the attempt as unmetered in the report,
        and never changes the bytes already delivered to the caller. The metric row is
        keyed by the same identity as the receipt, so a repeated terminal observation
        replaces the earlier row instead of adding a second one.
        """
        try:
            with self.connection:
                self.connection.execute(
                    "UPDATE requests SET receipt=?,reason=?,elapsed_ms=?,upstream_status=? WHERE service=? AND request_id=?",
                    (
                        json.dumps(receipt),
                        reason,
                        elapsed_ms,
                        upstream_status,
                        receipt["caller_service"],
                        receipt["request_id"],
                    ),
                )
        except Exception:
            # The authoritative receipt could not be written. That failure still surfaces to
            # the caller; this observation only records that it happened, with a fixed code and
            # without any exception text, SQL or path.
            self._observe("receipt.persist_failed")
            raise
        self.project_metric(
            "request_metrics",
            "INSERT OR REPLACE INTO request_metrics "
            "(service, request_id, completed_at_ms, first_upstream_byte_ms, first_event_ms, "
            "first_output_ms, usage_source, input_tokens, output_tokens, usage_complete) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                receipt["caller_service"],
                receipt["request_id"],
                int(time.time() * 1000),
                metrics.first_upstream_byte_ms,
                metrics.first_event_ms,
                metrics.first_output_ms,
                metrics.usage_source,
                metrics.input_tokens,
                metrics.output_tokens,
                int(metrics.usage_complete),
            ),
        )

    def project_metric(self, table, statement, values):
        """Best-effort private projection of one finished attempt.

        Diagnostics never change the transfer outcome: a failure here is logged as a
        bounded, value-free degradation and the attempt keeps no private row. It is not
        retried, it does not roll back the receipt, and the report counts it under
        ``coverage.unmetered_total`` rather than as zero usage or zero latency.
        """
        try:
            with self.connection:
                self.connection.execute(statement, values)
        except Exception:
            # Neither the values nor the exception text is logged; both can carry
            # provider-supplied content and the missing row is already visible in the
            # report. Cancellation is a BaseException and still propagates.
            LOG.warning("metric_write_degraded table=%s", table)

    def get(self, service, request_id):
        row = self.connection.execute(
            "SELECT receipt FROM requests WHERE service=? AND request_id=?", (service, request_id)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def native_remember_config(self, identity, version, stable):
        digest = hashlib.sha256(
            json.dumps(stable, sort_keys=True, ensure_ascii=True).encode()
        ).hexdigest()
        row = self.connection.execute(
            "SELECT digest FROM native_configs WHERE contract=? AND principal_id=? AND caller_service=? AND credential_namespace=? AND native_config_version=?",
            (*identity, version),
        ).fetchone()
        if row and row[0] != digest:
            raise Rejected("version_conflict", 409)
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO native_configs VALUES (?,?,?,?,?,?)",
                (*identity, version, digest),
            )

    def native_revoke(self, identity, version):
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO native_revoked VALUES (?,?,?,?,?)", (*identity, version)
            )

    def native_is_revoked(self, identity, version):
        return bool(
            self.connection.execute(
                "SELECT 1 FROM native_revoked WHERE contract=? AND principal_id=? AND caller_service=? AND credential_namespace=? AND native_config_version=?",
                (*identity, version),
            ).fetchone()
        )

    def native_begin(self, receipt, turn_id):
        identity, version = native_identity(receipt), receipt["native_config_version"]
        try:
            with self.connection:
                if turn_id is not None:
                    row = self.connection.execute(
                        "SELECT native_config_version FROM native_turns WHERE contract=? AND principal_id=? AND caller_service=? AND credential_namespace=? AND turn_id=?",
                        (*identity, turn_id),
                    ).fetchone()
                    if row and row[0] != version:
                        raise Rejected("version_conflict", 409)
                    self.connection.execute(
                        "INSERT OR IGNORE INTO native_turns VALUES (?,?,?,?,?,?)",
                        (*identity, turn_id, version),
                    )
                self.connection.execute(
                    "INSERT INTO native_requests (contract,principal_id,caller_service,credential_namespace,request_id,receipt,reason) VALUES (?,?,?,?,?,?,?)",
                    (*identity, receipt["request_id"], json.dumps(receipt), "in_flight"),
                )
        except sqlite3.IntegrityError:
            raise Rejected("idempotency_conflict", 409) from None

    def native_finish(self, receipt, reason, elapsed_ms, upstream_status, metrics=UNOBSERVED):
        """Persist the authoritative native terminal receipt, then the private metric row.

        Same separation as the Chat terminal: a repeated observation replaces the metric
        row, and a metric failure never rolls back the native receipt or the delivered
        native bytes.
        """
        identity = native_identity(receipt)
        try:
            with self.connection:
                self.connection.execute(
                    "UPDATE native_requests SET receipt=?,reason=?,elapsed_ms=?,upstream_status=? WHERE contract=? AND principal_id=? AND caller_service=? AND credential_namespace=? AND request_id=?",
                    (
                        json.dumps(receipt),
                        reason,
                        elapsed_ms,
                        upstream_status,
                        *identity,
                        receipt["request_id"],
                    ),
                )
        except Exception:
            self._observe("receipt.persist_failed")
            raise
        self.project_metric(
            "native_request_metrics",
            "INSERT OR REPLACE INTO native_request_metrics "
            "(contract, principal_id, caller_service, credential_namespace, request_id, "
            "completed_at_ms, first_upstream_byte_ms, first_event_ms, first_output_ms, "
            "usage_source, input_tokens, output_tokens, usage_complete) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                *identity,
                receipt["request_id"],
                int(time.time() * 1000),
                metrics.first_upstream_byte_ms,
                metrics.first_event_ms,
                metrics.first_output_ms,
                metrics.usage_source,
                metrics.input_tokens,
                metrics.output_tokens,
                int(metrics.usage_complete),
            ),
        )

    def native_get(self, identity, request_id):
        row = self.connection.execute(
            "SELECT receipt FROM native_requests WHERE contract=? AND principal_id=? AND caller_service=? AND credential_namespace=? AND request_id=?",
            (*identity, request_id),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def observe(self, observer):
        """Inject the observation port; the default records nothing.

        This module keeps owning only its tables and its transaction rules: the port receives
        one fixed, already-registered event name and never a row, a statement or an exception.
        """
        self._observer = observer if callable(observer) else _no_observation
        return self

    def _observe(self, name):
        """Report one fixed observation. A failing port is swallowed, never propagated."""
        try:
            self._observer(name)
        except Exception:
            return

    def reachable(self):
        """Read-only reachability of the ledger this process already has open.

        Deliberately opens nothing: a readiness probe that connected on demand could create a
        database file, recover a journal or take a write lock, and none of that is allowed on
        a read-only probe. One read statement on the existing connection answers the only
        question readiness asks.
        """
        try:
            self.connection.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def close(self):
        self.connection.close()
