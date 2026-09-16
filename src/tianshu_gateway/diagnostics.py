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
"""

import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass

from .contracts import Rejected

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
BACKUP_SUFFIX = ".ts043-backup"
# Where an observed usage value came from. A missing value is never reported as zero, and
# an attempt that never inspected a complete upstream response is not "no usage reported".
USAGE_SOURCES = (
    "upstream_json_usage",
    "upstream_stream_usage",
    "not_reported",
    "unobserved",
)


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
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("invalid normalized token count")


UNOBSERVED = AttemptMetrics()


def attempt_metrics(receipt, observer, elapsed_ms, stream, inspected):
    """Project one finished attempt into private metrics.

    ``observer`` is the bounded SSE observer of this attempt, or ``None`` for a
    non-streaming call. ``inspected`` says whether a complete upstream response was read
    and examined, which is what separates "the provider reported no usage" from "no usage
    could be observed". The normalized input/output counts are read from the receipt's
    projection; the vendor-native structure is deliberately not copied or aggregated.
    """
    usage = receipt.get("usage")
    usage = usage if isinstance(usage, dict) else {}

    def tokens(key):
        value = usage.get(key)
        return value if type(value) is int and value >= 0 else None

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

    return AttemptMetrics(
        first_upstream_byte_ms=timing("first_upstream_byte_ms"),
        first_event_ms=timing("first_event_ms"),
        first_output_ms=timing("first_output_ms"),
        usage_source=source,
        input_tokens=tokens("input_tokens"),
        output_tokens=tokens("output_tokens"),
        usage_complete=receipt.get("usage_complete") is True,
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

    def migrate(self, deployed):
        """Add the private metric tables, copying an existing database first.

        Only new tables are created: the pre-existing tables keep their exact shape, so a
        deployment can move between this build and the previous one on the same file. An
        already populated database is copied once, before the tables appear, to
        ``<path>{suffix}``.
        """
        current = table_names(self.connection)
        if set(PRIVATE_TABLES) <= current:
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
        self.connection.executescript(PRIVATE_SCHEMA)
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO schema_migrations VALUES (?,?)",
                (MIGRATION_VERSION, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            )
        return backup

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
        """Persist the terminal receipt and this attempt's private metric row together.

        The metric row is keyed by the same identity as the receipt, so a repeated
        terminal observation replaces the earlier row instead of adding a second one.
        """
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
            self.connection.execute(
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
        identity = native_identity(receipt)
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
            self.connection.execute(
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

    def close(self):
        self.connection.close()
