"""Small local SQLite ledger. No native bodies, credential values or exception text.

The Chat tables and the native tables are separate key spaces. A native version is never
written into the Chat ``configs``/``revoked`` chain, and native receipts are keyed by
contract plus trusted principal, caller service and credential namespace, so a request ID
alone never addresses another subject's record.
"""

import hashlib
import json
import sqlite3

from .contracts import Rejected

NATIVE_IDENTITY_FIELDS = ("contract", "principal_id", "caller_service", "credential_namespace")


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


class Diagnostics:
    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=1000")
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

    def finish(self, receipt, reason, elapsed_ms, upstream_status):
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

    def native_finish(self, receipt, reason, elapsed_ms, upstream_status):
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

    def native_get(self, identity, request_id):
        row = self.connection.execute(
            "SELECT receipt FROM native_requests WHERE contract=? AND principal_id=? AND caller_service=? AND credential_namespace=? AND request_id=?",
            (*identity, request_id),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def close(self):
        self.connection.close()
