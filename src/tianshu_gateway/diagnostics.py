"""Small local SQLite ledger. No native bodies, credential values or exception text."""

import hashlib
import json
import sqlite3

from .contracts import Rejected


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

    def close(self):
        self.connection.close()
