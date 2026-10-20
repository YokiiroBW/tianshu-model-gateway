"""Native PostgreSQL schema and one-time import for the gateway's own ledger."""

import hashlib
import json
import time

from tianshu_storage import PostgresConnection, StorageError

from .ledger_schema import ADMISSION_SCHEMA, CORE_SCHEMA, PRIVATE_SCHEMA

TABLE_KEYS = {
    "requests": ("service", "request_id"),
    "turns": ("service", "turn_id"),
    "revoked": ("version",),
    "configs": ("version",),
    "native_configs": (
        "contract",
        "principal_id",
        "caller_service",
        "credential_namespace",
        "native_config_version",
    ),
    "native_revoked": (
        "contract",
        "principal_id",
        "caller_service",
        "credential_namespace",
        "native_config_version",
    ),
    "native_requests": (
        "contract",
        "principal_id",
        "caller_service",
        "credential_namespace",
        "request_id",
    ),
    "native_turns": (
        "contract",
        "principal_id",
        "caller_service",
        "credential_namespace",
        "turn_id",
    ),
    "request_metrics": ("service", "request_id"),
    "native_request_metrics": (
        "contract",
        "principal_id",
        "caller_service",
        "credential_namespace",
        "request_id",
    ),
    "schema_migrations": ("version",),
    "admission_metrics": ("service", "request_id"),
}


def postgres_connection(connection):
    return isinstance(connection, PostgresConnection)


def table_names(connection):
    if postgres_connection(connection):
        return {
            row[0]
            for row in connection.native(
                "SELECT tablename FROM pg_tables WHERE schemaname=current_schema()"
            )
        }
    return {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def row_digest(cursor):
    digest = hashlib.sha256()
    count = 0
    for row in cursor:
        encoded = json.dumps(tuple(row), ensure_ascii=True, separators=(",", ":")).encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        count += 1
    return count, digest.digest()


def prepare_ledger(connection, source):
    initialized = (
        connection.native(
            "SELECT state FROM __tianshu_storage_binding WHERE singleton=1"
        ).fetchone()[0]
        == "ready"
    )
    if initialized and not TABLE_KEYS.keys() <= table_names(connection):
        raise StorageError("gateway_postgresql_ledger_incomplete")
    # SQLite INTEGER facts are signed 64-bit. PG INTEGER would truncate timestamps,
    # normalized usage and configuration versions, so use BIGINT explicitly.
    for schema in (CORE_SCHEMA, PRIVATE_SCHEMA, ADMISSION_SCHEMA):
        connection.native(schema.replace("INTEGER", "BIGINT"))
    if source is not None:
        existing = table_names(source)
        for table, keys in TABLE_KEYS.items():
            if table not in existing:
                continue
            columns = [row[1] for row in source.execute(f"PRAGMA table_info({table})")]
            target_columns = [
                row[0]
                for row in connection.native(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=current_schema() AND table_name=? ORDER BY ordinal_position",
                    (table,),
                )
            ]
            if columns != target_columns:
                raise StorageError("gateway_sqlite_schema_unsupported")
            selected = ",".join(columns)
            statement = (
                f"INSERT INTO {table} ({selected}) VALUES ({','.join(['%s'] * len(columns))})"
            )
            rows = source.execute(f"SELECT {selected} FROM {table}")
            with connection.raw.cursor() as target:
                while batch := rows.fetchmany(256):
                    target.executemany(statement, [tuple(row) for row in batch])
            ordered = f"SELECT {selected} FROM {table} ORDER BY {','.join(keys)}"
            if row_digest(source.execute(ordered)) != row_digest(connection.native(ordered)):
                raise StorageError("gateway_sqlite_import_mismatch")
    for version in (1, 2):
        connection.native(
            "INSERT INTO schema_migrations VALUES (?,?) ON CONFLICT(version) DO NOTHING",
            (version, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        )
