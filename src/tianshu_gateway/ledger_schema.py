"""Owned ledger DDL; portable integer facts use PostgreSQL BIGINT on PG."""

CORE_SCHEMA = """
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
        """

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

ADMISSION_SCHEMA = """
    CREATE TABLE IF NOT EXISTS admission_metrics (
        service TEXT NOT NULL, request_id TEXT NOT NULL, workload_class TEXT NOT NULL,
        outcome TEXT NOT NULL, wait_ms INTEGER,
        PRIMARY KEY(service, request_id));
"""
