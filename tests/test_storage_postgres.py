"""Real PostgreSQL authority/import tests and reused public HTTP boundary cases."""

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
import test_gateway_http as chat_cases
import test_responses_native as native_cases
import test_usage_report as usage_cases
from tianshu_storage import StorageOptions

from tianshu_gateway.contracts import Rejected
from tianshu_gateway.diagnostics import AttemptMetrics, Diagnostics
from tianshu_gateway.usage import build_report
from tianshu_gateway.usage_report import open_ledger

PG_URL = os.environ.get("TIANSHU_TEST_POSTGRES_URL", "")


@unittest.skipUnless(PG_URL, "explicit isolated PostgreSQL URL required")
class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ledger.sqlite"
        self.options = StorageOptions(
            postgresql_url=PG_URL, namespace="gateway_test_" + uuid4().hex[:16]
        )

    def ledger(self):
        result = Diagnostics(self.path, self.options)
        self.addCleanup(result.close)
        self.assertEqual(result.database.backend, "postgresql")
        return result

    def test_import_covers_all_ledger_tables_and_keeps_sqlite_source_unchanged(self):
        old = Diagnostics(self.path, StorageOptions())
        old.remember_config(7, {"config": "旧版本"})
        old.revoke(8)
        receipt = {"caller_service": "companion", "request_id": "old", "config_version": 7}
        old.begin(receipt, "turn")
        old.finish(receipt, "completed", 10, 200, AttemptMetrics(input_tokens=10**12))
        identity = ("model-protocol/v1", "one", "caller", "scope")
        native = dict(
            zip(("contract", "principal_id", "caller_service", "credential_namespace"), identity)
        )
        native.update(request_id="native", native_config_version=7)
        old.native_remember_config(identity, 7, {"native": True})
        old.native_revoke(identity, 8)
        old.native_begin(native, "native-turn")
        old.native_finish(native, "response_completed", 20, 200)
        old.record_admission("companion", "old", "interactive", "admitted", 2)
        old.close()
        before = self.path.read_bytes()
        current = self.ledger()
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(current.get("companion", "old"), receipt)
        self.assertEqual(current.native_get(identity, "native"), native)
        self.assertTrue(current.is_revoked(8))
        self.assertTrue(current.native_is_revoked(identity, 8))
        self.assertEqual(current.applied_migrations(), {1, 2})
        self.assertEqual(current.admission("companion", "old"), ("interactive", "admitted", 2))

    def test_owner_rejection_unknown_recovery_and_idempotency_never_replay(self):
        current = self.ledger()
        receipt = {
            "caller_service": "companion",
            "request_id": "pending",
            "config_version": 7,
            "execution": {"state": "running"},
        }
        current.begin(receipt, "turn")
        with self.assertRaisesRegex(sqlite3.OperationalError, "owner_unavailable"):
            Diagnostics(self.path, self.options)
        self.assertEqual(current.get("companion", "pending")["execution"]["state"], "running")
        current.close()
        restarted = self.ledger()
        self.assertEqual(restarted.get("companion", "pending")["outcome"], "unknown")
        self.assertEqual(
            restarted.get("companion", "pending")["execution"]["error_code"], "interrupted"
        )
        with self.assertRaises(Rejected) as conflict:
            restarted.begin(receipt, "turn")
        self.assertEqual(conflict.exception.code, "idempotency_conflict")
        self.assertFalse(self.path.exists())

    def test_identity_scoping_bigint_metrics_and_read_only_current_report(self):
        ledger = self.ledger()
        for service, count in (("one", 10**12), ("two", 42)):
            receipt = {"caller_service": service, "request_id": "same", "config_version": 7}
            ledger.begin(receipt, "turn")
            ledger.finish(
                receipt,
                "completed",
                13,
                200,
                AttemptMetrics(
                    input_tokens=count,
                    output_tokens=1,
                    usage_source="upstream_json_usage",
                    usage_complete=True,
                ),
            )
        marker = ledger.storage.state_path.read_bytes()
        with closing(open_ledger(self.path, self.options)) as reader:
            report = build_report(
                reader, "chat", {"service": "one"}, usage_cases.window(view="attempts")
            )
            self.assertEqual(report["counts"]["total"], 1)
            self.assertEqual(report["usage"]["input_tokens"]["sum"], 10**12)
            json.dumps(report)
            with self.assertRaises(sqlite3.Error):
                reader.execute("DELETE FROM requests")
        self.assertEqual(ledger.storage.state_path.read_bytes(), marker)
        self.assertFalse(self.path.exists())

    def test_lost_owner_connection_does_not_reconnect_and_restart_marks_unknown(self):
        ledger = self.ledger()
        receipt = {"caller_service": "one", "request_id": "lost", "config_version": 7}
        ledger.begin(receipt, "turn")
        pid = ledger.connection.native("SELECT pg_backend_pid()").fetchone()[0]
        with psycopg.connect(PG_URL, autocommit=True) as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
        self.assertFalse(ledger.reachable())
        with self.assertRaises(sqlite3.Error):
            ledger.finish(receipt, "completed", 1, 200)
        ledger.close()
        restarted = self.ledger()
        self.assertEqual(restarted.get("one", "lost")["outcome"], "unknown")

    def test_active_pg_configuration_missing_or_broken_cannot_open_old_sqlite(self):
        ledger = self.ledger()
        ledger.close()
        for url in ("", "postgresql://synthetic@127.0.0.1:1/missing"):
            wrong = StorageOptions(
                namespace=self.options.namespace, postgresql_url=url, connect_timeout=0.1
            )
            with self.assertRaises(sqlite3.Error):
                Diagnostics(self.path, wrong)
        self.assertFalse(self.path.exists())


class RealHttpMixin:
    async def asyncSetUp(self):
        options = {"postgresql_url": PG_URL, "namespace": "gateway_http_" + uuid4().hex[:16]}

        def create(settings):
            settings.storage_backends = options
            return self.real_app_factory(settings)

        with patch.object(self.fixture_module, "create_app", side_effect=create):
            await super().asyncSetUp()
        self.assertEqual(self.gateway.diagnostics.database.backend, "postgresql")


@unittest.skipUnless(PG_URL, "explicit isolated PostgreSQL URL required")
class ChatHttpTests(RealHttpMixin, chat_cases.GatewayHttpTests):
    fixture_module = chat_cases
    real_app_factory = staticmethod(chat_cases.create_app)


@unittest.skipUnless(PG_URL, "explicit isolated PostgreSQL URL required")
class NativeHttpTests(RealHttpMixin, native_cases.NativeHttpTests):
    fixture_module = native_cases
    real_app_factory = staticmethod(native_cases.create_app)


@unittest.skipUnless(PG_URL, "explicit isolated PostgreSQL URL required")
class UsageHttpTests(RealHttpMixin, usage_cases.UsageHttpTests):
    fixture_module = usage_cases
    real_app_factory = staticmethod(usage_cases.create_app)

    async def test_usage_cli_reads_active_postgres_without_changing_binding(self):
        async with await self.chat(headers=self.chat_headers(request_id="cli-report")) as response:
            self.assertEqual(response.status, 200)
        await self.idle()
        marker = self.gateway.diagnostics.storage.state_path
        before = marker.read_bytes()
        settings = Path(self.temp.name) / "cli-settings.json"
        settings.write_text(json.dumps(asdict(self.settings)), encoding="utf-8")
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "tianshu_gateway",
                "usage-report",
                "--settings",
                str(settings),
                "--service",
                "companion",
                "--compact",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["counts"]["total"], 1)
        self.assertEqual(marker.read_bytes(), before)
        self.assertFalse(Path(self.settings.diagnostics_path).exists())

    async def test_the_report_survives_a_restart_on_the_same_ledger(self):
        # The SQLite fixture also opens a second concurrent owner. For PG exercise
        # an actual restart, after releasing the previous runtime's session lock.
        async with await self.chat(
            headers=self.chat_headers(request_id="before-restart")
        ) as response:
            self.assertEqual(response.status, 200)
        await self.idle()
        before = await self.report()
        await self.runner.cleanup()
        app = self.real_app_factory(self.settings)
        self.runner, self.url = await usage_cases.start_http(app)
        self.addAsyncCleanup(self.runner.cleanup)
        after = await self.report()
        for key in ("counts", "usage", "latency_ms"):
            self.assertEqual(after[key], before[key])


def load_tests(loader, _tests, _pattern):
    """Reuse existing wire/authorization cases without duplicating their implementations."""
    suite = loader.loadTestsFromTestCase(LedgerTests)
    selected = {
        ChatHttpTests: (
            "test_native_http_fidelity_and_published_relationship",
            "test_external_preserve_and_internal_header_spoof_rejected",
            "test_real_one_byte_sse_tool_utf8_and_usage_fidelity",
            "test_incomplete_clean_eof_and_transport_drop_never_succeed",
            "test_revocation_and_credential_resolution_fail_closed",
            "test_config_content_is_immutable_and_turn_and_request_ids_are_pinned",
            "test_sqlite_restart_preserves_receipt_revocation_and_turn_version",
            "test_diagnostics_write_failure_after_send_remains_unknown",
        ),
        NativeHttpTests: (
            "test_identity_comes_from_registration_and_routes_never_cross",
            "test_native_versions_are_authorized_not_inherited",
            "test_native_revocation_is_separate_from_chat_revocation",
            "test_receipt_reads_are_scoped_and_unknown_ids_do_not_leak",
            "test_native_timeout_and_cancel_report_unknown",
            "test_native_diagnostics_write_failure_stays_unknown",
        ),
        UsageHttpTests: (
            "test_usage_cli_reads_active_postgres_without_changing_binding",
            "test_json_and_stream_attempts_are_sourced_and_counted",
            "test_reads_are_scoped_to_the_authenticated_identity",
            "test_native_usage_report_is_its_own_key_space",
            "test_the_report_survives_a_restart_on_the_same_ledger",
            "test_the_report_never_exposes_bodies_tools_credentials_or_urls",
        ),
    }
    for case, names in selected.items():
        suite.addTests(case(name) for name in names)
    return suite
