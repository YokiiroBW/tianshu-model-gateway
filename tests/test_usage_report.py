"""TS-043 usage and latency diagnostics: private metrics, bounded report, read ports.

Everything runs against isolated loopback fixtures (platform substitute plus recording
upstream). No real platform, model provider, account or paid call is involved. The
recorded upstream is the only source of usage, so a missing value is proven missing.
"""

import asyncio
import copy
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

import aiohttp
from aiohttp import web

from gateway_fixtures import (
    CONTRACT,
    DOCUMENTS,
    NATIVE_CONTRACT,
    NATIVE_DOCUMENTS,
    SECRETS,
    RecordingServices,
    event,
    native_event,
    registration,
    start_http,
)
from tianshu_gateway.config import ClientGrant, utcnow
from tianshu_gateway.contracts import Rejected
from tianshu_gateway.diagnostics import (
    NATIVE_IDENTITY_FIELDS,
    UNOBSERVED,
    AttemptMetrics,
    Diagnostics,
    attempt_metrics,
)
from tianshu_gateway.native import NativeGrant
from tianshu_gateway.responses import ResponsesObserver
from tianshu_gateway.routing import StreamObserver
from tianshu_gateway.server import GATEWAY, Settings, create_app
from tianshu_gateway.usage import build_report, parse_selection

NATIVE_PATH = "/v1/responses"
RECEIPT_PATH = "/internal/v1/native-model-requests/"
USAGE_PATH = "/internal/v1/model-usage"
NATIVE_USAGE_PATH = "/internal/v1/native-model-usage"
MODEL = NATIVE_DOCUMENTS["native_request"]["model"]
NAMESPACE = "credential-namespace-fixture"
# Extra fixture credentials for the revoked and retired cases only. They are not part of
# the shared fixture secret set, so they cannot collide with another task's values. The
# retired reference names an environment variable that is never set in this deployment.
RETIRED_ENV = "TS043_TEST_UNSET_CLIENT"
RETIRED_TOKEN = "ts043-retired-client-fixture"
REVOKED_ENV = "TS043_TEST_REVOKED_NATIVE"
REVOKED_TOKEN = "ts043-revoked-native-fixture"
NATIVE_SELECTORS = (
    "--principal-id",
    "principal-fixture",
    "--caller-service",
    "caller-fixture",
    "--credential-namespace",
    NAMESPACE,
)


def window(**overrides):
    """A report window that certainly contains rows written by this test run."""
    now = utcnow()
    values = {
        "since": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "until": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
    }
    values.update(overrides)
    return values


def query(values):
    return "?" + urlencode(values)


def native_grant(**overrides):
    fields = {
        "service": "caller-fixture",
        "credential_ref": "secret-ref:fixture/native",
        "principal_id": "principal-fixture",
        "credential_namespace": NAMESPACE,
        "provider_ids": ("provider-fixture",),
        "native_config_versions": (7,),
        "native_config_version": 7,
        "internal": True,
        "expires_at": (utcnow() + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
    }
    fields.update(overrides)
    return NativeGrant(**fields)


def native_body(**overrides):
    body = copy.deepcopy(NATIVE_DOCUMENTS["native_request"])
    body.update(overrides)
    return body


def receipt_document(service, request_id, usage, complete, outcome="unknown"):
    return {
        "schema_version": 1,
        "request_id": request_id,
        "caller_service": service,
        "outcome": outcome,
        "usage": usage,
        "usage_complete": complete,
        "native_usage": None,
    }


def write_attempt(ledger, service, request_id, reason, status, elapsed, metrics, when_ms):
    """Insert one metric row and its receipt row exactly as the transports do."""
    usage = None
    if metrics.input_tokens is not None or metrics.output_tokens is not None:
        usage = {"input_tokens": metrics.input_tokens, "output_tokens": metrics.output_tokens}
    with ledger.connection:
        ledger.connection.execute(
            "INSERT OR REPLACE INTO requests VALUES (?,?,?,?,?,?)",
            (
                service,
                request_id,
                json.dumps(receipt_document(service, request_id, usage, metrics.usage_complete)),
                reason,
                elapsed,
                status,
            ),
        )
        ledger.connection.execute(
            "INSERT OR REPLACE INTO request_metrics VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                service,
                request_id,
                when_ms,
                metrics.first_upstream_byte_ms,
                metrics.first_event_ms,
                metrics.first_output_ms,
                metrics.usage_source,
                metrics.input_tokens,
                metrics.output_tokens,
                int(metrics.usage_complete),
            ),
        )


class MetricFaultConnection:
    """A connection that fails exactly the private metric statement."""

    def __init__(self, connection):
        self.wrapped = connection

    def execute(self, sql, parameters=()):
        if "request_metrics" in sql:
            raise sqlite3.OperationalError("fixture metric failure")
        return self.wrapped.execute(sql, parameters)

    def __enter__(self):
        return self.wrapped.__enter__()

    def __exit__(self, *exc_info):
        return self.wrapped.__exit__(*exc_info)

    def __getattr__(self, name):
        return getattr(self.wrapped, name)


class UsageBoundaryTests(unittest.TestCase):
    """Component level: migration, metric projection, observer timings, parameters."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "ledger.sqlite")

    def old_deployment(self):
        """A database written by the build before the private metric tables existed."""
        connection = sqlite3.connect(self.path)
        connection.executescript("""
            CREATE TABLE requests (
                service TEXT NOT NULL, request_id TEXT NOT NULL, receipt TEXT NOT NULL,
                reason TEXT NOT NULL, elapsed_ms INTEGER, upstream_status INTEGER,
                PRIMARY KEY(service, request_id));
            CREATE TABLE turns (
                service TEXT NOT NULL, turn_id TEXT NOT NULL, version INTEGER NOT NULL,
                PRIMARY KEY(service, turn_id));
            CREATE TABLE revoked (version INTEGER PRIMARY KEY);
            CREATE TABLE configs (version INTEGER PRIMARY KEY, digest TEXT NOT NULL);
        """)
        with connection:
            connection.execute(
                "INSERT INTO requests VALUES (?,?,?,?,?,?)",
                (
                    "companion",
                    "legacy-0",
                    json.dumps(receipt_document("companion", "legacy-0", None, False)),
                    "in_flight",
                    None,
                    None,
                ),
            )
        connection.close()

    def test_metric_migration_copies_an_existing_deployment_and_keeps_old_tables(self):
        self.old_deployment()
        ledger = Diagnostics(self.path)
        self.addCleanup(ledger.close)
        self.assertEqual(ledger.migration_backup, self.path + ".ts043-backup")
        self.assertTrue(os.path.exists(ledger.migration_backup))
        names = {
            row[0]
            for row in ledger.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertLessEqual({"request_metrics", "native_request_metrics"}, names)
        self.assertEqual(
            ledger.connection.execute("SELECT version FROM schema_migrations").fetchall(), [(1,)]
        )
        # The pre-existing rows and tables are untouched: the older build still reads them.
        self.assertEqual(ledger.get("companion", "legacy-0")["outcome"], "unknown")
        older = sqlite3.connect(self.path)
        try:
            self.assertEqual(older.execute("SELECT COUNT(*) FROM requests").fetchone()[0], 1)
            self.assertEqual(older.execute("SELECT version FROM revoked").fetchall(), [])
        finally:
            older.close()
        # The copy is the pre-upgrade image, so it has no metric table at all.
        original = sqlite3.connect(ledger.migration_backup)
        try:
            copied = {
                row[0]
                for row in original.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        finally:
            original.close()
        self.assertNotIn("request_metrics", copied)

    def test_a_fresh_ledger_is_not_backed_up_and_a_second_open_is_idempotent(self):
        ledger = Diagnostics(self.path)
        self.assertIsNone(ledger.migration_backup)
        self.assertFalse(os.path.exists(self.path + ".ts043-backup"))
        ledger.close()
        again = Diagnostics(self.path)
        self.addCleanup(again.close)
        self.assertIsNone(again.migration_backup)
        self.assertFalse(os.path.exists(self.path + ".ts043-backup"))

    def test_a_repeated_terminal_replaces_and_is_never_counted_twice(self):
        ledger = Diagnostics(self.path)
        self.addCleanup(ledger.close)
        document = receipt_document("companion", "repeat", {"input_tokens": 3}, False)
        metrics = AttemptMetrics(
            first_event_ms=4, usage_source="upstream_stream_usage", input_tokens=3
        )
        with ledger.connection:
            ledger.connection.execute(
                "INSERT INTO requests VALUES (?,?,?,?,?,?)",
                ("companion", "repeat", json.dumps(document), "in_flight", None, None),
            )
        ledger.finish(document, "completed", 20, 200, metrics)
        ledger.finish(document, "completed", 20, 200, metrics)
        self.assertEqual(
            ledger.connection.execute(
                "SELECT COUNT(*) FROM request_metrics WHERE service='companion'"
            ).fetchone()[0],
            1,
        )
        report = build_report(
            ledger.connection, "chat", {"service": "companion"}, window(view="summary")
        )
        self.assertEqual(report["counts"]["total"], 1)
        self.assertEqual(
            report["usage"]["normalized"]["input_tokens"],
            {"sum": 3, "reported": 1, "missing": 0},
        )

    def test_a_failing_metric_write_rolls_back_the_receipt_too(self):
        ledger = Diagnostics(self.path)
        self.addCleanup(ledger.close)
        document = receipt_document("companion", "atomic", {"input_tokens": 1}, True)
        with ledger.connection:
            ledger.connection.execute(
                "INSERT INTO requests VALUES (?,?,?,?,?,?)",
                ("companion", "atomic", json.dumps(document), "in_flight", None, None),
            )
        real = ledger.connection
        ledger.connection = MetricFaultConnection(real)
        with self.assertRaises(sqlite3.OperationalError):
            ledger.finish(document, "completed", 5, 200, UNOBSERVED)
        ledger.connection = real
        reason, stored = ledger.connection.execute(
            "SELECT reason, receipt FROM requests WHERE service='companion' AND request_id='atomic'"
        ).fetchone()
        self.assertEqual(reason, "in_flight")
        self.assertEqual(json.loads(stored)["outcome"], "unknown")
        self.assertEqual(
            ledger.connection.execute("SELECT COUNT(*) FROM request_metrics").fetchone()[0], 0
        )

    def test_an_attempt_without_a_metric_row_is_reported_as_unmetered(self):
        ledger = Diagnostics(self.path)
        self.addCleanup(ledger.close)
        stamp = int(utcnow().timestamp() * 1000)
        write_attempt(
            ledger,
            "companion",
            "metered",
            "completed",
            200,
            10,
            AttemptMetrics(usage_source="upstream_json_usage", input_tokens=1, output_tokens=2),
            stamp,
        )
        with ledger.connection:
            ledger.connection.execute(
                "INSERT INTO requests VALUES (?,?,?,?,?,?)",
                (
                    "companion",
                    "crashed",
                    json.dumps(receipt_document("companion", "crashed", None, False)),
                    "in_flight",
                    None,
                    None,
                ),
            )
        report = build_report(
            ledger.connection, "chat", {"service": "companion"}, window(view="attempts")
        )
        self.assertEqual(report["counts"]["total"], 1)
        self.assertEqual(report["coverage"]["unmetered_total"], 1)
        self.assertEqual([row["request_id"] for row in report["attempts"]], ["metered"])

    def test_native_finish_writes_one_row_the_native_report_can_read(self):
        """The native metric row is written by the native terminal, in its own key space."""
        ledger = Diagnostics(self.path)
        self.addCleanup(ledger.close)
        document = {
            "contract": "model-protocol/v1",
            "principal_id": "principal-fixture",
            "caller_service": "caller-fixture",
            "credential_namespace": NAMESPACE,
            "request_id": "native-1",
            "native_config_version": 7,
        }
        ledger.native_begin(document, "native-turn-1")
        receipt = {
            **document,
            "outcome": "succeeded",
            "usage": {"input_tokens": 2, "output_tokens": 3},
            "native_usage": {"input_tokens": 2, "output_tokens": 3, "vendor": {"x": None}},
            "usage_complete": True,
            "observed_at": "2026-01-01T00:00:00Z",
        }
        metrics = attempt_metrics(receipt, None, 12, False, True)
        ledger.native_finish(receipt, "response_completed", 12, 200, metrics)
        identity = {name: document[name] for name in NATIVE_IDENTITY_FIELDS}
        report = build_report(ledger.connection, "native", identity, window(view="attempts"))
        self.assertEqual(report["identity"], identity)
        self.assertEqual(report["counts"]["succeeded"], 1)
        self.assertEqual(report["usage"]["normalized"]["output_tokens"]["sum"], 3)
        self.assertEqual(report["latency_ms"]["request_total_ms"]["max"], 12)
        row = report["attempts"][0]
        self.assertEqual(row["request_id"], "native-1")
        self.assertEqual(row["usage_source"], "upstream_json_usage")
        self.assertEqual(row["outcome"], "succeeded")
        # The vendor-native structure stays in the receipt and is never aggregated.
        self.assertEqual(
            ledger.connection.execute("SELECT COUNT(*) FROM native_request_metrics").fetchone()[0],
            1,
        )
        self.assertFalse(report["usage"]["vendor_fields_aggregated"])
        self.assertEqual(set(report["usage"]["normalized"]), {"input_tokens", "output_tokens"})

    def test_missing_observations_are_never_recorded_as_zero(self):
        self.assertIsNone(UNOBSERVED.first_event_ms)
        self.assertIsNone(UNOBSERVED.input_tokens)
        self.assertFalse(UNOBSERVED.usage_complete)
        absent = {"usage": None, "usage_complete": False, "native_usage": None}
        inspected = attempt_metrics(absent, None, 30, True, True)
        self.assertEqual(inspected.usage_source, "not_reported")
        self.assertIsNone(inspected.first_event_ms)
        self.assertIsNone(inspected.input_tokens)
        # A response that never arrived is a different fact from an empty usage report.
        self.assertEqual(attempt_metrics(absent, None, 30, True, False).usage_source, "unobserved")
        # A partial upstream report keeps the missing half absent, not zero.
        partial = attempt_metrics(
            {
                "usage": {"input_tokens": 0},
                "usage_complete": False,
                "native_usage": {"prompt_tokens": 0},
            },
            None,
            30,
            False,
            True,
        )
        self.assertEqual(partial.usage_source, "upstream_json_usage")
        self.assertEqual(partial.input_tokens, 0)
        self.assertIsNone(partial.output_tokens)
        for build in (
            lambda: AttemptMetrics(first_event_ms=-1),
            lambda: AttemptMetrics(first_output_ms=True),
            lambda: AttemptMetrics(usage_source="vendor_guess"),
            lambda: AttemptMetrics(usage_source=None),
            lambda: AttemptMetrics(input_tokens=-2),
            lambda: AttemptMetrics(usage_complete=1),
        ):
            with self.subTest(build=build), self.assertRaises(ValueError):
                build()

    def test_latency_facts_never_exceed_the_attempt_total(self):
        class Late:
            first_upstream_byte_ms = 900
            first_event_ms = 950
            first_output_ms = 999

        metrics = attempt_metrics(
            {"usage": None, "usage_complete": False, "native_usage": None}, Late(), 10, True, True
        )
        self.assertEqual(metrics.first_upstream_byte_ms, 10)
        self.assertEqual(metrics.first_event_ms, 10)
        self.assertEqual(metrics.first_output_ms, 10)

    def test_first_event_is_wire_truth_while_first_output_needs_real_output(self):
        observer = StreamObserver(expected_choices=1, started=time.monotonic())
        observer.feed(b": fixture keepalive\r\n\r\n")
        self.assertIsNone(observer.first_event_ms)
        self.assertIsNone(observer.first_output_ms)
        observer.feed(event({"choices": [{"index": 0, "delta": {}, "finish_reason": None}]}))
        self.assertIsNotNone(observer.first_event_ms)
        self.assertIsNone(observer.first_output_ms)
        observer.feed(
            event({"choices": [{"index": 0, "delta": {"content": "字"}, "finish_reason": "stop"}]})
        )
        self.assertIsNotNone(observer.first_output_ms)
        self.assertLessEqual(observer.first_event_ms, observer.first_output_ms)

    def test_a_repeated_usage_fragment_replaces_instead_of_accumulating(self):
        observer = StreamObserver(expected_choices=1, started=time.monotonic())
        usage = {"prompt_tokens": 3, "completion_tokens": 4}
        observer.feed(event({"choices": [], "usage": dict(usage)}))
        observer.feed(event({"choices": [], "usage": dict(usage)}))
        self.assertEqual(observer.native_usage, usage)
        observer.feed(event({"choices": [], "usage": {"prompt_tokens": 9, "completion_tokens": 1}}))
        self.assertEqual(observer.native_usage, {"prompt_tokens": 9, "completion_tokens": 1})
        observer.feed(b"data: [DONE]\r\n\r\n")
        # A second terminal event invalidates the observation instead of double counting it.
        observer.feed(b"data: [DONE]\r\n\r\n")
        self.assertTrue(observer.invalid)
        self.assertFalse(observer.complete)

    def test_unknown_native_events_never_invent_a_first_output(self):
        observer = ResponsesObserver(started=time.monotonic())
        observer.feed(native_event("response.vendor_future", unknown={"unchanged": True}))
        self.assertIsNotNone(observer.first_event_ms)
        self.assertIsNone(observer.first_output_ms)
        observer.feed(native_event("response.output_text.delta", delta="你"))
        self.assertIsNotNone(observer.first_output_ms)

    def test_report_parameters_are_bounded_and_reject_unknown_values(self):
        for value in ({}, {"view": "summary"}, {"view": "attempts", "limit": "5000"}):
            parse_selection(value)
        for bad in (
            {"view": "rows"},
            {"limit": "0"},
            {"limit": "5001"},
            {"limit": "-1"},
            {"limit": "01"},
            {"limit": "1.0"},
            {"offset": "100001"},
            {"since": "2026-01-01"},
            {"since": "2026-01-01T00:00:00"},
            {"until": "not-a-time"},
            {"vendor": "1"},
            {"since": "2026-01-01T00:00:00Z", "until": "2026-01-01T00:00:00Z"},
            {"since": "2024-01-01T00:00:00Z", "until": "2026-01-01T00:00:00Z"},
        ):
            with self.subTest(bad=bad), self.assertRaises(Rejected):
                parse_selection(bad)

    def test_report_is_scoped_to_the_requested_identity_newest_first(self):
        ledger = Diagnostics(self.path)
        self.addCleanup(ledger.close)
        stamp = int(utcnow().timestamp() * 1000)
        for index, service in enumerate(("companion", "other", "companion")):
            write_attempt(
                ledger,
                service,
                f"{service}-{index}",
                "completed",
                200,
                10 * index,
                AttemptMetrics(usage_source="upstream_json_usage", input_tokens=index),
                stamp + index * 10,
            )
        report = build_report(
            ledger.connection, "chat", {"service": "companion"}, window(view="attempts")
        )
        self.assertEqual(report["identity"], {"service": "companion"})
        self.assertEqual(
            [row["request_id"] for row in report["attempts"]], ["companion-2", "companion-0"]
        )
        self.assertEqual(report["coverage"]["matching"], 2)
        self.assertEqual(
            build_report(ledger.connection, "chat", {"service": "other"}, window(view="summary"))[
                "counts"
            ]["total"],
            1,
        )
        self.assertEqual(
            build_report(ledger.connection, "chat", {"service": "absent"}, window(view="summary"))[
                "counts"
            ]["total"],
            0,
        )


class UsageHttpTests(unittest.IsolatedAsyncioTestCase):
    """The real gateway, recorded upstream and isolated loopback platform substitute."""

    async def asyncSetUp(self):
        self.environment = patch.dict(os.environ, {**SECRETS, REVOKED_ENV: REVOKED_TOKEN})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ledger_path = str(Path(self.temp.name) / "usage.sqlite")
        self.services = RecordingServices()
        upstream_app = web.Application()
        upstream_app.router.add_post("/v1/chat/completions", self.services.upstream)
        upstream_app.router.add_post(NATIVE_PATH, self.services.native_upstream)
        runner, self.upstream_url = await start_http(upstream_app)
        self.addAsyncCleanup(runner.cleanup)
        self.services.configure(self.upstream_url)
        self.native_credential_ref = "secret-ref:fixture/native-provider"
        self.services.native_config["providers"][0]["credential_ref"] = self.native_credential_ref
        platform_app = web.Application()
        platform_app.router.add_post("/internal/v1/model-config/snapshot", self.services.snapshot)
        platform_app.router.add_post(
            "/internal/v1/model-config/native/snapshot", self.services.native_snapshot
        )
        runner, self.platform_url = await start_http(platform_app)
        self.addAsyncCleanup(runner.cleanup)
        references = {
            "secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM",
            "secret-ref:fixture/native-provider": "TS042_TEST_NATIVE_UPSTREAM",
            "secret-ref:fixture/native": "TS042_TEST_NATIVE",
            "secret-ref:fixture/native-external": "TS042_TEST_NATIVE_EXTERNAL",
            "secret-ref:fixture/native-other": "TS042_TEST_NATIVE_OTHER",
            "secret-ref:fixture/client": "TS041_TEST_CLIENT",
            "secret-ref:fixture/external": "TS041_TEST_EXTERNAL",
            "secret-ref:fixture/other": "TS041_TEST_OTHER",
            "secret-ref:fixture/platform": "TS041_TEST_PLATFORM",
            "secret-ref:fixture/retired": RETIRED_ENV,
            "secret-ref:fixture/native-revoked": REVOKED_ENV,
        }
        self.grants = [
            native_grant(),
            native_grant(
                service="caller-external",
                principal_id="principal-external",
                credential_ref="secret-ref:fixture/native-external",
                native_config_version=None,
                internal=False,
            ),
            native_grant(
                service="caller-expired",
                principal_id="principal-expired",
                credential_ref="secret-ref:fixture/native-other",
                expires_at="2020-01-01T00:00:00Z",
            ),
            native_grant(
                service="caller-revoked",
                principal_id="principal-revoked",
                credential_ref="secret-ref:fixture/native-revoked",
                revoked=True,
            ),
        ]
        self.settings = Settings(
            str(CONTRACT),
            self.ledger_path,
            self.platform_url,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            references,
            [registration(self.platform_url), registration(self.upstream_url + "/v1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                ),
                ClientGrant("other", "secret-ref:fixture/other", "provider-fixture", 7, True, (8,)),
                ClientGrant(
                    "retired", "secret-ref:fixture/retired", "provider-fixture", 7, True, (8,)
                ),
            ],
            native_enabled=True,
            native_contract_directory=str(NATIVE_CONTRACT),
            native_clients=self.grants,
        )
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.gateway = self.app[GATEWAY]
        self.client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10), trust_env=False
        )
        self.addAsyncCleanup(self.client.close)
        self.sequence = 0
        self.body = copy.deepcopy(DOCUMENTS["native_request"])
        self.body["vendor_extension"] = {"null": None, "nested": [1, {"x": "正文"}]}

    def chat_headers(self, *, request_id=None, turn=None, token="TS041_TEST_CLIENT", version=7):
        self.sequence += 1
        return {
            "Authorization": "Bearer " + SECRETS[token],
            "X-Request-ID": request_id or f"usage-{self.sequence}",
            "X-Tianshu-Turn-ID": turn or f"usage-turn-{self.sequence}",
            "X-Tianshu-Config-Version": str(version),
            "X-Tianshu-Workload": "companion.text",
        }

    def native_headers(self, *, request_id=None, turn=None, version=7):
        self.sequence += 1
        headers = {
            "Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"],
            "X-Request-ID": request_id or f"usage-native-{self.sequence}",
            "X-Tianshu-Turn-ID": turn or f"usage-native-turn-{self.sequence}",
        }
        if version is not None:
            headers["X-Tianshu-Native-Config-Version"] = str(version)
        return headers

    async def chat(self, headers=None, **overrides):
        payload = copy.deepcopy(self.body)
        payload.update(overrides)
        return await self.client.post(
            self.url + "/v1/chat/completions",
            json=payload,
            headers=self.chat_headers() if headers is None else headers,
        )

    async def native(self, **overrides):
        return await self.client.post(
            self.url + NATIVE_PATH, json=native_body(**overrides), headers=self.native_headers()
        )

    async def report(self, path=USAGE_PATH, token="TS041_TEST_CLIENT", **parameters):
        # The window is half-open, so a report always pins both ends explicitly; that is
        # what lets a row written in the same millisecond as the query stay inside it.
        values = {**window(), **parameters}
        async with self.client.get(
            self.url + path + query(values),
            headers={"Authorization": "Bearer " + os.environ[token]},
        ) as response:
            body = await response.text()
            self.assertEqual(response.status, 200, body)
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            return json.loads(body)

    async def usage_status(
        self, path=USAGE_PATH, token="TS041_TEST_CLIENT", raw_query="", raw_token=None, **values
    ):
        bearer = raw_token if raw_token is not None else os.environ[token]
        async with self.client.get(
            self.url + path + (raw_query or query({**window(), **values})),
            headers={"Authorization": "Bearer " + bearer},
        ) as response:
            return response.status, await response.json()

    async def idle(self):
        async with asyncio.timeout(2):
            while self.gateway.active:
                await asyncio.sleep(0.01)

    async def stored_receipt(self, request_id):
        async with self.client.get(
            self.url + "/internal/v1/model-requests/" + request_id,
            headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]},
        ) as response:
            self.assertEqual(response.status, 200)
            return await response.json()

    def metric_row(self, request_id):
        row = self.gateway.diagnostics.connection.execute(
            "SELECT usage_source, first_upstream_byte_ms, first_event_ms, first_output_ms, "
            "input_tokens, output_tokens, usage_complete FROM request_metrics "
            "WHERE service='companion' AND request_id=?",
            (request_id,),
        ).fetchone()
        self.assertIsNotNone(row)
        return {
            "usage_source": row[0],
            "first_upstream_byte_ms": row[1],
            "first_event_ms": row[2],
            "first_output_ms": row[3],
            "input_tokens": row[4],
            "output_tokens": row[5],
            "usage_complete": row[6],
        }

    def elapsed_row(self, request_id):
        return self.gateway.diagnostics.connection.execute(
            "SELECT elapsed_ms FROM requests WHERE service='companion' AND request_id=?",
            (request_id,),
        ).fetchone()[0]

    async def test_json_and_stream_attempts_are_sourced_and_counted(self):
        self.services.response["usage"] = {
            "prompt_tokens": 4,
            "completion_tokens": 6,
            "vendor_unknown": {"partial": True},
        }
        async with await self.chat(
            headers=self.chat_headers(request_id="json-attempt")
        ) as response:
            self.assertEqual(response.status, 200)
        self.services.mode = "stream"
        async with await self.chat(
            headers=self.chat_headers(request_id="stream-attempt"), stream=True
        ) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        await self.idle()
        report = await self.report()
        self.assertEqual(
            [report["counts"][name] for name in ("total", "succeeded", "failed", "cancelled")],
            [2, 2, 0, 0],
        )
        self.assertEqual(report["counts"]["unknown"], 0)
        rows = {row["request_id"]: row for row in (await self.report(view="attempts"))["attempts"]}
        self.assertEqual(rows["json-attempt"]["usage_source"], "upstream_json_usage")
        self.assertEqual(rows["stream-attempt"]["usage_source"], "upstream_stream_usage")
        # A non-streaming call has no event or first-output latency at all, so its total
        # duration is never offered as model generation time.
        self.assertIsNone(rows["json-attempt"]["first_event_ms"])
        self.assertIsNone(rows["json-attempt"]["first_output_ms"])
        self.assertIsNotNone(rows["json-attempt"]["request_total_ms"])
        self.assertIsNotNone(rows["stream-attempt"]["first_event_ms"])
        self.assertIsNotNone(rows["stream-attempt"]["first_output_ms"])
        self.assertTrue(all(row["usage_complete"] for row in rows.values()))
        self.assertEqual(report["usage"]["by_source"]["upstream_json_usage"]["attempts"], 1)
        self.assertEqual(report["usage"]["by_source"]["upstream_stream_usage"]["attempts"], 1)
        self.assertEqual(report["usage"]["complete"], 2)
        self.assertEqual(report["usage"]["missing"], 0)
        self.assertEqual(report["usage"]["normalized"]["input_tokens"]["sum"], 4)
        self.assertEqual(report["usage"]["normalized"]["output_tokens"]["sum"], 15)
        self.assertFalse(report["usage"]["vendor_fields_aggregated"])
        self.assertLessEqual(
            report["latency_ms"]["first_event_ms"]["max"],
            report["latency_ms"]["request_total_ms"]["max"],
        )

    async def test_private_metrics_agree_with_the_stored_receipt(self):
        async with await self.chat(headers=self.chat_headers(request_id="agree-json")) as response:
            self.assertEqual(response.status, 200)
        self.services.mode = "stream"
        async with await self.chat(
            headers=self.chat_headers(request_id="agree-stream"), stream=True
        ) as response:
            await response.read()
        await self.idle()
        attempts = {
            row["request_id"]: row for row in (await self.report(view="attempts"))["attempts"]
        }
        for request_id, expected_source in (
            ("agree-json", "upstream_json_usage"),
            ("agree-stream", "upstream_stream_usage"),
        ):
            receipt = await self.stored_receipt(request_id)
            row = self.metric_row(request_id)
            self.assertEqual(row["usage_source"], expected_source)
            # The receipt stays authoritative; the private row only indexes it.
            self.assertEqual(row["input_tokens"], (receipt["usage"] or {}).get("input_tokens"))
            self.assertEqual(row["output_tokens"], (receipt["usage"] or {}).get("output_tokens"))
            self.assertEqual(bool(row["usage_complete"]), receipt["usage_complete"])
            self.assertEqual(attempts[request_id]["outcome"], receipt["outcome"])
            self.assertEqual(attempts[request_id]["request_total_ms"], self.elapsed_row(request_id))

    async def test_missing_usage_is_counted_as_missing_and_never_as_zero(self):
        self.services.response.pop("usage", None)
        async with await self.chat(headers=self.chat_headers(request_id="no-usage")) as response:
            self.assertEqual(response.status, 200)
        await self.idle()
        report = await self.report()
        self.assertEqual(report["counts"]["succeeded"], 1)
        self.assertEqual(report["usage"]["missing"], 1)
        self.assertEqual(report["usage"]["complete"], 0)
        self.assertEqual(report["usage"]["partial"], 0)
        for key in ("input_tokens", "output_tokens"):
            self.assertEqual(
                report["usage"]["normalized"][key], {"sum": 0, "reported": 0, "missing": 1}
            )
        self.assertEqual(report["usage"]["by_source"]["not_reported"]["attempts"], 1)
        row = self.metric_row("no-usage")
        self.assertEqual(row["usage_source"], "not_reported")
        self.assertIsNone(row["input_tokens"])
        self.assertIsNone(row["output_tokens"])
        self.assertFalse(bool(row["usage_complete"]))

    async def test_first_event_latency_is_not_the_request_duration(self):
        self.services.mode = "sse_hold"
        task = asyncio.ensure_future(
            self.chat(headers=self.chat_headers(request_id="held-stream"), stream=True)
        )
        async with asyncio.timeout(3):
            await self.services.started.wait()
        await asyncio.sleep(0.15)
        self.services.release.set()
        async with await task as response:
            self.assertEqual(response.status, 200)
            await response.read()
        await self.idle()
        row = {
            item["request_id"]: item for item in (await self.report(view="attempts"))["attempts"]
        }["held-stream"]
        self.assertIsNotNone(row["first_upstream_byte_ms"])
        self.assertIsNotNone(row["first_event_ms"])
        self.assertIsNotNone(row["first_output_ms"])
        # The first chunk arrived well before the release, while the attempt stayed open for
        # another 150ms: the total duration is not the model's time to first output.
        self.assertLess(row["first_event_ms"], 100)
        self.assertGreaterEqual(row["request_total_ms"] - row["first_event_ms"], 50)
        self.assertLessEqual(row["first_upstream_byte_ms"], row["first_event_ms"])
        self.assertLessEqual(row["first_event_ms"], row["first_output_ms"])
        self.assertLess(row["first_output_ms"], row["request_total_ms"])

    async def test_cancellation_timeout_and_failure_are_separate_buckets(self):
        self.services.mode = "hold"
        task = asyncio.ensure_future(
            self.chat(headers=self.chat_headers(request_id="cancelled-attempt"), stream=True)
        )
        async with asyncio.timeout(3):
            await self.services.started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        # A provider that never answers at all is a timeout, which stays unknown.
        self.services.config["bindings"][0]["timeout_ms"] = 20
        async with await self.chat(headers=self.chat_headers(request_id="timeout-attempt")) as resp:
            self.assertEqual(resp.status, 502)
        self.services.config["bindings"][0]["timeout_ms"] = 1500
        self.services.mode = "error"
        self.services.http_status = 429
        async with await self.chat(headers=self.chat_headers(request_id="failed-attempt")) as resp:
            self.assertEqual(resp.status, 429)
        await self.idle()
        report = await self.report()
        self.assertEqual(
            [report["counts"][name] for name in ("cancelled", "failed", "unknown", "succeeded")],
            [1, 1, 1, 0],
        )
        self.assertEqual(report["counts"]["total"], 3)
        rows = {row["request_id"]: row for row in (await self.report(view="attempts"))["attempts"]}
        self.assertEqual(rows["cancelled-attempt"]["usage_source"], "unobserved")
        self.assertEqual(rows["timeout-attempt"]["usage_source"], "unobserved")
        self.assertEqual(rows["failed-attempt"]["usage_source"], "unobserved")
        self.assertFalse(any(row["usage_complete"] for row in rows.values()))
        self.assertEqual(report["usage"]["missing"], 3)
        self.assertIn(
            {"reason": "cancelled_unknown", "upstream_status": None, "attempts": 1},
            report["reasons"],
        )

    async def test_a_duplicate_concurrent_request_is_counted_once(self):
        headers = self.chat_headers(request_id="duplicate-attempt", turn="duplicate-turn")
        self.services.mode = "hold"
        first = asyncio.ensure_future(self.chat(headers=headers))
        async with asyncio.timeout(3):
            await self.services.started.wait()
        async with await self.chat(headers=headers) as response:
            self.assertEqual(response.status, 409)
            self.assertEqual((await response.json())["code"], "idempotency_conflict")
        self.services.release.set()
        async with await first as response:
            self.assertEqual(response.status, 200)
        await self.idle()
        report = await self.report()
        self.assertEqual(report["counts"]["total"], 1)
        self.assertEqual(len(self.services.calls), 1)
        self.assertEqual(
            self.gateway.diagnostics.connection.execute(
                "SELECT COUNT(*) FROM request_metrics WHERE service='companion'"
            ).fetchone()[0],
            1,
        )

    async def test_reads_are_scoped_to_the_authenticated_identity(self):
        async with await self.chat(headers=self.chat_headers(request_id="mine")) as response:
            self.assertEqual(response.status, 200)
        async with await self.chat(
            headers=self.chat_headers(request_id="theirs", token="TS041_TEST_OTHER")
        ) as response:
            self.assertEqual(response.status, 200)
        await self.idle()
        mine = await self.report()
        theirs = await self.report(token="TS041_TEST_OTHER")
        self.assertEqual(mine["identity"], {"service": "companion"})
        self.assertEqual(theirs["identity"], {"service": "other"})
        self.assertEqual(mine["counts"]["total"], 1)
        self.assertEqual(theirs["counts"]["total"], 1)
        self.assertEqual(
            [row["request_id"] for row in (await self.report(view="attempts"))["attempts"]],
            ["mine"],
        )
        # Key spaces never cross: a Chat credential is not a native identity and the
        # native read port is not reachable with it.
        self.assertEqual(
            (await self.usage_status(path=NATIVE_USAGE_PATH, token="TS041_TEST_CLIENT"))[0], 401
        )
        self.assertEqual((await self.usage_status(token="TS042_TEST_NATIVE"))[0], 401)
        self.assertEqual(
            (await self.usage_status(path=NATIVE_USAGE_PATH, token="TS042_TEST_NATIVE_EXTERNAL"))[
                0
            ],
            200,
        )

    async def test_native_usage_report_is_its_own_key_space(self):
        async with await self.native(stream=False) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        self.services.native_mode = "stream"
        async with await self.native(stream=True) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        await self.idle()
        report = await self.report(path=NATIVE_USAGE_PATH, token="TS042_TEST_NATIVE")
        self.assertEqual(report["key_space"], "native")
        self.assertEqual(report["identity"]["contract"], "model-protocol/v1")
        self.assertEqual(report["identity"]["principal_id"], "principal-fixture")
        self.assertEqual(report["identity"]["caller_service"], "caller-fixture")
        self.assertEqual(report["identity"]["credential_namespace"], NAMESPACE)
        self.assertEqual(report["counts"]["succeeded"], 2)
        attempts = (
            await self.report(path=NATIVE_USAGE_PATH, token="TS042_TEST_NATIVE", view="attempts")
        )["attempts"]
        self.assertEqual(
            {row["usage_source"] for row in attempts},
            {"upstream_json_usage", "upstream_stream_usage"},
        )
        # The sibling native caller sees nothing of this subject's history.
        self.assertEqual(
            (await self.report(path=NATIVE_USAGE_PATH, token="TS042_TEST_NATIVE_EXTERNAL"))[
                "counts"
            ]["total"],
            0,
        )

    async def test_a_revoked_registration_and_a_retired_credential_read_nothing(self):
        async with await self.chat(headers=self.chat_headers(request_id="before-retirement")) as r:
            self.assertEqual(r.status, 200)
        async with await self.native(stream=False) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        await self.idle()
        self.assertEqual((await self.report())["counts"]["total"], 1)
        self.assertEqual(
            (await self.report(path=NATIVE_USAGE_PATH, token="TS042_TEST_NATIVE"))["counts"][
                "total"
            ],
            1,
        )
        # A registration whose credential reference no longer resolves keeps its name but
        # cannot authenticate, so history stays closed even though the rows exist.
        self.assertNotIn(RETIRED_ENV, os.environ)
        self.assertEqual(
            (await self.usage_status(raw_token=RETIRED_TOKEN))[0],
            401,
        )
        # A registration explicitly marked revoked is refused by the published native
        # authorization relation before any row is read.
        self.assertEqual(
            (await self.usage_status(path=NATIVE_USAGE_PATH, token=REVOKED_ENV))[0], 403
        )
        # The revoked registration is refused on the native route as well: the read port
        # and the routing port agree on one authorization relation.
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=False),
            headers={
                "Authorization": "Bearer " + REVOKED_TOKEN,
                "X-Request-ID": "revoked-route",
                "X-Tianshu-Turn-ID": "revoked-route-turn",
                "X-Tianshu-Native-Config-Version": "7",
            },
        ) as response:
            self.assertEqual(response.status, 403)
            self.assertEqual((await response.json())["code"], "forbidden")

    async def test_expired_or_unregistered_native_identity_cannot_read_history(self):
        async with await self.native(stream=False) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        await self.idle()
        self.assertEqual(
            (await self.report(path=NATIVE_USAGE_PATH, token="TS042_TEST_NATIVE"))["counts"][
                "total"
            ],
            1,
        )
        for token in ("TS042_TEST_NATIVE_OTHER", "TS041_TEST_CLIENT", "TS041_TEST_PLATFORM"):
            with self.subTest(token=token):
                status, _ = await self.usage_status(path=NATIVE_USAGE_PATH, token=token)
                self.assertIn(status, (401, 403))
        self.assertEqual(
            (await self.usage_status(path=NATIVE_USAGE_PATH, token="TS042_TEST_NATIVE_OTHER"))[0],
            403,
        )

    async def test_the_report_survives_a_restart_on_the_same_ledger(self):
        async with await self.chat(headers=self.chat_headers(request_id="before-restart")) as resp:
            self.assertEqual(resp.status, 200)
        await self.idle()
        before = await self.report()
        restarted = create_app(self.settings)
        runner, url = await start_http(restarted)
        try:
            async with self.client.get(
                url + USAGE_PATH + query(window()),
                headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]},
            ) as response:
                self.assertEqual(response.status, 200)
                after = await response.json()
            async with self.client.get(
                url + "/internal/v1/model-requests/before-restart",
                headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]},
            ) as response:
                self.assertEqual(response.status, 200)
        finally:
            await runner.cleanup()
        self.assertEqual(after["counts"], before["counts"])
        self.assertEqual(after["usage"], before["usage"])
        self.assertEqual(after["latency_ms"], before["latency_ms"])

    async def test_report_parameters_are_validated_over_http(self):
        for values in (
            {"view": "rows"},
            {"limit": "0"},
            {"limit": "5001"},
            {"offset": "-1"},
            {"since": "2026-01-01"},
            {"vendor": "1"},
            {"since": "2024-01-01T00:00:00Z", "until": "2026-01-01T00:00:00Z"},
        ):
            with self.subTest(values=values):
                self.assertEqual((await self.usage_status(**values))[0], 400)
        self.assertEqual((await self.usage_status(raw_query="?limit=1&limit=2"))[0], 400)
        self.assertEqual((await self.usage_status(raw_query="?limit=1"))[0], 200)
        self.assertEqual(
            (await self.usage_status(raw_query="?view=attempts&limit=1&offset=0"))[0], 200
        )

    async def test_paging_is_bounded_by_the_row_cap(self):
        for index in range(5):
            async with await self.chat(
                headers=self.chat_headers(request_id=f"paged-{index}")
            ) as response:
                self.assertEqual(response.status, 200)
        await self.idle()
        first = await self.report(view="attempts", limit="2", offset="0")
        self.assertEqual(first["coverage"]["matching"], 5)
        self.assertEqual(first["coverage"]["scanned"], 2)
        self.assertTrue(first["coverage"]["truncated"])
        self.assertEqual(first["counts"]["total"], 2)
        second = await self.report(view="attempts", limit="2", offset="2")
        self.assertEqual(second["coverage"]["scanned"], 2)
        self.assertEqual(
            set(row["request_id"] for row in first["attempts"])
            & set(row["request_id"] for row in second["attempts"]),
            set(),
        )
        whole = await self.report(view="attempts", limit="5000")
        self.assertEqual(whole["coverage"]["scanned"], 5)
        self.assertFalse(whole["coverage"]["truncated"])

    async def test_the_report_never_exposes_bodies_tools_credentials_or_urls(self):
        self.services.mode = "stream"
        async with await self.chat(
            headers=self.chat_headers(request_id="private-attempt"), stream=True
        ) as response:
            await response.read()
        await self.idle()
        for view in ("summary", "attempts"):
            document = json.dumps(await self.report(view=view))
            for forbidden in (
                "正文",
                "你好",
                "fixture_readonly",
                '"q":',
                SECRETS["TS041_TEST_UPSTREAM"],
                SECRETS["TS041_TEST_CLIENT"],
                self.upstream_url,
                "native_usage",
                "prompt_tokens",
                "completion_tokens",
                "unknown_vendor_detail",
            ):
                self.assertNotIn(forbidden, document)
            self.assertNotIn(self.ledger_path, document)


class UsageCliTests(unittest.IsolatedAsyncioTestCase):
    """The read-only local CLI against a real ledger written by the real gateway."""

    async def asyncSetUp(self):
        self.environment = patch.dict(os.environ, SECRETS)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ledger_path = str(Path(self.temp.name) / "cli.sqlite")
        self.services = RecordingServices()
        upstream_app = web.Application()
        upstream_app.router.add_post("/v1/chat/completions", self.services.upstream)
        runner, self.upstream_url = await start_http(upstream_app)
        self.addAsyncCleanup(runner.cleanup)
        self.services.configure(self.upstream_url)
        platform_app = web.Application()
        platform_app.router.add_post("/internal/v1/model-config/snapshot", self.services.snapshot)
        runner, self.platform_url = await start_http(platform_app)
        self.addAsyncCleanup(runner.cleanup)
        self.settings = Settings(
            str(CONTRACT),
            self.ledger_path,
            self.platform_url,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            {
                "secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM",
                "secret-ref:fixture/client": "TS041_TEST_CLIENT",
                "secret-ref:fixture/other": "TS041_TEST_OTHER",
                "secret-ref:fixture/platform": "TS041_TEST_PLATFORM",
                "secret-ref:fixture/native": "TS042_TEST_NATIVE",
                "secret-ref:fixture/native-other": "TS042_TEST_NATIVE_OTHER",
            },
            [registration(self.platform_url), registration(self.upstream_url + "/v1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                ),
            ],
            native_enabled=True,
            native_contract_directory=str(NATIVE_CONTRACT),
            native_clients=[
                native_grant(),
                native_grant(
                    service="caller-expired",
                    principal_id="principal-expired",
                    credential_ref="secret-ref:fixture/native-other",
                    expires_at="2020-01-01T00:00:00Z",
                ),
            ],
        )
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10), trust_env=False
        )
        self.addAsyncCleanup(self.client.close)
        self.body = copy.deepcopy(DOCUMENTS["native_request"])

    async def serve_one(self, request_id):
        async with self.client.post(
            self.url + "/v1/chat/completions",
            json=self.body,
            headers={
                "Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"],
                "X-Request-ID": request_id,
                "X-Tianshu-Turn-ID": request_id + "-turn",
                "X-Tianshu-Config-Version": "7",
                "X-Tianshu-Workload": "companion.text",
            },
        ) as response:
            self.assertEqual(response.status, 200)
        async with asyncio.timeout(2):
            while self.app[GATEWAY].active:
                await asyncio.sleep(0.01)

    def settings_document(self):
        document = json.loads(json.dumps(asdict(self.settings)))
        document["diagnostics_path"] = self.ledger_path
        return document

    async def run_cli(self, *arguments):
        path = Path(self.temp.name) / "cli-settings.json"
        path.write_text(json.dumps(self.settings_document()), encoding="utf-8")
        command = [
            sys.executable,
            "-B",
            "-m",
            "tianshu_gateway",
            "usage-report",
            "--settings",
            str(path),
            *arguments,
        ]
        flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=flags,
        )
        out, error = await asyncio.wait_for(process.communicate(), 10)
        return process.returncode, out.decode(), error.decode()

    async def test_cli_reports_the_same_ledger_read_only(self):
        await self.serve_one("cli-attempt")
        values = window()
        code, out, error = await self.run_cli(
            "--service",
            "companion",
            "--since",
            values["since"],
            "--until",
            values["until"],
            "--view",
            "attempts",
        )
        self.assertEqual(code, 0, error)
        report = json.loads(out)
        self.assertEqual(report["key_space"], "chat")
        self.assertEqual(report["identity"], {"service": "companion"})
        self.assertEqual(report["counts"]["total"], 1)
        self.assertEqual(report["attempts"][0]["request_id"], "cli-attempt")
        self.assertEqual(report["attempts"][0]["usage_source"], "upstream_json_usage")
        async with self.client.get(
            self.url + USAGE_PATH + query(values),
            headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]},
        ) as response:
            self.assertEqual(json.loads(await response.text())["counts"], report["counts"])
        for secret in SECRETS.values():
            self.assertNotIn(secret, out + error)

    async def test_cli_reads_the_native_key_space_with_an_explicit_identity(self):
        code, out, error = await self.run_cli(*NATIVE_SELECTORS)
        self.assertEqual(code, 0, error)
        report = json.loads(out)
        self.assertEqual(report["key_space"], "native")
        self.assertEqual(report["identity"]["principal_id"], "principal-fixture")
        self.assertEqual(report["counts"]["total"], 0)
        code, _, error = await self.run_cli(
            "--principal-id",
            "principal-expired",
            "--caller-service",
            "caller-expired",
            "--credential-namespace",
            NAMESPACE,
        )
        self.assertEqual(code, 3)
        self.assertIn("revoked, expired or unauthorized", error)

    async def test_cli_refuses_unregistered_or_unusable_identities(self):
        code, out, error = await self.run_cli("--service", "absent")
        self.assertEqual(code, 3)
        self.assertEqual(out, "")
        self.assertEqual(error.strip(), "usage-report: no chat registration matches --service")
        code, _, _ = await self.run_cli(
            "--principal-id", "principal-fixture", "--caller-service", "caller-fixture"
        )
        self.assertEqual(code, 3)
        code, _, error = await self.run_cli("--service", "companion", "--view", "rows")
        self.assertEqual(code, 2)
        self.assertNotIn(SECRETS["TS041_TEST_CLIENT"], error)
        code, _, _ = await self.run_cli("--service", "companion", "--limit", "0")
        self.assertEqual(code, 2)
        code, _, _ = await self.run_cli("--service", "companion", "--since", "2026-01-01")
        self.assertEqual(code, 2)

    async def test_cli_needs_the_migrated_ledger_and_never_migrates_it(self):
        code, _, error = await self.run_cli("--service", "companion")
        self.assertEqual(code, 0, error)
        # A database that only the older build ever wrote is not migrated by a read.
        old_path = str(Path(self.temp.name) / "old.sqlite")
        older = sqlite3.connect(old_path)
        older.executescript(
            "CREATE TABLE requests (service TEXT NOT NULL, request_id TEXT NOT NULL,"
            " receipt TEXT NOT NULL, reason TEXT NOT NULL, elapsed_ms INTEGER,"
            " upstream_status INTEGER, PRIMARY KEY(service, request_id));"
        )
        older.close()
        self.ledger_path = old_path
        code, out, error = await self.run_cli("--service", "companion")
        self.assertEqual(code, 2)
        self.assertIn("no metric tables", error)
        after = sqlite3.connect(old_path)
        try:
            names = {
                row[0] for row in after.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        finally:
            after.close()
        self.assertEqual(names, {"requests"})


class UsageNativeDisabledTests(unittest.IsolatedAsyncioTestCase):
    """The native report path stays closed while a deployment has native off."""

    async def asyncSetUp(self):
        self.environment = patch.dict(os.environ, SECRETS)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "disabled.sqlite"),
            "http://127.0.0.1:1",
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            {"secret-ref:fixture/client": "TS041_TEST_CLIENT"},
            [registration("http://127.0.0.1:1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                )
            ],
        )
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=False)
        self.addAsyncCleanup(self.client.close)

    async def test_native_usage_path_is_closed_without_native_material(self):
        headers = {"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]}
        async with self.client.get(self.url + NATIVE_USAGE_PATH, headers=headers) as response:
            self.assertEqual(response.status, 501)
            document = await response.json()
        self.assertEqual(document["contract"], "model-protocol/v1")
        self.assertEqual(document["code"], "unsupported_operation")
        async with self.client.get(
            self.url + USAGE_PATH + query(window()), headers=headers
        ) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual((await response.json())["counts"]["total"], 0)
        async with self.client.get(self.url + USAGE_PATH) as response:
            self.assertEqual(response.status, 401)


if __name__ == "__main__":
    unittest.main()
