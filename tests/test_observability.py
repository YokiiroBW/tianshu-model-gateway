"""TS-103: full runtime event registration, the durable gate and the bounded sink.

Every test here is isolated and synthetic: a temporary log directory, a temporary private
ledger, in-process platform and upstream doubles on loopback, and no network egress. Nothing in
this module touches a NAS, a production container, a real account or a paid model call.
"""

import asyncio
import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


from gateway_fixtures import SECRETS, WORKSPACE
from observability_fixtures import (
    FIXTURE_INSTANCE_ID,
    PROBE_TOKEN,
    ObservedTestCase,
    check_providers,
    observation_settings,
    platform_headers,
    read_log,
)
from tianshu_gateway import __main__ as gateway_main
from tianshu_gateway.observability import (
    REGISTRY,
    Observability,
    ObservabilitySettings,
    bind_correlation,
    events,
    health,
    reset_correlation,
    sink,
)
from tianshu_gateway.server import (
    ATTEMPT_TERMINALS,
    OUTCOME_TERMINALS,
    TERMINAL_EVENTS,
    HTTP_STATUS_CODES,
)

DIAGNOSTICS_CONTRACT = WORKSPACE / "contracts/diagnostics/v1"

# The card's required coverage, written as the closed vocabulary it must be met with. Every
# entry is (event, the outcomes it may carry). A missing or renamed event fails this table
# rather than being noticed later in production.
REQUIRED_EVENTS = {
    "runtime.starting": ("started",),
    "runtime.started": ("succeeded",),
    "runtime.startup_failed": ("failed",),
    "runtime.stopping": ("started",),
    "runtime.stopped": ("succeeded",),
    "health.readiness_changed": ("succeeded", "degraded"),
    "request.accepted": ("succeeded",),
    "request.authenticated": ("succeeded",),
    "request.unauthenticated": ("rejected",),
    "request.rejected": ("rejected",),
    "request.finished": ("succeeded", "failed", "cancelled", "unknown", "rejected"),
    "request.route_unmatched": ("rejected",),
    "request.disconnected": ("cancelled",),
    "request.queued": ("started",),
    "request.admitted": ("succeeded",),
    "request.queue_full": ("rejected",),
    "request.queue_expired": ("failed",),
    "request.cancelled": ("cancelled",),
    "request.duplicate": ("rejected",),
    "request.revoked": ("rejected",),
    "upstream.call_started": ("started",),
    "upstream.first_output": ("started",),
    "upstream.call_finished": ("succeeded", "failed", "cancelled", "unknown"),
    "receipt.persist_failed": ("failed",),
    "log.unavailable": ("failed",),
    "log.capacity_exceeded": ("failed",),
    "log.recovered": ("succeeded",),
    "log.segment_sealed": ("succeeded",),
    "log.non_durable": ("degraded",),
    "maintenance.log_recovery_check": ("succeeded", "failed"),
}


class EventRegistrationTests(unittest.TestCase):
    """The static catalogue: registration is complete, closed and free of sampling."""

    def test_every_required_event_is_registered_with_its_outcomes(self):
        for name, outcomes in REQUIRED_EVENTS.items():
            self.assertIn(name, REGISTRY, name)
            self.assertEqual(set(REGISTRY[name].outcomes), set(outcomes), name)

    def test_every_registered_event_has_a_fixed_level_from_the_closed_set(self):
        for spec in events.CATALOGUE:
            self.assertIn(spec.level, events.LEVELS, spec.name)
            self.assertTrue(spec.outcomes, spec.name)
            self.assertIn(spec.outcome, spec.outcomes, spec.name)
            if spec.codes is not None:
                self.assertTrue(spec.codes <= events.ERROR_CODES, spec.name)

    def test_catalogue_names_are_unique_and_match_the_published_pattern(self):
        names = [spec.name for spec in events.CATALOGUE]
        self.assertEqual(len(names), len(set(names)))
        for name in names:
            self.assertRegex(name, r"^[a-z][a-z0-9_.]{0,63}$")

    def test_every_registered_name_outcome_and_code_combination_encodes(self):
        """No registered fact may be silently dropped by the encoder."""
        for spec in events.CATALOGUE:
            codes = sorted(spec.codes) if spec.codes else [None]
            for outcome in sorted(spec.outcomes):
                for code in codes:
                    record = events.build_record(
                        spec.name,
                        instance_id=FIXTURE_INSTANCE_ID,
                        sequence=1,
                        event_id=str(uuid.uuid4()),
                        outcome=outcome,
                        error_code=code,
                        correlation_id="a" * 32,
                        duration_ms=3,
                    )
                    self.assertLessEqual(len(events.encode_record(record)), events.MAX_LINE_BYTES)
                    self.assertEqual(record["event"], spec.name)

    def test_an_unregistered_combination_is_refused(self):
        def build(name, **fields):
            fields.setdefault("instance_id", FIXTURE_INSTANCE_ID)
            fields.setdefault("sequence", 1)
            fields.setdefault("event_id", str(uuid.uuid4()))
            return events.build_record(name, **fields)

        with self.assertRaises(ValueError):
            build("request.accepted", outcome="failed")
        with self.assertRaises(ValueError):
            build("not.a.registered.event")
        with self.assertRaises(ValueError):
            build("request.finished", outcome="succeeded", error_code="not_a_code")
        with self.assertRaises(ValueError):
            # An event that never carries a code refuses one rather than inventing a level.
            build("request.accepted", error_code="internal_error")

    def test_the_two_terminal_tables_name_only_registered_facts(self):
        allowed = REGISTRY["upstream.call_finished"].codes or frozenset()
        for reason, (outcome, code) in ATTEMPT_TERMINALS.items():
            self.assertIn(outcome, REGISTRY["upstream.call_finished"].outcomes, reason)
            if code is not None:
                self.assertIn(code, allowed, reason)
        for outcome, (mapped, code) in OUTCOME_TERMINALS.items():
            self.assertIn(mapped, REGISTRY["upstream.call_finished"].outcomes, outcome)
            if code is not None:
                self.assertIn(code, allowed, outcome)
        for status, code in HTTP_STATUS_CODES.items():
            self.assertIn(code, events.ERROR_CODES, status)

    def test_every_terminal_event_accepts_the_code_a_mapped_status_produces(self):
        for status, code in HTTP_STATUS_CODES.items():
            if status in (404, 405):
                spec = REGISTRY["request.route_unmatched"]
            else:
                spec = REGISTRY["request.rejected"]
            self.assertIn(code, spec.codes or frozenset(), status)

    def test_terminal_events_are_a_subset_of_the_catalogue(self):
        for name in TERMINAL_EVENTS:
            self.assertIn(name, REGISTRY, name)

    def test_queue_fact_mapping_covers_every_registered_admission_outcome(self):
        for outcome, name in events.QUEUE_FACT_EVENTS.items():
            self.assertIn(name, REGISTRY, outcome)
            code = events.QUEUE_FACT_CODES.get(outcome)
            self.assertIn(code, REGISTRY[name].codes or frozenset({None}), outcome)


class ClosedRecordTests(unittest.TestCase):
    """The wire format: exact field order, closed set and the frozen contract."""

    def build(self, name, **fields):
        fields.setdefault("instance_id", FIXTURE_INSTANCE_ID)
        fields.setdefault("sequence", 1)
        fields.setdefault("event_id", str(uuid.uuid4()))
        return events.build_record(name, **fields)

    def test_field_order_is_the_frozen_order(self):
        record = self.build("request.accepted")
        self.assertEqual(
            list(record),
            [
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
            ],
        )

    def test_line_is_utf8_json_with_one_lf_and_a_byte_budget(self):
        line = events.encode_record(self.build("request.accepted"))
        self.assertTrue(line.endswith(b"\n"))
        self.assertEqual(line.count(b"\n"), 1)
        self.assertEqual(json.loads(line.decode("utf-8"))["event"], "request.accepted")
        self.assertLessEqual(len(line), events.MAX_LINE_BYTES)

    def test_non_finite_and_negative_durations_are_refused(self):
        for value in (float("nan"), float("inf"), -1, 1.5, True):
            with self.assertRaises(ValueError):
                self.build("request.accepted", duration_ms=value)

    def test_a_backwards_clock_can_only_produce_zero(self):
        self.assertEqual(events.duration_ms(10.0, 5.0, time.monotonic), 0)
        self.assertEqual(events.duration_ms(10.0, 10.0, time.monotonic), 0)
        self.assertEqual(events.duration_ms(10.0, 10.25, time.monotonic), 250)

    def test_published_contract_is_verified_against_this_catalogue(self):
        contract = events.load_contract(str(DIAGNOSTICS_CONTRACT))
        self.assertEqual(contract.version, events.SCHEMA_VERSION)
        self.assertEqual(contract.status, "development_frozen_pending_joint_acceptance")
        self.assertEqual(
            dict(contract.files)["event.schema.json"],
            "c91ccc13f2a975f234934eb6e7d9ef03d9ad0a650e4c2acbdeb87761b3bf0a53",
        )
        for name in ("README.md", "event.schema.json", "examples.json", "negative-examples.json"):
            self.assertIn(name, dict(contract.files))

    def test_a_tampered_contract_package_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            package = Path(temp) / "diagnostics" / "v1"
            package.mkdir(parents=True)
            for path in DIAGNOSTICS_CONTRACT.iterdir():
                if path.is_file():
                    (package / path.name).write_bytes(path.read_bytes())
            target = package / "event.schema.json"
            target.write_bytes(target.read_bytes() + b"\n")
            with self.assertRaises(ValueError):
                events.load_contract(str(package))

    def test_an_unknown_release_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            package = Path(temp)
            manifest = json.loads(
                (DIAGNOSTICS_CONTRACT / "manifest.json").read_bytes().replace(b"\r\n", b"\n")
            )
            manifest["version"] = "9.9.9"
            (package / "manifest.json").write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                events.load_contract(str(package))

    def test_sequence_is_monotonic_without_gaps(self):
        identity = sink.RuntimeIdentity(instance_id=FIXTURE_INSTANCE_ID)
        numbers = [identity.next_sequence() for _ in range(64)]
        self.assertEqual(numbers, list(range(1, 65)))
        self.assertEqual(
            sink.sequence_gaps([{"instance_id": "x", "sequence": n} for n in numbers]), {}
        )


class CorrelationTests(unittest.TestCase):
    """One legal correlation value per request; nothing else is ever propagated."""

    def test_a_legal_header_is_kept_and_an_illegal_one_is_replaced(self):
        legal = "0123456789abcdef0123456789abcdef"
        token = bind_correlation(legal)
        self.assertEqual(events.current_correlation(), legal)
        reset_correlation(token)
        for presented in (None, "", "not-hex", "A" * 32, "0" * 31, "0" * 33, "z" * 32):
            token = bind_correlation(presented)
            produced = events.current_correlation()
            self.assertRegex(produced, r"^[a-f0-9]{32}$")
            if presented:
                self.assertNotEqual(produced, presented)
            reset_correlation(token)
        self.assertIsNone(events.current_correlation())

    def test_two_generated_correlations_differ(self):
        self.assertNotEqual(events.new_correlation(), events.new_correlation())


class ObservedRequestTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """One real request through a real server, observed end to end."""

    async def asyncSetUp(self):
        await self.observe()
        self.body = self.harness.body()

    async def call(self, body=None, **kwargs):
        header_args = kwargs.pop("header_args", {})
        headers = self.harness.headers(**header_args)
        return await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.body if body is None else body,
            headers=headers,
            **kwargs,
        )

    async def test_one_request_records_its_whole_lifecycle_in_order(self):
        async with await self.call() as response:
            self.assertEqual(response.status, 200)
            await response.read()
        await self.harness.observability.shutdown()
        names = [record["event"] for record in self.harness.records()]
        for required in (
            "runtime.starting",
            "runtime.started",
            "request.accepted",
            "upstream.call_started",
            "upstream.first_output",
            "upstream.call_finished",
            "request.finished",
        ):
            self.assertIn(required, names, names)
        self.assertLess(names.index("request.accepted"), names.index("upstream.call_started"))
        self.assertLess(names.index("upstream.call_started"), names.index("upstream.call_finished"))
        self.assertLess(names.index("upstream.call_finished"), names.index("request.finished"))

    async def test_no_sampling_two_identical_requests_produce_two_full_records(self):
        for _ in range(2):
            async with await self.call() as response:
                await response.read()
        await self.harness.observability.shutdown()
        self.assertEqual(len(self.harness.named("request.accepted")), 2)
        self.assertEqual(len(self.harness.named("upstream.call_started")), 2)
        self.assertEqual(len(self.harness.named("request.finished")), 2)

    async def test_exactly_one_terminal_per_request(self):
        async with await self.call() as response:
            await response.read()
        async with await self.call(body={"model": "x"}) as response:
            self.assertIn(response.status, (400, 422))
            await response.read()
        await self.harness.observability.shutdown()
        terminals = [
            record for record in self.harness.records() if record["event"] in TERMINAL_EVENTS
        ]
        self.assertEqual(len(terminals), 2, [r["event"] for r in terminals])

    async def test_the_log_carries_no_secret_and_no_prompt(self):
        async with await self.call() as response:
            await response.read()
        await self.harness.observability.shutdown()
        corpus = self.harness.corpus()
        for secret in (SECRETS["TS041_TEST_UPSTREAM"], SECRETS["TS041_TEST_PLATFORM"]):
            self.assertNotIn(secret.encode(), corpus)
        self.assertNotIn(SECRETS["TS041_TEST_CLIENT"].encode(), corpus)
        self.assertNotIn("你好".encode(), corpus)
        self.assertNotIn(b"Bearer ", corpus)
        self.assertNotIn(self.harness.upstream_url.encode(), corpus)

    async def test_a_reflected_credential_never_reaches_the_log(self):
        self.harness.services.mode = "secret_json"
        async with await self.call() as response:
            self.assertEqual(response.status, 502)
            document = await response.json()
            self.assertEqual(document["code"], "result_unknown")
        await self.harness.observability.shutdown()
        corpus = self.harness.corpus()
        self.assertNotIn(SECRETS["TS041_TEST_UPSTREAM"].encode(), corpus)
        self.assertIn(b"upstream.call_finished", corpus)
        self.assertIn(b"result_unknown", corpus)

    async def test_a_legal_correlation_is_kept_and_an_illegal_one_is_not_echoed(self):
        legal = "0123456789abcdef0123456789abcdef"
        async with await self.call(header_args={"correlation": legal}) as response:
            await response.read()
        await self.harness.observability.shutdown()
        records = self.harness.records()
        self.assertTrue(records)
        per_request = [
            record for record in records if record["event"].startswith(("request.", "upstream."))
        ]
        self.assertTrue(per_request)
        self.assertEqual({record["correlation_id"] for record in per_request}, {legal})
        self.assertEqual(
            {
                headers.get("X-Tianshu-Correlation-Id")
                for headers in platform_headers(self.harness.services)
            },
            {legal},
        )

    async def test_an_illegal_correlation_becomes_a_fresh_value(self):
        presented = "NOT-A-CORRELATION-ID"
        async with await self.call(header_args={"correlation": presented}) as response:
            await response.read()
        await self.harness.observability.shutdown()
        produced = {
            record["correlation_id"]
            for record in self.harness.records()
            if record["event"].startswith(("request.", "upstream."))
        }
        self.assertEqual(len(produced), 1)
        value = produced.pop()
        self.assertRegex(value, r"^[a-f0-9]{32}$")
        self.assertNotIn(presented, self.harness.corpus().decode("utf-8", "replace"))

    async def test_the_correlation_never_reaches_the_model_upstream(self):
        async with await self.call() as response:
            await response.read()
        for _raw, headers in self.harness.services.calls:
            self.assertNotIn("X-Tianshu-Correlation-Id", headers)

    async def test_an_unknown_route_is_recorded_as_unmatched(self):
        response = await self.client.get(self.harness.url + "/v1/not-published")
        await response.read()
        self.assertEqual(response.status, 404)
        await self.harness.observability.shutdown()
        self.assertTrue(self.harness.named("request.route_unmatched"))
        self.assertEqual(
            self.harness.named("request.route_unmatched")[0]["error_code"], "not_found"
        )

    async def test_an_unauthenticated_request_is_recorded_as_such(self):
        response = await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.body,
            headers={"Authorization": "Bearer " + "x" * 32, "Content-Type": "application/json"},
        )
        await response.read()
        self.assertEqual(response.status, 401)
        await self.harness.observability.shutdown()
        records = self.harness.named("request.unauthenticated")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["error_code"], "unauthorized")

    async def test_the_instance_and_sequence_identify_this_process(self):
        async with await self.call() as response:
            await response.read()
        await self.harness.observability.shutdown()
        records = self.harness.records()
        self.assertEqual({record["instance_id"] for record in records}, {FIXTURE_INSTANCE_ID})
        sequences = [record["sequence"] for record in records]
        self.assertEqual(sequences, list(range(1, len(records) + 1)))
        self.assertEqual(sink.sequence_gaps(records), {})
        self.assertEqual(len({record["event_id"] for record in records}), len(records))


class CanaryLeakTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """Sensitive canaries through every channel a caller controls, scanned in every output.

    The scan covers the stored segments, the emergency channel and the application log, and the
    canaries cover body text, a reflected credential, exception text, a hostile request ID, the
    URL query and the URL path. Each channel is exercised on a real request through a real
    server; nothing here inspects a helper in isolation.
    """

    PROMPT = "canary-prompt-8f14ac02"
    QUERY = "canary-query-3d9e71bb"
    PATH = "canary-path-6a0c4e59"
    HOSTILE_ID = 'canary-request-id-51ea9c7d"}{"event":"injected'
    HEADER = "canary-header-b27f0d31"
    EXCEPTION = "canary-exception-4c8b2e6a"

    async def asyncSetUp(self):
        await self.observe()

    def canaries(self):
        return [
            self.PROMPT,
            self.QUERY,
            self.PATH,
            self.HOSTILE_ID,
            self.HEADER,
            self.EXCEPTION,
            SECRETS["TS041_TEST_UPSTREAM"],
            SECRETS["TS041_TEST_PLATFORM"],
            SECRETS["TS041_TEST_CLIENT"],
        ]

    def assert_clean(self, *extra):
        text = self.all_output().decode("utf-8", "replace")
        for canary in self.canaries():
            self.assertNotIn(canary, text, canary)
            self.assertNotIn(canary, "".join(extra), canary)

    async def test_no_channel_reaches_the_log_the_emergency_stream_or_the_app_log(self):
        body = self.harness.body()
        body["messages"] = [{"role": "user", "content": self.PROMPT}]
        self.harness.services.http_status = 500
        # This mode reflects the upstream credential in the body text, the X-Request-ID header,
        # a Location header and a Set-Cookie header at once.
        self.harness.services.mode = "error"
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=body,
            headers=self.harness.headers(correlation=self.HEADER),
        ) as response:
            visible = await response.text()
            # A failed upstream is reported as a fixed envelope, not by copying its own body or
            # its own headers (which here carry the credential in three different places).
            self.assertEqual(response.status, 500)
        await self.harness.observability.shutdown()
        self.assert_clean(visible)
        # The scan is not vacuous: the events that saw that request are present.
        names = [record["event"] for record in self.harness.records()]
        self.assertIn("request.finished", names)
        self.assertIn("upstream.call_started", names)
        self.assertIn("upstream.call_finished", names)
        self.assertEqual(len(self.harness.services.calls), 1)

    async def test_a_canary_in_the_url_is_refused_and_never_stored(self):
        # A query string on a published business route is refused outright, so the caller's text
        # never reaches a downstream call, a response body or a stored record.
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions?" + self.QUERY,
            json=self.harness.body(),
            headers=self.harness.headers(),
        ) as response:
            self.assertEqual(response.status, 400)
            visible = await response.text()
        # An unpublished path is caller-controlled text too.
        async with self.client.get(self.harness.url + "/v1/" + self.PATH) as response:
            self.assertEqual(response.status, 404)
            visible += await response.text()
        await self.harness.observability.shutdown()
        self.assertEqual(self.harness.services.calls, [])
        self.assert_clean(visible)
        self.assertTrue(self.harness.named("request.route_unmatched"))
        self.assertTrue(self.harness.named("request.rejected"))

    async def test_a_hostile_request_id_is_refused_and_never_stored(self):
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.harness.body(),
            headers=self.harness.headers(request_id=self.HOSTILE_ID),
        ) as response:
            self.assertEqual(response.status, 400)
            visible = await response.text()
        await self.harness.observability.shutdown()
        # Refused before any side effect, and the presented value is neither echoed nor stored.
        self.assertEqual(self.harness.services.calls, [])
        self.assert_clean(visible)
        self.assertTrue(self.harness.named("request.rejected"))
        self.assertNotIn(self.HOSTILE_ID, self.harness.corpus().decode("utf-8", "replace"))

    async def test_a_reflected_credential_never_reaches_any_output(self):
        self.harness.services.mode = "secret_json"
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.harness.body(),
            headers=self.harness.headers(),
        ) as response:
            self.assertEqual(response.status, 502)
            visible = await response.text()
        await self.harness.observability.shutdown()
        self.assert_clean(visible)
        self.assertIn("upstream.call_finished", self.harness.corpus().decode("utf-8", "replace"))

    async def test_a_reflected_credential_in_a_stream_never_reaches_any_output(self):
        self.harness.services.mode = "secret_stream"
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.harness.body(),
            headers=self.harness.headers(),
        ) as response:
            visible = (await response.read()).decode("utf-8", "replace")
        await self.harness.observability.shutdown()
        self.assert_clean(visible)

    async def test_a_malformed_upstream_body_is_never_copied_into_the_log(self):
        self.harness.services.mode = "invalid_json"
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.harness.body(),
            headers=self.harness.headers(),
        ) as response:
            visible = await response.text()
        await self.harness.observability.shutdown()
        self.assert_clean(visible)

    async def test_an_encoded_upstream_body_is_never_copied_into_the_log(self):
        self.harness.services.mode = "encoded"
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.harness.body(),
            headers=self.harness.headers(),
        ) as response:
            visible = await response.text()
        await self.harness.observability.shutdown()
        self.assert_clean(visible)

    async def test_an_illegal_correlation_header_is_neither_echoed_nor_stored(self):
        for presented in (self.HEADER, self.HOSTILE_ID, "0" * 31, "A" * 32, self.QUERY):
            async with await self.client.post(
                self.harness.url + "/v1/chat/completions",
                json=self.harness.body(),
                headers=self.harness.headers(correlation=presented),
            ) as response:
                visible = await response.text()
            self.assertNotIn(presented, visible, presented)
        await self.harness.observability.shutdown()
        self.assert_clean()
        self.assertIn("request.finished", self.harness.corpus().decode("utf-8", "replace"))

    async def test_an_exception_message_can_never_become_a_record_field(self):
        """The wire format has no free-text field, so an exception message cannot be recorded."""
        identity = {"instance_id": FIXTURE_INSTANCE_ID, "sequence": 1, "event_id": "canary-event-1"}
        with self.assertRaises(ValueError):
            events.build_record("failed: " + self.EXCEPTION, **identity)
        for field in ("error_code", "outcome"):
            with self.assertRaises(ValueError, msg=field):
                events.build_record(
                    "request.finished", **identity, **{field: "failed: " + self.EXCEPTION}
                )
        record = events.build_record("request.finished", **identity)
        for field in ("event", "level", "outcome", "error_code", "correlation_id"):
            with self.assertRaises(ValueError, msg=field):
                events.validate_record({**record, field: self.EXCEPTION})
        with self.assertRaises(ValueError):
            events.validate_record({**record, "error_message": self.EXCEPTION})

    async def test_the_probe_never_answers_with_a_canary(self):
        for path in ("/health/live", "/health/ready"):
            for headers in (None, {"Authorization": "Bearer " + self.HEADER}):
                async with self.client.get(self.harness.url + path, headers=headers) as response:
                    visible = await response.text()
                self.assertNotIn(self.HEADER, visible)
                self.assertNotIn(SECRETS["TS041_TEST_CLIENT"], visible)
        await self.harness.observability.shutdown()
        self.assert_clean()


class DurableOrderTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """The durable pre-record is proved from inside the upstream double itself."""

    async def asyncSetUp(self):
        self.observed = []

        def wrapper(services):
            async def inspected(request):
                # Read the stored segments at the instant the model upstream is entered: the
                # durable record must already be there, not merely queued in this process.
                self.observed.append(
                    [record["event"] for record in read_log(self.harness.log_directory)]
                )
                return await services.upstream(request)

            return inspected

        await self.observe(upstream_wrapper=wrapper)
        self.body = self.harness.body()

    async def test_the_durable_record_is_on_disk_before_the_upstream_is_called(self):
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.body,
            headers=self.harness.headers(),
        ) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        self.assertTrue(self.observed, "the upstream double was never entered")
        self.assertIn("upstream.call_started", self.observed[0])


class DurableGateTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """The side-effect gate: recorded durably first, or refused before anything is sent.

    The deployment condition used throughout is a genuinely exhausted directory budget: the log
    directory already holds its configured number of bytes, so the sink must refuse a *new*
    record and report itself unusable. Nothing here pokes a counter or fakes a failure.
    """

    async def asyncSetUp(self):
        await self.observe(fill_log_budget=True)
        self.body = self.harness.body()

    async def post(self):
        return await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.body,
            headers=self.harness.headers(),
        )

    async def test_an_exhausted_budget_refuses_the_business_before_any_side_effect(self):
        self.assertEqual(self.harness.observability.logging_status(), "failed")
        self.assertFalse(self.harness.observability.accepts_new_work())
        for _ in range(3):
            async with await self.post() as response:
                self.assertEqual(response.status, 503)
                document = await response.json()
                self.assertEqual(document["code"], "dependency_unavailable")
        # Not one attempt was forwarded, and the earlier refusals are never retried.
        self.assertEqual(self.harness.services.calls, [])

    async def test_an_exhausted_budget_is_reported_as_capacity_not_as_io(self):
        self.assertEqual(self.harness.observability.state, sink.STATE_UNAVAILABLE)
        self.assertEqual(self.harness.observability.reason, sink.REASON_CAPACITY)
        self.assertFalse(self.harness.observability.accepts_new_work())
        # A capacity refusal is not a successful attempt: nothing in the file claims one.
        await self.harness.observability.shutdown()
        names = [record["event"] for record in self.harness.records()]
        self.assertNotIn("request.finished", names)
        self.assertNotIn("upstream.call_started", names)

    async def test_a_refused_attempt_creates_no_ledger_row(self):
        import sqlite3

        async with await self.post() as response:
            self.assertEqual(response.status, 503)
            await response.read()
        connection = sqlite3.connect(self.harness.settings.diagnostics_path)
        self.addCleanup(connection.close)
        for table in ("requests", "turns", "request_metrics", "admission_metrics"):
            rows = connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
            self.assertEqual(rows, 0, table)

    async def test_no_stored_byte_is_destroyed_by_a_refusal(self):
        before = {
            path.name: path.read_bytes()
            for path in sink.segment_paths(self.harness.log_directory)
            if path.stat().st_size < 4096
        }
        async with await self.post() as response:
            self.assertEqual(response.status, 503)
            await response.read()
        after = {
            path.name: path.read_bytes()
            for path in sink.segment_paths(self.harness.log_directory)
            if path.stat().st_size < 4096
        }
        self.assertEqual(after, before)

    async def test_recovery_allows_a_new_attempt_and_never_resends_the_refused_one(self):
        async with await self.post() as response:
            self.assertEqual(response.status, 503)
            await response.read()
        self.assertEqual(self.harness.services.calls, [])
        # An operator frees the space, then runs the explicit maintenance action. A readiness
        # probe could never do this: it is read-only by construction.
        for path in self.harness.log_directory.iterdir():
            if path.name.endswith(".bin"):
                path.unlink()
        self.assertTrue(self.harness.observability.log.probe_write())
        self.assertEqual(self.harness.observability.logging_status(), "ok")
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
            await response.read()
        # Exactly one new upstream attempt: the refused one was not replayed.
        self.assertEqual(len(self.harness.services.calls), 1)

    async def test_a_readiness_probe_cannot_recover_the_sink(self):
        from observability_fixtures import check_providers

        for _ in range(3):
            response = await self.client.get(
                self.harness.url + "/health/ready",
                headers={"Authorization": "Bearer " + PROBE_TOKEN},
            )
            document = await response.json()
            await response.read()
            self.assertEqual(response.status, 503)
            self.assertEqual(document["status"], health.NOT_READY)
            self.assertEqual(document["checks"]["logging"], "failed")
        self.assertEqual(self.harness.observability.logging_status(), "failed")
        self.assertEqual(check_providers(logging="failed").collect()["logging"], "failed")

    async def test_a_degradation_is_announced_once_on_the_emergency_channel(self):
        stderr = io.StringIO()
        log = sink.RuntimeLog(
            str(self.harness.log_directory),
            instance_id="emergencyfixture0000000000000000001",
            stderr=stderr,
        )
        log.open()
        log._degrade(sink.REASON_IO)
        log._degrade(sink.REASON_IO)
        log._degrade(sink.REASON_CAPACITY)
        lines = [line for line in stderr.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        self.assertIn("log.unavailable", lines[0])
        log.close_sync()


class NonDurableFallbackTests(unittest.IsolatedAsyncioTestCase):
    """No configured directory: explicitly degraded, never silently durable."""

    async def asyncSetUp(self):
        self.environment = patch.dict(
            os.environ, {**SECRETS, "TS103_DIAGNOSTICS_TOKEN": PROBE_TOKEN}
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.stderr = io.StringIO()
        self.observability = Observability(
            observation_settings(None),
            contract=None,
            stderr=self.stderr,
            instance_id="nondurable00000000000000000000001",
        )

    async def test_the_fallback_is_reported_and_readiness_is_never_ready(self):
        await self.observability.start()
        self.assertEqual(self.observability.state, sink.STATE_NON_DURABLE)
        self.assertEqual(self.observability.logging_status(), "non_durable")
        self.assertIn("log.non_durable", self.stderr.getvalue())
        checks = check_providers(logging=self.observability.logging_status()).collect()
        self.assertEqual(health.ready_status(checks), health.NOT_READY)
        self.assertFalse(self.observability.readiness_ok)
        await self.observability.shutdown()

    async def test_the_fallback_still_records_what_it_can_on_the_emergency_channel(self):
        await self.observability.start()
        self.assertTrue(self.observability.event("request.accepted"))
        await self.observability.shutdown()
        self.assertIn("request.accepted", self.stderr.getvalue())

    async def test_a_durable_record_cannot_be_promised_without_a_directory(self):
        """The fallback admits the work, and says so; it never claims durability."""
        await self.observability.start()
        self.assertTrue(await self.observability.durable_event("upstream.call_started"))
        self.assertEqual(self.observability.state, sink.STATE_NON_DURABLE)
        self.assertEqual(self.observability.logging_status(), "non_durable")
        await self.observability.shutdown()


class BoundedSinkTests(unittest.IsolatedAsyncioTestCase):
    """Bounded memory and a bounded directory: refuse the new, never destroy the old."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "log"
        self.stderr = io.StringIO()

    def log(self, **overrides):
        fields = {
            "instance_id": "boundedfixture000000000000000000001",
            "max_directory_bytes": sink.MIN_DIRECTORY_BYTES,
            "stderr": self.stderr,
        }
        fields.update(overrides)
        return sink.RuntimeLog(str(self.directory), **fields)

    async def test_a_full_directory_refuses_the_new_record_and_keeps_the_old(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        filler = self.directory / "gateway-operator-filler.bin"
        with open(filler, "wb") as handle:
            handle.truncate(sink.MIN_DIRECTORY_BYTES)
        log = self.log()
        log.open()
        await log.start()
        self.addAsyncCleanup(log.shutdown)
        self.assertFalse(await log.submit_durable("request.accepted"))
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        self.assertEqual(log.reason, sink.REASON_CAPACITY)
        self.assertIn("log.capacity_exceeded", self.stderr.getvalue())
        self.assertFalse(log.accepts_new_work())
        self.assertEqual(log.check_status(), "failed")
        # Nothing was deleted, truncated or overwritten to make room for the refused record.
        self.assertTrue(filler.exists())
        self.assertEqual(filler.stat().st_size, sink.MIN_DIRECTORY_BYTES)
        self.assertEqual(read_log(self.directory), [])

    async def test_a_full_queue_refuses_the_new_record_without_evicting(self):
        log = self.log()
        log.open()
        log._signal = None
        accepted = 0
        for _ in range(sink.QUEUE_ENTRIES + 5):
            if log.submit("request.accepted"):
                accepted += 1
        self.assertEqual(accepted, sink.QUEUE_ENTRIES)
        self.assertEqual(log.stats()["dropped"], 5)
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        log.close_sync()

    async def test_the_queue_is_bounded_in_bytes_as_well_as_entries(self):
        self.assertLessEqual(sink.QUEUE_BYTES, 8 * 1024 * 1024)
        self.assertLessEqual(sink.QUEUE_ENTRIES, 1024)
        self.assertEqual(sink.SEGMENT_BYTES, 64 * 1024 * 1024)
        self.assertEqual(sink.MIN_DIRECTORY_BYTES, 32 * 1024 * 1024)
        self.assertEqual(sink.MAX_DIRECTORY_BYTES, 64 * 1024 * 1024 * 1024)

    async def test_no_stored_segment_is_ever_deleted(self):
        foreign = self.directory / "operator-note.jsonl"
        self.directory.mkdir(parents=True, exist_ok=True)
        foreign.write_bytes(b"pre-existing operator content\n")
        log = self.log()
        log.open()
        await log.start()
        for _ in range(8):
            log.submit("request.accepted")
        await log.shutdown()
        self.assertTrue(foreign.exists())
        self.assertEqual(foreign.read_bytes(), b"pre-existing operator content\n")
        self.assertGreaterEqual(len(sink.segment_paths(self.directory)), 1)

    async def test_segment_names_carry_the_instance_identity(self):
        log = self.log()
        log.open()
        await log.start()
        log.submit("request.accepted")
        await log.shutdown()
        for path in sink.segment_paths(self.directory):
            self.assertIn(log.instance_id, path.name)

    async def test_two_processes_get_independent_identity_and_sequences(self):
        first = self.log(instance_id="firstprocess0000000000000000000001")
        first.open()
        await first.start()
        first.submit("request.accepted")
        await first.shutdown()
        second = self.log(instance_id="secondprocess000000000000000000001")
        second.open()
        await second.start()
        second.submit("request.accepted")
        await second.shutdown()
        records = read_log(self.directory)
        self.assertEqual(len({record["instance_id"] for record in records}), 2)
        self.assertEqual([record["sequence"] for record in records], [1, 1])
        self.assertEqual(sink.sequence_gaps(records), {})

    async def test_a_sequence_hole_is_detectable_rather_than_silent(self):
        records = [
            {"instance_id": "x", "sequence": 1},
            {"instance_id": "x", "sequence": 3},
        ]
        self.assertEqual(sink.sequence_gaps(records), {"x": [2]})


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    """Recovery is proved by a real successful write, never by a probe."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "log"
        self.stderr = io.StringIO()

    async def test_a_probe_cannot_recover_and_a_real_write_can(self):
        log = sink.RuntimeLog(
            str(self.directory),
            instance_id="recoveryfixture0000000000000000001",
            stderr=self.stderr,
        )
        log.open()
        await log.start()
        log._degrade(sink.REASON_IO)
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        self.assertFalse(log.accepts_new_work())
        # Read-only inspection changes nothing.
        self.assertEqual(log.check_status(), "failed")
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        self.assertTrue(log.probe_write())
        self.assertEqual(log.state, sink.STATE_OK)
        self.assertEqual(log.check_status(), "ok")
        self.assertTrue(log.accepts_new_work())
        await log.shutdown()
        names = [record["event"] for record in read_log(self.directory)]
        self.assertIn("maintenance.log_recovery_check", names)
        self.assertIn("log.recovered", names)

    async def test_an_unusable_directory_stays_unavailable(self):
        target = Path(self.temp.name) / "not-a-directory"
        target.write_bytes(b"x")
        log = sink.RuntimeLog(
            str(target / "log"),
            instance_id="recoveryfixture0000000000000000002",
            stderr=self.stderr,
        )
        log.open()
        await log.start()
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        self.assertFalse(log.probe_write())
        self.assertIn("log.unavailable", self.stderr.getvalue())
        log.close_sync()


class MaintenanceCommandTests(unittest.TestCase):
    """The explicit maintenance subcommand is the only thing that may prove recovery."""

    def deployment(self, log_directory):
        return {
            "contract_directory": str(WORKSPACE / "contracts/text-dialogue/v1"),
            "diagnostics_path": str(Path(log_directory).parent / "diagnostics.sqlite"),
            "platform_base_url": "https://platform.internal",
            "platform_credential_ref": "platform",
            "platform_origin_env": "TS041_TEST_ORIGIN",
            "secret_references": {"platform": "TS041_TEST_PLATFORM"},
            "targets": [{"base_url": "https://platform.internal", "addresses": ["10.0.0.10"]}],
            "clients": [
                {
                    "service": "companion",
                    "credential_ref": "platform",
                    "provider_id": "provider-primary",
                    "config_version": 1,
                }
            ],
            "observability": {"log_directory": str(log_directory)},
        }

    def run_command(self, settings_path):
        return subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "tianshu_gateway",
                "log-recovery-check",
                "--settings",
                str(settings_path),
            ],
            capture_output=True,
            cwd=str(WORKSPACE.parent),
            timeout=60,
        )

    def test_the_command_proves_recovery_with_a_real_write(self):
        with tempfile.TemporaryDirectory() as temp:
            log_directory = Path(temp) / "log"
            settings = Path(temp) / "settings.json"
            settings.write_text(json.dumps(self.deployment(log_directory)))
            result = self.run_command(settings)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout.decode("utf-8").strip().splitlines()[-1])
            self.assertEqual(report["result"], "recovered")
            self.assertEqual(report["state"], "ok")
            names = [record["event"] for record in read_log(log_directory)]
            self.assertIn("maintenance.log_recovery_check", names)

    def test_the_command_reports_an_unusable_directory_with_a_nonzero_exit(self):
        with tempfile.TemporaryDirectory() as temp:
            blocker = Path(temp) / "blocker"
            blocker.write_bytes(b"x")
            settings = Path(temp) / "settings.json"
            settings.write_text(json.dumps(self.deployment(blocker / "log")))
            result = self.run_command(settings)
            self.assertEqual(result.returncode, 1, result.stdout)
            report = json.loads(result.stdout.decode("utf-8").strip().splitlines()[-1])
            self.assertEqual(report["result"], "failed")
            self.assertEqual(report["state"], "unavailable")

    def test_an_unconfigured_log_directory_is_reported_as_such(self):
        with tempfile.TemporaryDirectory() as temp:
            document = self.deployment(Path(temp) / "log")
            document.pop("observability")
            settings = Path(temp) / "settings.json"
            settings.write_text(json.dumps(document))
            result = self.run_command(settings)
            self.assertEqual(result.returncode, 2, result.stdout)
            self.assertIn("not_configured", result.stdout.decode("utf-8"))

    def test_the_command_writes_no_business_state(self):
        with tempfile.TemporaryDirectory() as temp:
            log_directory = Path(temp) / "log"
            settings = Path(temp) / "settings.json"
            settings.write_text(json.dumps(self.deployment(log_directory)))
            self.run_command(settings)
            self.assertFalse((Path(temp) / "diagnostics.sqlite").exists())


class EntryAssemblyTests(unittest.TestCase):
    """The entry point assembles; the adapter never reaches back into a business rule."""

    def test_no_adapter_is_built_without_an_observation_block(self):
        from tianshu_gateway.server import load_settings

        document = {
            "contract_directory": str(WORKSPACE / "contracts/text-dialogue/v1"),
            "diagnostics_path": "diagnostics.sqlite",
            "platform_base_url": "https://platform.internal",
            "platform_credential_ref": "platform",
            "platform_origin_env": "TS041_TEST_ORIGIN",
            "secret_references": {"platform": "TS041_TEST_PLATFORM"},
            "targets": [{"base_url": "https://platform.internal", "addresses": ["10.0.0.10"]}],
            "clients": [
                {
                    "service": "companion",
                    "credential_ref": "platform",
                    "provider_id": "provider-primary",
                    "config_version": 1,
                }
            ],
        }
        settings = load_settings(json.dumps(document).encode())
        self.assertIsNone(settings.observability)
        self.assertIsNone(settings.diagnostics_contract_directory)
        self.assertIsNone(gateway_main.build_observability(settings))

    def test_an_unusable_observation_block_stops_the_start(self):
        for block in (
            {"log_directory": "relative/path"},
            {"log_directory": "/var/log/tianshu", "max_directory_bytes": 1024},
            {"log_directory": "/var/log/tianshu", "probe_budget_ms": 0},
            {"log_directory": "/var/log/tianshu", "probe_token_env": ""},
            {"log_directory": "/var/log/tianshu", "observation_validity_seconds": -1},
        ):
            with self.assertRaises(ValueError):
                ObservabilitySettings(**block).validate()

    def test_the_adapter_module_never_imports_a_business_module(self):
        for name in ("events", "sink", "health"):
            source = (Path(events.__file__).parent / (name + ".py")).read_text(encoding="utf-8")
            for forbidden in (
                "from ..server",
                "from ..config",
                "from ..diagnostics",
                "from ..routing",
                "from ..scheduler",
                "from ..responses",
                "from ..native",
            ):
                self.assertNotIn(forbidden, source, name)

    def test_the_probe_never_writes_and_never_touches_a_dependency(self):
        """Read-only is a property of the code path, checked at the source level."""
        source = Path(health.__file__).read_text(encoding="utf-8")
        body = source.split('"""', 2)[2]
        for forbidden in ("open(", "os.makedirs", "sqlite3", "aiohttp", "submit(", "fsync"):
            self.assertNotIn(forbidden, body, forbidden)


class ProbeIsolationTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """A probe is read-only: it records nothing and changes nothing."""

    async def asyncSetUp(self):
        await self.observe()

    async def test_a_probe_writes_no_record_and_changes_no_state(self):
        await self.harness.settle()
        before = self.harness.corpus()
        names_before = [record["event"] for record in self.harness.records()]
        for path in ("/health/live", "/health/live", "/health/ready", "/health/ready"):
            response = await self.client.get(
                self.harness.url + path,
                headers={"Authorization": "Bearer " + PROBE_TOKEN},
            )
            await response.read()
        await self.harness.observability.shutdown()
        # Byte-identical: no record, no sequence number, no instance state moved.
        self.assertEqual(self.harness.corpus(), before)
        self.assertEqual([record["event"] for record in self.harness.records()], names_before)
        self.assertEqual(self.harness.named("request.accepted"), [])

    async def test_a_probe_consumes_no_capacity_and_creates_no_ledger_row(self):
        import sqlite3

        gateway = self.harness.gateway
        for _ in range(4):
            response = await self.client.get(
                self.harness.url + "/health/ready",
                headers={"Authorization": "Bearer " + PROBE_TOKEN},
            )
            await response.read()
        if gateway.scheduler is not None:
            self.assertEqual(gateway.scheduler.inflight, 0)
            self.assertEqual(gateway.scheduler.waiting, 0)
        connection = sqlite3.connect(self.harness.settings.diagnostics_path)
        self.addCleanup(connection.close)
        rows = connection.execute("SELECT COUNT(*) FROM requests").fetchone()[0]
        self.assertEqual(rows, 0)

    async def test_a_probe_does_not_refresh_the_configuration_cache(self):
        self.harness.services.config_calls.clear()
        for _ in range(3):
            response = await self.client.get(
                self.harness.url + "/health/ready",
                headers={"Authorization": "Bearer " + PROBE_TOKEN},
            )
            await response.read()
        self.assertEqual(self.harness.services.config_calls, [])


class _SlowHandle:
    """A real segment handle with exactly one deliberately slow operation.

    The tests that prove "no disk operation runs on the event loop" need a genuinely slow disk,
    not a mocked one: every operation still reaches the real file, it just takes measurable
    time. Only the named operation is delayed, so the measurement is attributable.
    """

    def __init__(self, handle, slow, delay):
        self.handle = handle
        self.slow = slow
        self.delay = delay

    def __getattr__(self, name):
        return getattr(self.handle, name)

    def _pause(self, name):
        if self.slow == name:
            time.sleep(self.delay)

    def write(self, data):
        self._pause("write")
        return self.handle.write(data)

    def flush(self):
        self._pause("flush")
        return self.handle.flush()

    def close(self):
        self._pause("close")
        return self.handle.close()


def _slow_fsync(delay):
    real = sink.os.fsync

    def fsync(fd):
        time.sleep(delay)
        return real(fd)

    return fsync


def _stuck_fsync(release):
    real = sink.os.fsync

    def fsync(fd):
        release.wait(30.0)
        return real(fd)

    return fsync


class SinkTestCase(unittest.TestCase):
    """A raw sink on a temporary directory, with no server and no event loop around it."""

    def setUp(self):
        self.temp = Path(tempfile.mkdtemp(prefix="ts103-sink-"))
        self.addCleanup(shutil.rmtree, self.temp, ignore_errors=True)
        self.stderr = io.StringIO()

    def build(self, **overrides):
        options = {"stderr": self.stderr}
        options.update(overrides)
        return sink.RuntimeLog(self.temp, **options)

    def stored(self):
        return sum(path.stat().st_size for path in sink.segment_paths(self.temp))

    def fill(self, log, room):
        """Make the current segment and the whole directory exactly ``room`` bytes big.

        A truncated file is a sparse file, so the real 64 MiB and 32 MiB thresholds below cost
        no disk. The handle is left at the end so the next append really appends.
        """
        log._handle.truncate(room)
        log._handle.seek(0, os.SEEK_END)
        log._directory_bytes = room
        log._segment_bytes = room


class SlowDiskTests(SinkTestCase, unittest.IsolatedAsyncioTestCase):
    """R1: the durable writer owns the disk, so a slow disk cannot stall the event loop.

    The reproduction injected 250 ms into ``fsync`` and measured a 20 ms heartbeat taking
    250 ms. Every operation the writer performs is now measured the same way: the write must
    still wait for the real acknowledgement, and the loop must keep its own cadence while it
    waits.
    """

    async def heartbeat(self, ticks):
        while True:
            mark = time.monotonic()
            await asyncio.sleep(0.02)
            ticks.append(time.monotonic() - mark)

    async def ticker(self):
        ticks = []
        task = asyncio.create_task(self.heartbeat(ticks))
        self.addCleanup(task.cancel)
        return ticks, task

    async def test_no_slow_disk_operation_delays_the_loop(self):
        for slow in ("write", "flush", "fsync"):
            with self.subTest(slow=slow):
                directory = self.temp / slow
                log = sink.RuntimeLog(directory, stderr=self.stderr).open()
                await log.start()
                self.addCleanup(log.close_sync)
                ticks, task = await self.ticker()
                if slow == "fsync":
                    context = patch.object(sink.os, "fsync", _slow_fsync(0.25))
                else:
                    log._handle = _SlowHandle(log._handle, slow, 0.25)
                    context = contextlib.nullcontext()
                with context:
                    mark = time.monotonic()
                    self.assertTrue(await log.submit_durable("runtime.starting"))
                    waited = time.monotonic() - mark
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                # The acknowledgement really waited for the slow operation...
                self.assertGreaterEqual(waited, 0.25)
                # ...and the loop kept ticking at its own 20 ms cadence throughout.
                self.assertTrue(ticks, "the heartbeat never ran")
                self.assertLess(max(ticks), 0.15)
                await log.shutdown()

    async def test_the_durable_answer_comes_only_after_the_record_is_on_disk(self):
        log = self.build().open()
        await log.start()
        self.addCleanup(log.close_sync)
        with patch.object(sink.os, "fsync", _slow_fsync(0.25)):
            mark = time.monotonic()
            self.assertTrue(await log.submit_durable("runtime.starting"))
            waited = time.monotonic() - mark
        self.assertGreaterEqual(waited, 0.25)
        stored = read_log(self.temp)
        self.assertEqual([record["event"] for record in stored], ["runtime.starting"])
        await log.shutdown()

    async def test_a_slow_close_does_not_delay_the_loop_during_shutdown(self):
        log = self.build().open()
        await log.start()
        real = log._handle
        log._handle = _SlowHandle(real, "close", 0.25)
        ticks, task = await self.ticker()
        mark = time.monotonic()
        await log.shutdown()
        elapsed = time.monotonic() - mark
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertLess(max(ticks), 0.15)
        # The owner thread closed its own handle: no second writer ever touched it.
        self.assertTrue(real.closed)

    async def test_a_stuck_disk_cannot_hold_shutdown_open_and_never_replays_the_write(self):
        log = self.build().open()
        await log.start()
        release = threading.Event()
        self.addCleanup(release.set)
        with patch.object(sink.os, "fsync", _stuck_fsync(release)):
            waiter = asyncio.create_task(log.submit_durable("runtime.starting"))
            await asyncio.sleep(0.05)
            mark = time.monotonic()
            await log.shutdown(0.2)
            elapsed = time.monotonic() - mark
            # A write that has not returned is not a failed write: the caller is refused, and
            # the sink never claims a durability it could not prove.
            self.assertFalse(await waiter)
        self.assertLess(elapsed, 1.0)
        self.assertFalse(log.accepts_new_work())
        release.set()
        for _ in range(400):
            if not log.stats()["writer_alive"]:
                break
            await asyncio.sleep(0.01)
        self.assertFalse(log.stats()["writer_alive"], "the owner thread never finished")
        # The record is written at most once: a timeout never resends a model call or a message.
        lines = [record for record in read_log(self.temp) if record["event"] == "runtime.starting"]
        self.assertLessEqual(len(lines), 1)

    async def test_a_cancelled_wait_does_not_duplicate_or_lose_the_record(self):
        log = self.build().open()
        await log.start()
        self.addCleanup(log.close_sync)
        with patch.object(sink.os, "fsync", _slow_fsync(0.25)):
            waiter = asyncio.create_task(log.submit_durable("runtime.starting"))
            await asyncio.sleep(0.05)
            waiter.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiter
        await log.flush()
        names = [record["event"] for record in read_log(self.temp)]
        self.assertEqual(names.count("runtime.starting"), 1)
        # The sink is still usable and the next record is durable: cancelling a waiter is not a
        # failure of the sink.
        self.assertTrue(await log.submit_durable("runtime.started"))
        await log.shutdown()

    async def test_the_hand_off_stays_bounded_and_never_blocks_the_submitter(self):
        log = self.build().open()
        await log.start()
        self.addCleanup(log.close_sync)
        release = threading.Event()
        self.addCleanup(release.set)
        with patch.object(sink.os, "fsync", _stuck_fsync(release)):
            await asyncio.sleep(0)
            marks = []
            for _ in range(sink.QUEUE_ENTRIES + sink.BATCH_LIMIT + 200):
                mark = time.monotonic()
                log.submit("runtime.starting")
                marks.append(time.monotonic() - mark)
            # A full hand-off refuses the new record instead of evicting, blocking or growing.
            self.assertLess(max(marks), 0.05)
            self.assertGreater(log.stats()["overflowed"], 0)
            self.assertFalse(log.accepts_new_work())
            self.assertLessEqual(log.stats()["queued"], sink.QUEUE_ENTRIES)
        release.set()


class SlowDiskLivenessTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """R1 through the real HTTP surface: a stuck disk delays one attempt, nothing else."""

    async def asyncSetUp(self):
        await self.observe()

    async def test_liveness_still_answers_while_a_durable_write_is_waiting(self):
        release = threading.Event()
        self.addCleanup(release.set)
        with patch.object(sink.os, "fsync", _stuck_fsync(release)):
            waiter = asyncio.create_task(
                self.harness.observability.durable_event("upstream.call_started")
            )
            await asyncio.sleep(0.05)
            mark = time.monotonic()
            async with await self.client.get(self.harness.url + "/health/live") as response:
                self.assertEqual(response.status, 200)
                await response.json()
            elapsed = time.monotonic() - mark
            self.assertLess(elapsed, 1.0)
            # The attempt itself is still waiting for its real acknowledgement.
            self.assertFalse(waiter.done())
        release.set()
        self.assertTrue(await waiter)


class UnifiedBudgetTests(SinkTestCase):
    """R2: seal, recovery and maintenance bytes are reserved exactly like a business record.

    The reproduction measured a 64 MiB rotation over budget by 326 bytes and a 32 MiB recovery
    over budget by 322 bytes, with the sink still accepting new work. Both thresholds are real
    constants, so both are exercised at their real size -- as sparse files, which cost no disk.
    """

    def test_a_seal_at_the_segment_boundary_writes_nothing_at_all(self):
        log = self.build(max_directory_bytes=sink.SEGMENT_BYTES).open()
        self.addCleanup(log.close_sync)
        self.fill(log, sink.SEGMENT_BYTES - 1)
        line = events.encode_record(log.record("runtime.starting"))
        self.assertFalse(log._write(line))
        # Not one byte past the budget, and the sink says so instead of quietly overrunning it.
        self.assertEqual(self.stored(), sink.SEGMENT_BYTES - 1)
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        self.assertEqual(log.reason, sink.REASON_CAPACITY)
        self.assertFalse(log.accepts_new_work())

    def test_a_recovery_record_at_the_directory_boundary_writes_nothing_at_all(self):
        log = self.build(max_directory_bytes=sink.MIN_DIRECTORY_BYTES).open()
        self.addCleanup(log.close_sync)
        line = events.encode_record(log.record("runtime.starting"))
        room = sink.MIN_DIRECTORY_BYTES - len(line)
        self.fill(log, room)
        log._degrade(sink.REASON_IO)
        # The line itself would fit; the recovery record that must follow it does not, so the
        # pair is refused as a whole rather than half-written.
        self.assertFalse(log._write(line))
        self.assertEqual(self.stored(), room)
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        self.assertFalse(log.accepts_new_work())

    def test_a_seal_and_its_record_are_reserved_together(self):
        # The same arithmetic at a shrinkable equivalent boundary: the real thresholds are
        # covered above, and this one can be measured byte by byte.
        with patch.object(sink, "SEGMENT_BYTES", 2048):
            log = self.build(max_directory_bytes=4096).open()
            self.addCleanup(log.close_sync)
            line = events.encode_record(log.record("runtime.starting"))
            seal = log.identity.measure("log.segment_sealed")
            # Room for the line, deliberately not for the line plus the seal record.
            room = 4096 - seal - len(line) + 1
            self.fill(log, room)
            self.assertFalse(log._write(line))
            self.assertEqual(self.stored(), room)
            self.assertEqual(log.reason, sink.REASON_CAPACITY)

    def test_a_seal_that_fits_is_written_with_its_record(self):
        with patch.object(sink, "SEGMENT_BYTES", 2048):
            log = self.build(max_directory_bytes=4096).open()
            self.addCleanup(log.close_sync)
            line = events.encode_record(log.record("runtime.starting"))
            seal = log.identity.measure("log.segment_sealed")
            room = 4096 - seal - len(line)
            self.fill(log, room)
            self.assertTrue(log._write(line))
            self.assertTrue(log._sync())
            # Only the new segment is read: the filled one holds the zeros that made it full, and
            # those are not records.
            with open(sink.segment_paths(self.temp)[-1], "rb") as handle:
                names = [events.decode_line(raw)["event"] for raw in handle if raw.strip()]
            self.assertIn("log.segment_sealed", names)
            self.assertIn("runtime.starting", names)
            self.assertLessEqual(self.stored(), 4096)
            self.assertEqual(len(sink.segment_paths(self.temp)), 2)

    def test_a_rotation_while_degraded_reserves_its_recovery_record_too(self):
        with patch.object(sink, "SEGMENT_BYTES", 2048):
            log = self.build(max_directory_bytes=4096).open()
            self.addCleanup(log.close_sync)
            log._degrade(sink.REASON_IO)
            line = events.encode_record(log.record("runtime.starting"))
            seal = log.identity.measure("log.segment_sealed")
            recovered = log.identity.measure("log.recovered")
            # Room for the seal and the line, deliberately not for the recovery record that has
            # to follow them while the sink is degraded: the whole triple is refused, so the
            # budget holds even on the path that used to bypass it.
            room = 4096 - seal - len(line) - recovered + 1
            self.fill(log, room)
            self.assertFalse(log._write(line))
            self.assertEqual(self.stored(), room)
            self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
            self.assertFalse(log.accepts_new_work())

    def test_a_full_segment_rotates_within_the_budget_and_keeps_every_record(self):
        with patch.object(sink, "SEGMENT_BYTES", 2048):
            log = self.build(max_directory_bytes=4096).open()
            self.addCleanup(log.close_sync)
            written = 0
            while log._write(events.encode_record(log.record("runtime.starting"))):
                written += 1
                if written > 200:  # pragma: no cover - the budget is 4 KiB
                    break
            self.assertTrue(log._sync())
            self.assertGreater(written, 0)
            records = read_log(self.temp)
            # Every accepted record is stored exactly once and a refusal leaves no half record
            # behind: the stored bytes reconcile with the count of accepted records.
            self.assertEqual(
                [record["event"] for record in records].count("runtime.starting"), written
            )
            sequences = [record["sequence"] for record in records]
            self.assertEqual(len(sequences), len(set(sequences)))
            self.assertLessEqual(self.stored(), 4096)
            self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
            self.assertFalse(log.accepts_new_work())
            # Every allocated number is accounted for: 1..N are stored, one record per number and
            # no hole inside the range; N+1 is the record that was refused, and N+2 is the
            # emergency line that announces the refusal -- which by construction cannot be written
            # into the file that just refused it. That line carries its own number, so an operator
            # can match every missing sequence to its cause.
            self.assertEqual(sink.sequence_gaps(records), {})
            self.assertEqual(sorted(sequences), list(range(1, len(records) + 1)))
            emergency = json.loads(self.stderr.getvalue().strip().splitlines()[-1])
            self.assertEqual(emergency["event"], "log.capacity_exceeded")
            self.assertEqual(emergency["sequence"], len(records) + 2)

    def test_recovery_is_declared_only_after_its_own_record_is_durable(self):
        log = self.build().open()
        self.addCleanup(log.close_sync)
        log._degrade(sink.REASON_IO)
        calls = []
        real = sink.os.fsync

        def failing(fd):
            calls.append(fd)
            if len(calls) == 1:
                # The record is written into the buffer, but the sync that would make it durable
                # fails: that is not a successful write and must not be reported as one.
                raise OSError("injected")
            return real(fd)

        with patch.object(sink.os, "fsync", failing):
            self.assertTrue(log._write(events.encode_record(log.record("runtime.starting"))))
            self.assertFalse(log._sync())
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        self.assertFalse(log.accepts_new_work())
        # A real flush+fsync of the recovery record is what declares the healthy state again.
        self.assertTrue(log._recover())
        self.assertEqual(log.state, sink.STATE_OK)
        self.assertIsNone(log.reason)
        self.assertIn("log.recovered", [record["event"] for record in read_log(self.temp)])

    def test_an_interrupted_write_preserves_every_stored_byte(self):
        log = self.build().open()
        self.addCleanup(log.close_sync)
        self.assertTrue(log._write(events.encode_record(log.record("runtime.starting"))))
        self.assertTrue(log._sync())
        before = self.stored()
        good = log._handle

        class _Exploding:
            def __getattr__(self, name):
                return getattr(good, name)

            def write(self, data):
                good.write(data[:10])
                raise OSError("injected")

        log._handle = _Exploding()
        self.assertFalse(log._write(events.encode_record(log.record("runtime.started"))))
        log._handle = good
        self.assertEqual(log.state, sink.STATE_UNAVAILABLE)
        self.assertFalse(log.accepts_new_work())
        # The sink refuses new work; it never rewrites, truncates or repairs by deleting, and the
        # records that were already stored are still readable.
        self.assertGreaterEqual(self.stored(), before)
        self.assertEqual([record["event"] for record in read_log(self.temp)], ["runtime.starting"])


class TerminalCorrelationTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """R3: the request terminal is written inside its own correlation context.

    The reproduction posted a Chat request, then read the receipt with a correlation of its own
    and found ``request.finished`` with a null correlation. A non-forwarding success has no
    transfer path to record its terminal, so the middleware must do it before releasing the
    context.
    """

    async def asyncSetUp(self):
        await self.observe()

    def presented(self):
        return "b" * 32

    async def test_a_receipt_read_keeps_its_own_correlation(self):
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.harness.body(),
            headers=self.harness.headers(request_id="probe-receipt"),
        ) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        correlation = self.presented()
        async with await self.client.get(
            self.harness.url + "/internal/v1/model-requests/probe-receipt",
            headers=self.harness.headers(correlation=correlation),
        ) as response:
            self.assertEqual(response.status, 200)
            await response.json()
        await self.harness.settle()
        accepted = [
            record
            for record in self.harness.named("request.accepted")
            if record["correlation_id"] == correlation
        ]
        terminals = [
            record
            for record in self.harness.named("request.finished")
            if record["correlation_id"] == correlation
        ]
        # The non-forwarding success has no transfer path to close it: the middleware must
        # record the terminal inside the same correlation context it opened.
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(terminals), 1)
        self.assertEqual(terminals[0]["correlation_id"], accepted[0]["correlation_id"])
        self.assertEqual(terminals[0]["outcome"], "succeeded")

    async def test_a_usage_report_keeps_its_own_correlation(self):
        correlation = self.presented()
        async with await self.client.get(
            self.harness.url + "/internal/v1/model-usage",
            headers=self.harness.headers(correlation=correlation),
        ) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        await self.harness.settle()
        correlated = [
            record for record in self.harness.records() if record["correlation_id"] == correlation
        ]
        self.assertEqual(
            [record["event"] for record in correlated],
            ["request.accepted", "request.finished"],
        )
        self.assertEqual(correlated[-1]["outcome"], "succeeded")

    async def test_a_rejected_request_keeps_its_own_correlation(self):
        correlation = self.presented()
        headers = self.harness.headers(correlation=correlation)
        headers["Authorization"] = "Bearer not-a-real-credential"
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions", json=self.harness.body(), headers=headers
        ) as response:
            self.assertEqual(response.status, 401)
            await response.read()
        await self.harness.settle()
        rejected = [
            record for record in self.harness.records() if record["correlation_id"] == correlation
        ]
        # The refusal and its own provenance event both carry the correlation of the request that
        # was refused, rather than a null one.
        self.assertEqual(
            [record["event"] for record in rejected],
            ["request.accepted", "request.unauthenticated"],
        )
        self.assertEqual(rejected[-1]["error_code"], "unauthorized")

    async def test_concurrent_requests_never_swap_correlations(self):
        presented = [f"{index:032x}" for index in range(1, 9)]

        async def call(correlation):
            async with await self.client.get(
                self.harness.url + "/internal/v1/model-usage",
                headers=self.harness.headers(correlation=correlation),
            ) as response:
                await response.read()
                return response.status

        statuses = await asyncio.gather(*(call(value) for value in presented))
        self.assertEqual(statuses, [200] * len(presented))
        await self.harness.settle()
        for correlation in presented:
            names = [
                record["event"]
                for record in self.harness.records()
                if record["correlation_id"] == correlation
            ]
            self.assertEqual(names, ["request.accepted", "request.finished"])
        # Nothing was recorded under a correlation this process never saw, and no request was
        # left without a terminal.
        known = set(presented)
        strangers = {
            record["correlation_id"]
            for record in self.harness.records()
            if record["correlation_id"] is not None and record["correlation_id"] not in known
        }
        self.assertEqual(strangers, set())
        self.assertEqual(len(self.harness.named("request.accepted")), len(presented))
        self.assertEqual(len(self.harness.named("request.finished")), len(presented))

    async def test_a_cancelled_request_leaks_its_context_to_nobody(self):
        self.harness.services.mode = "hold"
        correlation = self.presented()
        request = asyncio.create_task(
            self.client.post(
                self.harness.url + "/v1/chat/completions",
                json=self.harness.body(),
                headers=self.harness.headers(correlation=correlation),
            )
        )
        await asyncio.wait_for(self.harness.services.started.wait(), 2)
        request.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await request
        self.harness.services.mode = "record"
        self.harness.services.release.set()
        await asyncio.sleep(0.05)
        after = "c" * 32
        async with await self.client.get(
            self.harness.url + "/internal/v1/model-usage",
            headers=self.harness.headers(correlation=after),
        ) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        await self.harness.settle()
        # The request that ran after the cancellation owns its own correlation, and the cancelled
        # one never handed its context to anybody else.
        self.assertEqual(
            [
                record["event"]
                for record in self.harness.records()
                if record["correlation_id"] == after
            ],
            ["request.accepted", "request.finished"],
        )
        cancelled = [
            record for record in self.harness.records() if record["correlation_id"] == correlation
        ]
        self.assertTrue(cancelled, "the cancelled request recorded nothing at all")
        # The cancelled request's own start and end carry its correlation, and none of its records
        # was re-labelled with the later request's context.
        self.assertTrue(
            {"request.accepted", "request.finished"} <= {record["event"] for record in cancelled}
        )
        self.assertNotIn(after, [record["correlation_id"] for record in cancelled])


if __name__ == "__main__":
    unittest.main()
