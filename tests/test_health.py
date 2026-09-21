"""TS-103: the read-only liveness and readiness surface.

Everything here is isolated and synthetic: a temporary log directory, a temporary private
ledger, in-process doubles on loopback and the fixture certificate. A probe is asserted to be
read-only by comparing the stored bytes before and after, not by trusting a docstring.
"""

import json
import os
import time
import unittest
from unittest.mock import patch

from observability_fixtures import (
    PROBE_TOKEN,
    ObservedTestCase,
    check_providers,
)
from tianshu_gateway.observability import health, sink
from tianshu_gateway.server import LIVE_PATH, READY_PATH

LIVE_BODY = {"status": "alive"}
READY_FIELDS = {"status", "service", "checks"}


class ReadinessRuleTests(unittest.TestCase):
    """The rule itself: the first five checks decide, the other three only inform."""

    def test_only_the_first_five_checks_can_block_readiness(self):
        for name in health.CHECK_NAMES:
            for value in ("failed", "not_configured", "not_verified", "non_durable"):
                checks = dict.fromkeys(health.CHECK_NAMES, "ok")
                checks[name] = value
                expected = health.NOT_READY if name in health.REQUIRED_CHECKS else health.READY
                self.assertEqual(health.ready_status(checks), expected, (name, value))

    def test_the_required_set_is_exactly_the_first_five_named_checks(self):
        self.assertEqual(health.REQUIRED_CHECKS, health.CHECK_NAMES[:5])

    def test_an_optional_dependency_never_makes_basic_chat_unavailable(self):
        checks = dict.fromkeys(health.CHECK_NAMES, "ok")
        checks.update(
            {"platform": "not_verified", "model": "not_verified", "native": "not_configured"}
        )
        self.assertEqual(health.ready_status(checks), health.READY)
        self.assertEqual(health.status_code(health.READY), 200)

    def test_a_check_value_outside_the_closed_vocabulary_becomes_failed(self):
        providers = check_providers(runtime="not_a_real_value", native="also_wrong")
        collected = providers.collect()
        self.assertEqual(collected["runtime"], "failed")
        self.assertEqual(collected["native"], "failed")
        self.assertEqual(set(collected), set(health.CHECK_NAMES))

    def test_one_broken_check_is_only_that_check(self):
        def broken():
            raise RuntimeError("fixture failure")

        providers = check_providers()
        providers = health.CheckProviders(
            configuration=providers.configuration,
            contracts=broken,
            runtime=providers.runtime,
            ledger=providers.ledger,
            logging=providers.logging,
            platform=providers.platform,
            model=providers.model,
            native=providers.native,
        )
        collected = providers.collect()
        self.assertEqual(collected["contracts"], "failed")
        self.assertEqual(collected["runtime"], "ok")

    def test_the_body_is_closed_and_carries_no_extra_field(self):
        body = health.ready_body(dict.fromkeys(health.CHECK_NAMES, "ok"))
        self.assertEqual(set(body), READY_FIELDS)
        self.assertEqual(set(body["checks"]), set(health.CHECK_NAMES))
        raw = health.encode_body(body)
        self.assertNotIn(b"checked_at", raw)
        self.assertLessEqual(len(raw), health.MAX_BODY_BYTES)

    def test_liveness_body_is_exactly_one_field(self):
        self.assertEqual(health.LIVE_BODY, LIVE_BODY)
        self.assertEqual(json.loads(health.encode_body(health.LIVE_BODY)), LIVE_BODY)


class AuthorizationTests(unittest.TestCase):
    """The readiness credential is independent, constant time and never echoed."""

    def test_a_missing_or_malformed_credential_is_unauthorized(self):
        for header in (
            None,
            "",
            "Bearer",
            "Bearer ",
            "Basic abc",
            PROBE_TOKEN,
            "bearer " + PROBE_TOKEN,
        ):
            self.assertEqual(
                health.probe_authorization(header, PROBE_TOKEN), health.STATUS_UNAUTHORIZED, header
            )

    def test_a_wrong_credential_is_unauthorized(self):
        self.assertEqual(
            health.probe_authorization("Bearer " + "x" * len(PROBE_TOKEN), PROBE_TOKEN),
            health.STATUS_UNAUTHORIZED,
        )

    def test_the_right_credential_is_accepted(self):
        self.assertEqual(
            health.probe_authorization("Bearer " + PROBE_TOKEN, PROBE_TOKEN), health.STATUS_OK
        )

    def test_an_unconfigured_token_refuses_everyone(self):
        for header in (None, "Bearer ", "Bearer anything"):
            self.assertEqual(health.probe_authorization(header, ""), health.STATUS_UNAVAILABLE)


class ProbeBoundTests(unittest.TestCase):
    """The probe is bounded in concurrency and in time."""

    def test_a_third_concurrent_probe_is_refused_rather_than_queued(self):
        probe = health.ReadinessProbe(budget_ms=1000, concurrency=2)
        statuses = []

        def reenter():
            status, _ = probe.evaluate(check_providers(runtime=reenter))
            statuses.append(status)
            return "ok"

        status, _ = probe.evaluate(check_providers(runtime=reenter))
        self.assertEqual(status, health.STATUS_OK)
        # Innermost first: the third nested evaluation found two already in flight.
        self.assertEqual(statuses, [health.STATUS_TOO_MANY, health.STATUS_OK])
        self.assertEqual(probe.active, 0)

    def test_an_over_budget_evaluation_is_reported_not_ready(self):
        probe = health.ReadinessProbe(budget_ms=1)

        def slow():
            time.sleep(0.05)
            return "ok"

        status, body = probe.evaluate(check_providers(configuration=slow))
        self.assertEqual(status, health.STATUS_UNAVAILABLE)
        self.assertEqual(body["status"], health.NOT_READY)
        self.assertEqual(set(body["checks"].values()), {health.FAILED})

    def test_the_counter_is_released_even_when_a_check_explodes(self):
        probe = health.ReadinessProbe(budget_ms=1000)

        def broken():
            raise RuntimeError("fixture failure")

        probe.evaluate(check_providers(runtime=broken))
        self.assertEqual(probe.active, 0)


class HealthRouteTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """The two routes over a real server, with the real check providers wired in."""

    async def asyncSetUp(self):
        await self.observe()

    async def live(self, headers=None):
        async with self.client.get(self.harness.url + LIVE_PATH, headers=headers) as response:
            return response.status, await response.read(), dict(response.headers)

    async def ready(self, headers=None):
        async with self.client.get(self.harness.url + READY_PATH, headers=headers) as response:
            return response.status, await response.read(), dict(response.headers)

    async def test_liveness_is_public_and_answers_the_closed_body(self):
        for headers in (None, {"Authorization": "Bearer wrong"}, {"Authorization": ""}):
            status, body, response_headers = await self.live(headers)
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), LIVE_BODY)
            self.assertEqual(response_headers.get("Cache-Control"), "no-store")
            self.assertNotIn("checks", json.loads(body))

    async def test_liveness_says_nothing_about_a_dependency(self):
        status, body, _ = await self.live()
        self.assertEqual(status, 200)
        self.assertNotIn(b"platform", body)
        self.assertNotIn(b"model", body)
        self.assertNotIn(b"ledger", body)

    async def test_readiness_without_a_credential_is_unauthorized_and_closed(self):
        status, body, _ = await self.ready()
        self.assertEqual(status, 401)
        document = json.loads(body)
        self.assertEqual(set(document), READY_FIELDS)
        self.assertEqual(document["status"], health.NOT_READY)
        self.assertEqual(set(document["checks"].values()), {"not_verified"})

    async def test_a_business_token_cannot_open_readiness(self):
        from gateway_fixtures import SECRETS

        for token in ("TS041_TEST_CLIENT", "TS041_TEST_PLATFORM", "TS041_TEST_UPSTREAM"):
            status, _, _ = await self.ready({"Authorization": "Bearer " + SECRETS[token]})
            self.assertEqual(status, 401, token)

    async def test_an_unconfigured_probe_token_refuses_everyone(self):
        with patch.dict(os.environ, {"TS103_DIAGNOSTICS_TOKEN": ""}):
            status, body, _ = await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body)["status"], health.NOT_READY)

    async def test_an_authorized_probe_reports_the_eight_checks(self):
        status, body, _ = await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        document = json.loads(body)
        self.assertEqual(set(document), READY_FIELDS)
        self.assertEqual(document["service"], "gateway")
        self.assertEqual(set(document["checks"]), set(health.CHECK_NAMES))
        self.assertTrue(set(document["checks"].values()) <= set(health.CHECK_VALUES))
        self.assertEqual(status, health.status_code(document["status"]))

    async def test_the_required_checks_are_ok_and_native_is_not_configured(self):
        status, body, _ = await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        checks = json.loads(body)["checks"]
        for name in health.REQUIRED_CHECKS:
            self.assertEqual(checks[name], "ok", name)
        self.assertEqual(checks["native"], "not_configured")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], health.READY)

    async def test_a_remote_dependency_is_not_verified_before_a_real_observation(self):
        _, body, _ = await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        checks = json.loads(body)["checks"]
        self.assertEqual(checks["platform"], "not_verified")
        self.assertEqual(checks["model"], "not_verified")

    async def test_a_real_platform_and_model_success_makes_them_ok(self):
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.harness.body(),
            headers=self.harness.headers(),
        ) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        _, body, _ = await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        checks = json.loads(body)["checks"]
        self.assertEqual(checks["platform"], "ok")
        self.assertEqual(checks["model"], "ok")

    async def test_an_expired_observation_returns_to_not_verified(self):
        async with await self.client.post(
            self.harness.url + "/v1/chat/completions",
            json=self.harness.body(),
            headers=self.harness.headers(),
        ) as response:
            await response.read()
        observation = self.harness.observability
        self.assertEqual(observation.platform_observation.status(), "ok")
        self.assertEqual(observation.model_observation.status(), "ok")
        # The window is measured on the monotonic clock, so the fixture moves that clock
        # instead of sleeping: an old observation must not stay green forever.
        validity = observation.settings.observation_validity_seconds
        expired = time.monotonic() + validity + 1
        with (
            patch.object(observation.platform_observation, "clock", lambda: expired),
            patch.object(observation.model_observation, "clock", lambda: expired),
        ):
            self.assertEqual(observation.platform_observation.status(), "not_verified")
            self.assertEqual(observation.model_observation.status(), "not_verified")
            _, body, _ = await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        checks = json.loads(body)["checks"]
        self.assertEqual(checks["platform"], "not_verified")
        self.assertEqual(checks["model"], "not_verified")
        # Neither is required, so an expired remote observation does not make chat unavailable.
        self.assertEqual(json.loads(body)["status"], health.READY)

    async def test_the_body_leaks_no_path_environment_or_business_identifier(self):
        _, body, _ = await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        text = body.decode("utf-8")
        for forbidden in (
            str(self.harness.log_directory),
            str(self.harness.settings.diagnostics_path),
            self.harness.settings.platform_base_url,
            "companion",
            "provider-fixture",
            "TS041",
            PROBE_TOKEN,
            "request-",
            "turn-",
        ):
            self.assertNotIn(forbidden, text, forbidden)

    async def test_a_probe_is_not_a_published_business_route(self):
        status, _, _ = await self.live()
        self.assertEqual(status, 200)
        # The probe paths answer only GET; the published business routes are unchanged.
        async with self.client.post(self.harness.url + LIVE_PATH, data=b"x") as response:
            self.assertEqual(response.status, 405)
            await response.read()

    async def test_probing_readiness_writes_nothing_at_all(self):
        await self.harness.settle()
        before = self.harness.corpus()
        names_before = [record["event"] for record in self.harness.records()]
        for _ in range(5):
            await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
            await self.ready()
            await self.live()
        await self.harness.observability.shutdown()
        self.assertEqual(self.harness.corpus(), before)
        self.assertEqual([record["event"] for record in self.harness.records()], names_before)

    async def test_probing_readiness_leaves_the_sink_in_the_same_state(self):
        state = self.harness.observability.state
        for _ in range(5):
            await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        self.assertEqual(self.harness.observability.state, state)
        self.assertEqual(self.harness.observability.stats()["dropped"], 0)
        self.assertEqual(self.harness.observability.probe.active, 0)

    async def test_probing_readiness_creates_no_ledger_row(self):
        import sqlite3

        for _ in range(5):
            await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        connection = sqlite3.connect(self.harness.settings.diagnostics_path)
        self.addCleanup(connection.close)
        for table in ("requests", "turns", "request_metrics", "admission_metrics"):
            rows = connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
            self.assertEqual(rows, 0, table)

    async def test_probing_readiness_fetches_no_configuration(self):
        self.harness.services.config_calls.clear()
        for _ in range(5):
            await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        self.assertEqual(self.harness.services.config_calls, [])

    async def test_readiness_reports_a_degraded_log_as_not_ready(self):
        self.harness.observability.log._degrade(sink.REASON_IO)
        status, body, _ = await self.ready({"Authorization": "Bearer " + PROBE_TOKEN})
        document = json.loads(body)
        self.assertEqual(status, 503)
        self.assertEqual(document["checks"]["logging"], "failed")
        self.assertEqual(document["status"], health.NOT_READY)
        # The probe reported the state; it did not change it and did not repair anything.
        self.assertEqual(self.harness.observability.logging_status(), "failed")

    async def test_liveness_stays_alive_while_readiness_is_not_ready(self):
        self.harness.observability.log._degrade(sink.REASON_IO)
        self.assertEqual((await self.live())[0], 200)
        self.assertEqual((await self.ready({"Authorization": "Bearer " + PROBE_TOKEN}))[0], 503)


class ProbeBudgetRouteTests(ObservedTestCase, unittest.IsolatedAsyncioTestCase):
    """A deployment that configures a very small probe budget still answers honestly."""

    async def asyncSetUp(self):
        await self.observe(observation={"probe_budget_ms": 1})

    async def test_an_over_budget_probe_is_reported_not_ready(self):
        def slow():
            time.sleep(0.05)
            return "ok"

        self.harness.gateway.check_providers = lambda: check_providers(configuration=slow)
        async with self.client.get(
            self.harness.url + READY_PATH,
            headers={"Authorization": "Bearer " + PROBE_TOKEN},
        ) as response:
            document = await response.json()
        self.assertEqual(response.status, 503)
        self.assertEqual(document["status"], health.NOT_READY)
        # An unmeasured answer is never presented as a measured one: every check degrades.
        self.assertEqual(set(document["checks"].values()), {health.FAILED})

    async def test_liveness_is_unaffected_by_the_probe_budget(self):
        async with self.client.get(self.harness.url + LIVE_PATH) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(await response.json(), LIVE_BODY)


if __name__ == "__main__":
    unittest.main()
