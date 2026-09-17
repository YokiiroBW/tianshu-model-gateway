"""TS-044 bounded admission scheduling: local policy, scheduler core and real HTTP.

The scheduler is exercised through its own narrow port first (an injected clock, a fake
diagnostics port, no HTTP and no model configuration), then through the real gateway with
loopback fixtures for the properties only a recording upstream can show: interactive work
enters while background work is in flight, background work is not starved, the queue length
is a hard bound, and a timed-out, cancelled, revoked or duplicated request never produces an
extra upstream attempt.
"""

import asyncio
import json
import logging
import os
import socket
import sqlite3
import sys
import tempfile
import unittest
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web
from gateway_fixtures import (
    CONTRACT,
    DOCUMENTS,
    NATIVE_CONTRACT,
    NATIVE_DOCUMENTS,
    SECRETS,
    RecordingServices,
    registration,
    start_http,
)
from tianshu_gateway.config import Classification, ClientGrant, SchedulingPolicy, utcnow
from tianshu_gateway.contracts import NATIVE_ROUTE_PATH
from tianshu_gateway.native import NativeGrant
from tianshu_gateway.scheduler import (
    ADMITTED,
    BACKGROUND,
    CANCELLED,
    DEADLINE_EXCEEDED,
    DUPLICATE,
    INTERACTIVE,
    QUEUE_FULL,
    BoundedAdmissionScheduler,
    DuplicateRequest,
    QueueFull,
)
from tianshu_gateway.server import GATEWAY, Settings, create_app, load_settings

NATIVE_PATH = NATIVE_ROUTE_PATH
NAMESPACE = "credential-namespace-fixture"
TOKENS = {"companion": "CLIENT", "memory-index": "OTHER", "external": "EXTERNAL"}


def policy(**overrides):
    fields = {
        "max_in_flight": 3,
        "max_provider_in_flight": 3,
        "interactive_reserve": 1,
        "max_queue_length": 4,
        "wait_timeout_ms": 1000,
    }
    fields.update(overrides)
    return SchedulingPolicy(**fields)


def classification(**overrides):
    fields = {"bindings": {"memory-index": "background"}, "default_class": "interactive"}
    fields.update(overrides)
    return Classification(**fields)


def deployment_document(**overrides):
    """A minimal deployment JSON; no secret value, address or key is a real one."""
    document = {
        "contract_directory": str(CONTRACT),
        "diagnostics_path": ":memory:",
        "platform_base_url": "https://platform.example.invalid",
        "platform_credential_ref": "secret-ref:gateway/platform",
        "platform_origin_env": "TIANSHU_PLATFORM_ORIGIN_REF",
        "secret_references": {},
        "targets": [{"base_url": "https://platform.example.invalid", "addresses": ["192.0.2.10"]}],
        "clients": [
            {
                "service": "companion",
                "credential_ref": "secret-ref:gateway/companion",
                "provider_id": "provider-a",
                "config_version": 7,
                "internal": True,
                "allowed_versions": [],
            }
        ],
    }
    document.update(overrides)
    return document


class FakeClock:
    """A replaceable monotonic clock: no test depends on shared global time state."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, milliseconds):
        self.now += milliseconds / 1000


class FakeDiagnostics:
    """The narrow observation port the scheduler is allowed to write."""

    def __init__(self, fail=False):
        self.rows = []
        self.fail = fail

    def record_admission(self, service, request_id, workload_class, outcome, wait_ms):
        if self.fail:
            raise sqlite3.OperationalError("fixture failure " + SECRETS["TS041_TEST_UPSTREAM"])
        self.rows.append((service, request_id, workload_class, outcome, wait_ms))


class PolicyTests(unittest.TestCase):
    """The new deployment configuration: validated locally, never a shared contract."""

    def test_scheduling_policy_validation_bounds(self):
        policy().validate()
        for overrides in (
            {"max_in_flight": 0},
            {"max_provider_in_flight": 0},
            {"max_queue_length": 0},
            {"wait_timeout_ms": 0},
            {"interactive_reserve": -1},
            {"interactive_reserve": 3},
            {"max_in_flight": True},
            {"max_in_flight": 2.5},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    policy(**overrides).validate()

    def test_existing_deployment_document_keeps_the_previous_behaviour(self):
        # A document written before this slice carries neither new field: it must keep the
        # exact old semantics, which the zero reserve selects.
        settings = load_settings(json.dumps(deployment_document()))
        self.assertIsNone(settings.scheduling)
        self.assertEqual(settings.policy.interactive_reserve, 0)
        self.assertEqual(settings.policy.max_in_flight, settings.max_concurrent)
        self.assertEqual(settings.policy.max_provider_in_flight, settings.max_provider_concurrent)
        self.assertEqual(settings.classification.default_class, INTERACTIVE)
        settings.validate()

    def test_scheduling_fields_survive_the_deployment_round_trip(self):
        document = deployment_document(
            scheduling=asdict(policy(max_in_flight=5, max_provider_in_flight=2)),
            workload_bindings={
                "bindings": {"memory-index": "background"},
                "default_class": "interactive",
            },
        )
        settings = load_settings(json.dumps(document))
        self.assertEqual(settings.policy, policy(max_in_flight=5, max_provider_in_flight=2))
        self.assertEqual(settings.classification.for_service("memory-index"), BACKGROUND)
        settings.validate()

    def test_unknown_or_unsatisfiable_workload_configuration_is_rejected(self):
        for overrides in (
            {"default_class": "priority"},
            {"bindings": {"memory-index": "highest"}},
            {"bindings": {"": "background"}},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    classification(**overrides).validate()
        # A reserve at or above a limit would leave background work nothing to run in.
        for scheduling in (
            policy(interactive_reserve=4, max_in_flight=4),
            policy(max_in_flight=2, max_provider_in_flight=4),
            policy(max_in_flight=2, max_provider_in_flight=2, interactive_reserve=2),
        ):
            with self.subTest(scheduling=scheduling):
                with self.assertRaises(ValueError):
                    load_settings(
                        json.dumps(deployment_document(scheduling=asdict(scheduling)))
                    ).validate()

    def test_class_comes_only_from_the_deployment_binding(self):
        mapping = classification(
            bindings={"memory-index": "background", "companion": "interactive"}
        )
        self.assertEqual(mapping.for_service("memory-index"), BACKGROUND)
        self.assertEqual(mapping.for_service("companion"), INTERACTIVE)
        # An unbound caller uses the deployment default and cannot choose otherwise, even
        # when it declares a different workload.
        self.assertEqual(mapping.for_service("unknown-client"), INTERACTIVE)
        self.assertEqual(mapping.for_service("unknown-client", "background"), INTERACTIVE)
        self.assertEqual(mapping.for_service("memory-index", "interactive"), BACKGROUND)


class SchedulerCoreTests(unittest.IsolatedAsyncioTestCase):
    """Component level: capacity, reserve, fairness, deadlines, cleanup, observation."""

    def build(self, **overrides):
        self.clock = FakeClock()
        self.observed = FakeDiagnostics()
        self.scheduler = BoundedAdmissionScheduler(
            policy(**overrides), self.observed, clock=self.clock
        )
        return self.scheduler

    async def permit(self, service, workload_class, key, provider="provider-a", timeout_ms=1000):
        """Acquire one permit through the scheduler's own port (no HTTP, no configuration)."""
        return await self.scheduler.acquire(
            service, workload_class, None, key, provider, timeout_ms
        )

    def acquiring(self, service, workload_class, key, provider="provider-a", *, timeout_ms=1000):
        """Start an acquisition without awaiting it, for queue and deadline cases."""
        return asyncio.create_task(self.permit(service, workload_class, key, provider, timeout_ms))

    async def waiting_for(self, *keys):
        """Wait until exactly these launch keys are queued, instead of guessing at timings."""

        async def poll():
            while self.scheduler.waiting_keys() != set(keys):
                await asyncio.sleep(0)

        await asyncio.wait_for(poll(), 2)

    async def wait_idle(self):
        async def poll():
            # Yielding first lets every released holder observe its release before the
            # ledger is inspected, so this is a real drain rather than a lucky timing.
            await asyncio.sleep(0)
            while self.scheduler.inflight or self.scheduler.waiting:
                await asyncio.sleep(0)

        await asyncio.wait_for(poll(), 2)
        self.assertEqual(self.scheduler.provider_active, {})
        self.assertEqual(self.scheduler.interactive_claimed, 0)

    async def test_interactive_reserve_is_not_background_capacity(self):
        # One reserved slot out of three: background may only reach two, interactive three.
        self.build(max_in_flight=3, max_provider_in_flight=3)
        self.assertEqual(self.scheduler.background_capacity(), 2)
        held = [
            await self.permit("companion", INTERACTIVE, f"i-{n}", "provider-a") for n in range(3)
        ]
        self.assertEqual(self.scheduler.interactive_claimed, 3)
        # The pool holds one interactive slot and one background slot, and the reserved slot
        # accepts interactive work only, so this background request waits for a release.
        late = self.acquiring("memory-index", BACKGROUND, "b-late", timeout_ms=20)
        with self.assertRaises(TimeoutError):
            await late
        # An expired deadline leaves no trace: no queue entry and no capacity held.
        await self.waiting_for()
        self.assertEqual(
            (self.scheduler.inflight, self.scheduler.provider_active), (3, {"provider-a": 3})
        )
        # The reserved slot is exactly what a queued interactive request can still be served
        # with, so being full for background never refuses interactive work.
        interactive = self.acquiring("companion", INTERACTIVE, "i-3")
        await self.waiting_for("i-3")
        for waiter in held[:2]:
            await self.scheduler.release(waiter)
        admitted = await asyncio.wait_for(interactive, 1)
        self.assertTrue(admitted.live())
        self.assertEqual(self.scheduler.interactive_claimed, 2)
        for waiter in (admitted, held[2]):
            await self.scheduler.release(waiter)
        await self.wait_idle()
        # With the pool empty again, background work is admitted as usual.
        background = await self.permit("memory-index", BACKGROUND, "b-1")
        self.assertTrue(background.live())
        await self.scheduler.release(background)
        self.assertEqual(self.scheduler.provider_active, {})

    async def test_provider_limit_is_independent_of_the_global_limit(self):
        self.build(max_in_flight=4, max_provider_in_flight=1)
        first = await self.permit("companion", INTERACTIVE, "i-1")
        with self.assertRaises(TimeoutError):
            # The global slot is free but this provider is at its own hard limit, and the
            # caller's deadline expires while it waits for that provider.
            await self.scheduler.acquire("companion", INTERACTIVE, None, "i-2", "provider-a", 20)
        self.assertEqual(self.scheduler.inflight, 1)
        self.assertEqual(self.scheduler.provider_active, {"provider-a": 1})
        await self.scheduler.release(first)
        self.assertEqual(self.scheduler.inflight, 0)
        other = await self.permit("companion", INTERACTIVE, "i-3", "provider-b")
        self.assertTrue(other.live())
        await self.scheduler.release(other)

    async def test_background_is_not_starved_and_queues_do_not_jump(self):
        self.build(max_in_flight=3, max_provider_in_flight=3)
        held = [
            await self.permit("companion", INTERACTIVE, f"i-{n}", "provider-a") for n in range(3)
        ]
        self.assertEqual(self.scheduler.interactive_claimed, 3)
        # A full pool keeps its last slot for interactive work, so the three interactive
        # requests hold every slot of this provider and background work has to queue.
        queued = self.acquiring("memory-index", BACKGROUND, "b-2")
        await self.waiting_for("b-2")
        # The first release is not enough for background work: the freed slot is the reserved
        # one, and the interactive request that has been waiting longest takes it.
        interactive = self.acquiring("companion", INTERACTIVE, "i-3")
        await self.waiting_for("b-2", "i-3")
        await self.scheduler.release(held[0])
        admitted = await asyncio.wait_for(interactive, 1)
        self.assertEqual(admitted.workload_class, INTERACTIVE)
        self.assertTrue(admitted.live())
        self.assertFalse(queued.done())
        # Once enough interactive requests have left, the queued background request is
        # admitted: the queue is bounded, and it is never a starvation trap.
        for waiter in held[1:]:
            await self.scheduler.release(waiter)
        first_background = await asyncio.wait_for(queued, 1)
        self.assertEqual(first_background.workload_class, BACKGROUND)
        self.assertTrue(first_background.live())
        self.assertEqual(self.scheduler.background_capacity(), 2)
        # A second background request still runs beside the interactive ones, up to that
        # background capacity, and a third one has to wait again rather than exceed it.
        beside = self.acquiring("memory-index", BACKGROUND, "b-3")
        second_background = await asyncio.wait_for(beside, 1)
        self.assertTrue(second_background.live())
        refused = self.acquiring("memory-index", BACKGROUND, "b-4", timeout_ms=20)
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(refused, 2)
        # The pool is full with one interactive and two background requests, the background
        # side exactly at its own capacity, and the refused request left nothing behind.
        self.assertEqual(self.scheduler.interactive_claimed, 1)
        self.assertEqual(self.scheduler.background_claimed, 2)
        self.assertEqual(self.scheduler.waiting_keys(), set())
        self.assertEqual(self.scheduler.inflight, 3)
        for waiter in (admitted, first_background, second_background):
            await self.scheduler.release(waiter)
        await self.wait_idle()

    async def test_queue_length_is_a_hard_bound(self):
        self.build(max_in_flight=3, max_provider_in_flight=3, max_queue_length=1)
        self.assertEqual(self.scheduler.background_capacity(), 2)
        held = [
            await self.permit("companion", INTERACTIVE, "i-1", "provider-a"),
            await self.permit("memory-index", BACKGROUND, "b-0", "provider-a"),
            await self.permit("memory-index", BACKGROUND, "b-1", "provider-a"),
        ]
        self.assertEqual(self.scheduler.background_claimed, 2)
        # The next request is the only one allowed to wait; the one after it is refused
        # immediately instead of growing the queue beyond its configured bound.
        waiting = self.acquiring("memory-index", BACKGROUND, "b-2")
        await self.waiting_for("b-2")
        with self.assertRaises(QueueFull):
            await self.scheduler.acquire(
                "memory-index", BACKGROUND, None, "b-3", "provider-a", 1000
            )
        # The same launch key may not be queued twice either.
        with self.assertRaises(DuplicateRequest):
            await self.scheduler.acquire(
                "memory-index", BACKGROUND, None, "b-2", "provider-a", 1000
            )
        self.assertEqual(self.scheduler.waiting, 1)
        await self.scheduler.release(held[1])
        admitted = await asyncio.wait_for(waiting, 1)
        self.assertTrue(admitted.live())
        for waiter in (admitted, held[0], held[2]):
            await self.scheduler.release(waiter)
        await self.wait_idle()

    async def test_wait_duration_comes_from_the_injected_clock(self):
        self.build(max_in_flight=3, max_provider_in_flight=3)
        first = await self.permit("companion", INTERACTIVE, "i-1", "provider-a")
        held = [
            await self.permit("memory-index", BACKGROUND, "b-0", "provider-a"),
            await self.permit("memory-index", BACKGROUND, "b-1", "provider-a"),
        ]
        waiting = self.acquiring("memory-index", BACKGROUND, "b-2")
        await self.waiting_for("b-2")
        # The clock is the only source of waiting time, so a test can pin the value.
        self.clock.advance(37)
        await self.scheduler.release(held[0])
        admitted = await asyncio.wait_for(waiting, 1)
        self.assertEqual(admitted.wait_ms, 37)
        self.assertEqual(self.scheduler.background_claimed, 2)
        for waiter in (admitted, first, held[1]):
            await self.scheduler.release(waiter)
        rows = {row[1]: row for row in self.observed.rows}
        self.assertEqual(
            {key: row[3] for key, row in rows.items()},
            {"i-1": ADMITTED, "b-0": ADMITTED, "b-1": ADMITTED, "b-2": ADMITTED},
        )
        self.assertEqual(rows["b-2"][4], 37)

    async def test_deadline_expires_while_the_slot_is_busy(self):
        self.build(max_in_flight=3, max_provider_in_flight=3)
        first = await self.permit("companion", INTERACTIVE, "i-1", "provider-a")
        held = [await self.permit("memory-index", BACKGROUND, f"b-{n}") for n in range(2)]
        waiting = self.acquiring("memory-index", BACKGROUND, "b-2", timeout_ms=20)
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(waiting, 2)
        self.assertEqual(self.scheduler.waiting, 0)
        rows = {row[1]: row for row in self.observed.rows}
        self.assertEqual(rows["b-2"][3], DEADLINE_EXCEEDED)
        self.assertIsNone(rows["b-2"][4])
        for waiter in (first, *held):
            await self.scheduler.release(waiter)
        await self.wait_idle()

    async def test_cancelled_waiter_leaves_nothing_behind(self):
        self.build(max_in_flight=3, max_provider_in_flight=3)
        first = await self.permit("companion", INTERACTIVE, "i-1", "provider-a")
        held = [await self.permit("memory-index", BACKGROUND, f"b-{n}") for n in range(2)]
        cancelled = self.acquiring("memory-index", BACKGROUND, "b-2")
        await self.waiting_for("b-2")
        cancelled.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled
        self.assertEqual(self.scheduler.waiting, 0)
        self.assertEqual(self.scheduler.waiting_keys(), set())
        await self.scheduler.release(held[0])
        again = await self.permit("memory-index", BACKGROUND, "b-3", "provider-a")
        self.assertTrue(again.live())
        for waiter in (again, first, held[1]):
            await self.scheduler.release(waiter)
        await self.wait_idle()

    async def test_release_is_idempotent_and_the_ledger_returns_to_zero(self):
        self.build()
        waiter = await self.permit("companion", INTERACTIVE, "i-1", "provider-a")
        self.assertEqual(self.scheduler.provider_active, {"provider-a": 1})
        await self.scheduler.release(waiter)
        await self.scheduler.release(waiter)
        self.assertEqual(self.scheduler.inflight, 0)
        self.assertEqual(self.scheduler.provider_active, {})
        self.assertEqual(self.scheduler.interactive_claimed, 0)

    async def test_observation_failure_never_changes_admission(self):
        self.build()
        self.observed.fail = True
        records = []
        logger = logging.getLogger("tianshu_gateway")

        class Grab(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = Grab()
        logger.addHandler(handler)
        # Diagnostics is a side channel: its failure removes the private row, not the permit.
        try:
            waiter = await self.permit("companion", INTERACTIVE, "i-1", "provider-a")
            self.assertTrue(waiter.live())
            self.assertEqual(self.scheduler.inflight, 1)
            await self.scheduler.release(waiter)
        finally:
            logger.removeHandler(handler)
        self.assertEqual(self.scheduler.inflight, 0)
        self.assertEqual([record.levelname for record in records], ["WARNING"])
        # Only a fixed local fact is logged: never the credential the fixture uses.
        logged = " ".join(record.getMessage() for record in records)
        self.assertEqual(logged, "admission_metric_degraded outcome=admitted")
        self.assertNotIn(SECRETS["TS041_TEST_UPSTREAM"], logged)

    async def test_refusals_are_recorded_as_bounded_private_facts(self):
        self.build(max_in_flight=3, max_provider_in_flight=3, max_queue_length=1)
        first = await self.permit("companion", INTERACTIVE, "i-1", "provider-a")
        held = [await self.permit("memory-index", BACKGROUND, f"b-{n}") for n in range(2)]
        waiting = self.acquiring("memory-index", BACKGROUND, "b-2")
        await self.waiting_for("b-2")
        with self.assertRaises(QueueFull):
            await self.scheduler.acquire(
                "memory-index", BACKGROUND, None, "b-3", "provider-a", 1000
            )
        # The same launch key may not queue twice; the second attempt is refused, not merged.
        with self.assertRaises(DuplicateRequest):
            await self.scheduler.acquire(
                "memory-index", BACKGROUND, None, "b-2", "provider-a", 1000
            )
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        for waiter in (first, *held):
            await self.scheduler.release(waiter)
        rows = {row[1]: row for row in self.observed.rows}
        # A refusal carries its outcome and no wait; an admitted request carries both.
        self.assertEqual((rows["b-3"][3], rows["b-3"][4]), (QUEUE_FULL, None))
        self.assertEqual((rows["b-2"][3], rows["b-2"][4]), (CANCELLED, None))
        self.assertEqual((rows["i-1"][3], type(rows["i-1"][4])), (ADMITTED, int))
        self.assertEqual((rows["i-1"][0], rows["i-1"][2]), ("companion", INTERACTIVE))
        recorded = [row[3] for row in self.observed.rows if row[1] == "b-2"]
        self.assertEqual(recorded, [DUPLICATE, CANCELLED])
        await self.wait_idle()


class LegacySemanticsTests(unittest.IsolatedAsyncioTestCase):
    """A deployment whose reserve is zero keeps the pre-slice behaviour exactly."""

    async def asyncSetUp(self):
        self.environment = patch.dict(os.environ, SECRETS)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
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
        references = {
            "secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM",
            "secret-ref:fixture/client": "TS041_TEST_CLIENT",
            "secret-ref:fixture/platform": "TS041_TEST_PLATFORM",
        }
        self.settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "legacy.sqlite"),
            self.platform_url,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            references,
            [registration(self.platform_url), registration(self.upstream_url + "/v1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                )
            ],
            max_concurrent=1,
            scheduling=policy(interactive_reserve=0, max_in_flight=1, max_provider_in_flight=1),
        )
        self.settings.validate()
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.gateway = self.app[GATEWAY]
        self.client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10), trust_env=False
        )
        self.addAsyncCleanup(self.client.close)
        self.body = DOCUMENTS["native_request"]

    def headers(self, request_id):
        return {
            "Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"],
            "X-Request-ID": request_id,
            "X-Tianshu-Turn-ID": "turn-" + request_id,
            "X-Tianshu-Config-Version": "7",
            "X-Tianshu-Workload": "companion.text",
        }

    async def test_no_queue_no_admission_row_and_the_same_refusal(self):
        self.assertIsNone(self.gateway.scheduler)
        self.services.mode = "hold"
        first = asyncio.create_task(
            self.client.post(
                self.url + "/v1/chat/completions", json=self.body, headers=self.headers("legacy-1")
            )
        )
        await self.services.wait_calls(1)
        async with self.client.post(
            self.url + "/v1/chat/completions", json=self.body, headers=self.headers("legacy-2")
        ) as refused:
            self.assertEqual(refused.status, 429)
            self.assertEqual((await refused.json())["code"], "queue_full")
        self.assertEqual(self.gateway.active, 1)
        self.assertEqual(self.services.call_count(), 1)
        # The old path records no private admission fact at all.
        self.assertIsNone(self.gateway.diagnostics.admission("companion", "legacy-2"))
        self.services.release_hold()
        async with await first as finished:
            self.assertEqual(finished.status, 200)
        self.assertTrue(self.gateway.idle)
        self.assertEqual(self.gateway.busy_providers, {})


class ScheduledFixtures:
    """Deployment fixtures and helpers shared by the adapter-level cases.

    Only the policy differs between the concrete cases at the end of this module, so the
    fixture lives here and each case fixes one policy. The scenarios live in their own mixin,
    so a case that needs a smaller queue does not silently re-run every scenario against it.
    """

    global_limit = 3
    provider_limit = 3
    reserve = 1
    queue_length = 4
    wait_ms = 1000

    async def asyncSetUp(self):
        self.environment = patch.dict(os.environ, SECRETS)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.services = RecordingServices()
        upstream_app = web.Application()
        upstream_app.router.add_post("/v1/chat/completions", self.services.upstream)
        upstream_app.router.add_post(NATIVE_PATH, self.services.native_upstream)
        runner, self.upstream_url = await start_http(upstream_app)
        self.addAsyncCleanup(runner.cleanup)
        self.services.configure(self.upstream_url)
        platform_app = web.Application()
        platform_app.router.add_post("/internal/v1/model-config/snapshot", self.services.snapshot)
        platform_app.router.add_post(
            "/internal/v1/model-config/native/snapshot", self.services.native_snapshot
        )
        runner, self.platform_url = await start_http(platform_app)
        self.addAsyncCleanup(runner.cleanup)
        references = {"secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM"}
        for name in ("CLIENT", "OTHER", "EXTERNAL", "PLATFORM"):
            references["secret-ref:fixture/" + name.lower()] = "TS041_TEST_" + name
        references["secret-ref:fixture/native"] = "TS042_TEST_NATIVE"
        self.settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "scheduled.sqlite"),
            self.platform_url,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            references,
            [registration(self.platform_url), registration(self.upstream_url + "/v1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                ),
                ClientGrant(
                    "memory-index", "secret-ref:fixture/other", "provider-fixture", 7, True, (8,)
                ),
                ClientGrant("external", "secret-ref:fixture/external", "provider-fixture", 7),
            ],
            scheduling=policy(
                max_in_flight=self.global_limit,
                max_provider_in_flight=self.provider_limit,
                interactive_reserve=self.reserve,
                max_queue_length=self.queue_length,
                wait_timeout_ms=self.wait_ms,
            ),
            workload_bindings=classification(),
            native_enabled=True,
            native_contract_directory=str(NATIVE_CONTRACT),
            native_clients=[
                NativeGrant(
                    service="native-caller",
                    credential_ref="secret-ref:fixture/native",
                    principal_id="principal-fixture",
                    credential_namespace=NAMESPACE,
                    provider_ids=("provider-fixture",),
                    native_config_versions=(7,),
                    native_config_version=7,
                    internal=True,
                    expires_at=(utcnow() + timedelta(minutes=10))
                    .isoformat()
                    .replace("+00:00", "Z"),
                )
            ],
        )
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.gateway = self.app[GATEWAY]
        self.scheduler = self.gateway.scheduler
        self.client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15), trust_env=False
        )
        self.addAsyncCleanup(self.client.close)
        self.sequence = 0
        self.body = DOCUMENTS["native_request"]

    def chat_headers(self, service="companion", request_id=None, turn=None, workload=None):
        self.sequence += 1
        headers = {"Authorization": "Bearer " + SECRETS["TS041_TEST_" + TOKENS[service]]}
        if service == "external":
            return headers
        return {
            **headers,
            "X-Request-ID": request_id or f"request-{self.sequence}",
            "X-Tianshu-Turn-ID": turn or f"turn-{self.sequence}",
            "X-Tianshu-Config-Version": "7",
            "X-Tianshu-Workload": workload or "companion.text",
        }

    def native_headers(self, request_id=None, turn=None, version=7):
        self.sequence += 1
        return {
            "Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"],
            "X-Request-ID": request_id or f"native-{self.sequence}",
            "X-Tianshu-Turn-ID": turn or f"native-turn-{self.sequence}",
            "X-Tianshu-Native-Config-Version": str(version),
        }

    async def chat(self, headers):
        return await self.client.post(
            self.url + "/v1/chat/completions", json=self.body, headers=headers
        )

    async def wait_for(self, predicate, timeout=5):
        async def poll():
            while not predicate():
                await asyncio.sleep(0.005)

        await asyncio.wait_for(poll(), timeout)

    async def release_held(self, tasks=()):
        """Drain the pool: release parked attempts until every admitted request is done.

        Admitting a queued request forwards it, which parks a *new* attempt at the fixture, so
        a single release is not enough to drain a queue. This releases until the gateway is
        idle and every client task has finished, so no caller is still in flight when a test
        asserts a count or reads a receipt.
        """
        tasks = list(tasks)

        async def drain():
            while not self.gateway.idle or any(not task.done() for task in tasks):
                self.services.release_hold()
                self.services.release_hold(native=True)
                await asyncio.sleep(0.005)
            self.services.release_hold()
            self.services.release_hold(native=True)

        await asyncio.wait_for(drain(), 10)


class ScheduledScenarios:
    """The properties only a real gateway with a recording upstream can show."""

    async def test_interactive_capacity_is_reserved_while_background_holds_the_pool(self):
        self.services.mode = "hold"
        background = [
            asyncio.create_task(self.chat(self.chat_headers("memory-index"))) for _ in range(2)
        ]
        await self.services.wait_calls(2)
        queued = asyncio.create_task(
            self.chat(self.chat_headers("memory-index", request_id="queued-background"))
        )
        await self.wait_for(lambda: self.scheduler.waiting == 1)
        # The pool is full, but the reserved slot is still reachable by interactive work, so
        # this request is admitted and forwarded while the background orders keep waiting.
        interactive = asyncio.create_task(
            self.chat(self.chat_headers("companion", request_id="live-interactive"))
        )
        await self.services.wait_calls(3)
        self.assertEqual(self.scheduler.interactive_claimed, 1)
        self.assertFalse(interactive.done())
        self.assertFalse(queued.done())
        await self.release_held([interactive, *background, queued])
        async with await interactive as response:
            self.assertEqual(response.status, 200)
        for task in [*background, queued]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        # Four admissions, four attempts: the queue itself never adds an attempt, and the
        # interactive request did not wait for the pooled background orders to finish.
        self.assertEqual(self.services.call_count(), 4)
        self.assertEqual(self.scheduler.inflight, 0)
        self.assertEqual(self.scheduler.waiting, 0)

    async def test_wait_deadline_expires_without_an_extra_upstream_attempt(self):
        self.services.mode = "hold"
        held = asyncio.create_task(self.chat(self.chat_headers("memory-index")))
        await self.services.wait_calls(1)
        blockers = [
            asyncio.create_task(self.chat(self.chat_headers("companion")))
            for _ in range(self.global_limit - 1)
        ]
        await self.wait_for(lambda: self.scheduler.inflight == self.global_limit)
        async with await self.chat(
            self.chat_headers("memory-index", request_id="deadline")
        ) as expired:
            self.assertEqual(expired.status, 408)
            self.assertEqual((await expired.json())["code"], "timeout")
        # The expired order never reached the upstream and left no waiter behind.
        self.assertEqual(self.services.call_count(), self.global_limit)
        self.assertEqual(self.scheduler.waiting, 0)
        self.gateway.diagnostics.revoke(0)  # unrelated Chat version: no effect here
        await self.release_held([held, *blockers])
        for task in [held, *blockers]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        self.assertEqual(self.services.call_count(), self.global_limit)
        await self.wait_for(lambda: self.gateway.idle)

    async def test_the_configured_wait_deadline_is_the_one_that_expires(self):
        self.services.mode = "hold"
        # A short deadline has to be visibly short: the queued request expires while the pool
        # is still genuinely busy, so a mere binding timeout would look nothing like this (it
        # would be reported only after the upstream attempt was made and parked).
        held = [asyncio.create_task(self.chat(self.chat_headers("memory-index"))) for _ in range(2)]
        await self.services.wait_calls(2)
        blockers = [
            asyncio.create_task(self.chat(self.chat_headers("companion")))
            for _ in range(self.global_limit - 2)
        ]
        await self.wait_for(lambda: self.scheduler.inflight == self.global_limit)
        queued = asyncio.create_task(
            self.chat(self.chat_headers("memory-index", request_id="short-deadline"))
        )
        await self.wait_for(lambda: self.scheduler.waiting == 1)
        loop = asyncio.get_running_loop()
        started = loop.time()
        async with await queued as refused:
            elapsed = loop.time() - started
            self.assertEqual(refused.status, 408)
            self.assertEqual((await refused.json())["code"], "timeout")
        self.assertLess(elapsed, self.wait_ms / 1000 + 1.0)
        # The refusal produced no attempt of its own, and the pool is still fully busy.
        self.assertEqual(self.services.call_count(), self.global_limit)
        self.assertEqual(self.scheduler.waiting, 0)
        self.assertEqual(self.scheduler.inflight, self.global_limit)
        await self.release_held([*held, *blockers])
        for task in [*held, *blockers]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        self.assertEqual(self.services.call_count(), self.global_limit)

    async def test_client_cancel_while_queued_is_removed_and_not_forwarded(self):
        self.services.mode = "hold"
        held = [asyncio.create_task(self.chat(self.chat_headers("memory-index"))) for _ in range(2)]
        await self.services.wait_calls(2)
        blockers = [
            asyncio.create_task(self.chat(self.chat_headers("companion")))
            for _ in range(self.global_limit - 2)
        ]
        await self.wait_for(lambda: self.scheduler.inflight == self.global_limit)
        queued = asyncio.create_task(
            self.chat(self.chat_headers("memory-index", request_id="cancelled-while-queued"))
        )
        await self.wait_for(lambda: self.scheduler.waiting == 1)
        queued.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await queued
        await self.wait_for(lambda: self.scheduler.waiting == 0)
        await self.release_held([*held, *blockers])
        for task in [*held, *blockers]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        self.assertEqual(self.services.call_count(), self.global_limit)
        await self.wait_for(lambda: self.gateway.idle)

    async def test_revocation_during_the_wait_prevents_the_send(self):
        self.services.mode = "hold"
        held = [asyncio.create_task(self.chat(self.chat_headers("memory-index"))) for _ in range(2)]
        await self.services.wait_calls(2)
        blockers = [
            asyncio.create_task(self.chat(self.chat_headers("companion")))
            for _ in range(self.global_limit - 2)
        ]
        await self.wait_for(lambda: self.scheduler.inflight == self.global_limit)
        queued = asyncio.create_task(
            self.chat(self.chat_headers("memory-index", request_id="revoked-while-queued"))
        )
        await self.wait_for(lambda: self.scheduler.waiting == 1)
        self.gateway.cache.revoke(7)
        await self.release_held([*held, *blockers])
        for task in [*held, *blockers]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        async with await queued as refused:
            self.assertEqual(refused.status, 403)
        # The pinned version is re-read only after the wait, and the revocation stops the send.
        self.assertEqual(self.services.call_count(), self.global_limit)
        await self.wait_for(lambda: self.gateway.idle)

    async def test_duplicate_request_id_never_holds_two_permits(self):
        self.services.mode = "hold"
        held = [asyncio.create_task(self.chat(self.chat_headers("memory-index"))) for _ in range(2)]
        await self.services.wait_calls(2)
        blockers = [
            asyncio.create_task(self.chat(self.chat_headers("companion")))
            for _ in range(self.global_limit - 2)
        ]
        await self.wait_for(lambda: self.scheduler.inflight == self.global_limit)
        first = asyncio.create_task(
            self.chat(self.chat_headers("memory-index", request_id="same-id", turn="same-turn"))
        )
        await self.wait_for(lambda: self.scheduler.waiting == 1)
        duplicate = asyncio.create_task(
            self.chat(self.chat_headers("memory-index", request_id="same-id", turn="same-turn"))
        )
        async with await duplicate as refused:
            self.assertEqual(refused.status, 400)
        self.assertEqual(self.scheduler.waiting, 1)
        self.assertEqual(self.services.call_count(), self.global_limit)
        await self.release_held([*held, *blockers, first])
        for task in [*held, *blockers, first]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        # One logical call, one upstream attempt.
        self.assertEqual(self.services.call_count(), self.global_limit + 1)
        await self.wait_for(lambda: self.gateway.idle)

    async def test_chat_and_responses_share_one_pool_and_provider_quota(self):
        self.services.mode = "hold"
        self.services.native_mode = "hold"
        chat = asyncio.create_task(self.chat(self.chat_headers("companion")))
        await self.services.wait_calls(1)
        # The shared pool is visible from the native side too: the two protocols add up to one
        # load, so a second permit comes from the same global and provider accounting instead
        # of a protocol-local counter that could exceed the deployment's limits.
        native = asyncio.create_task(
            self.client.post(
                self.url + NATIVE_PATH,
                json=NATIVE_DOCUMENTS["native_request"],
                headers=self.native_headers(request_id="native-shared"),
            )
        )
        await self.wait_for(lambda: len(self.services.native_calls) == 1)
        self.assertEqual(self.scheduler.inflight, 2)
        self.assertEqual(self.scheduler.provider_active, {"provider-fixture": 2})
        self.assertEqual(self.scheduler.waiting, 0)
        await self.release_held([chat, native])
        async with await chat as finished:
            self.assertEqual(finished.status, 200)
        async with await native as finished:
            self.assertEqual(finished.status, 200)
        self.assertEqual(self.services.call_count(), 1)
        self.assertEqual(len(self.services.native_calls), 1)
        # Both permits are released by their own protocol path: nothing leaks into the pool.
        await self.wait_for(lambda: self.gateway.idle)
        self.assertEqual(self.scheduler.inflight, 0)
        self.assertEqual(self.scheduler.provider_active, {})

    async def test_external_client_uses_the_registered_default_class(self):
        self.services.mode = "hold"
        background = [
            asyncio.create_task(self.chat(self.chat_headers("memory-index"))) for _ in range(2)
        ]
        await self.services.wait_calls(2)
        # An external caller has no binding, so the deployment default applies: interactive.
        external = asyncio.create_task(self.chat(self.chat_headers("external")))
        await self.services.wait_calls(3)
        self.assertEqual(self.scheduler.interactive_claimed, 1)
        await self.release_held()
        async with await external as response:
            self.assertEqual(response.status, 200)
            request_id = response.headers["X-Request-ID"]
        self.assertEqual(self.services.call_count(), 3)
        self.assertEqual(
            self.gateway.diagnostics.admission("external", request_id), (INTERACTIVE, ADMITTED, 0)
        )
        for task in background:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        await self.wait_for(lambda: self.gateway.idle)

    async def test_declared_workload_header_cannot_change_the_class(self):
        self.services.mode = "hold"
        background = [
            asyncio.create_task(self.chat(self.chat_headers("memory-index"))) for _ in range(2)
        ]
        # Both pooled attempts are forwarded first: what is measured is the background
        # capacity, not the order in which the test happens to create its tasks.
        await self.services.wait_calls(2)
        self.assertEqual(self.scheduler.background_claimed, 2)
        # The caller declares a workload that is not the one its own binding publishes. If the
        # declaration counted, this order would be admitted into the reserved interactive slot
        # instead of waiting behind the pooled background orders.
        queued = asyncio.create_task(
            self.chat(
                self.chat_headers("memory-index", request_id="spoof", workload="companion.text")
            )
        )
        await self.wait_for(lambda: self.scheduler.waiting == 1)
        self.assertFalse(queued.done())
        self.assertEqual(self.services.call_count(), 2)
        self.assertEqual(self.scheduler.interactive_claimed, 0)
        # A queued order is not a forwarded one, so there is no admission fact for it yet.
        self.assertIsNone(self.gateway.diagnostics.admission("memory-index", "spoof"))
        await self.release_held([*background, queued])
        for task in [*background, queued]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        # The class stays the bound one and the order really did wait for background capacity;
        # the declared value never reaches the decision.
        workload_class, outcome, wait_ms = self.gateway.diagnostics.admission(
            "memory-index", "spoof"
        )
        self.assertEqual((workload_class, outcome), (BACKGROUND, ADMITTED))
        self.assertGreaterEqual(wait_ms, 0)
        await self.wait_for(lambda: self.gateway.idle)

    async def test_only_published_workload_values_are_accepted_in_the_header(self):
        # The declared header is ordinary input validation, not a scheduling input: an
        # unpublished value is refused before any forwarding, and an unpublished deployment
        # binding is refused at start-up.
        async with await self.chat(
            self.chat_headers("memory-index", request_id="unknown-class", workload="realtime")
        ) as refused:
            self.assertEqual(refused.status, 400)
            self.assertEqual((await refused.json())["code"], "invalid_input")
        self.assertEqual(self.services.call_count(), 0)
        document = deployment_document(
            scheduling={**asdict(policy()), "interactive_reserve": self.reserve},
            workload_bindings={"bindings": {"memory-index": "realtime"}},
        )
        with self.assertRaises(ValueError):
            load_settings(json.dumps(document)).validate()

    async def test_wait_duration_is_private_and_the_receipt_is_unchanged(self):
        self.services.mode = "hold"
        held = [asyncio.create_task(self.chat(self.chat_headers("memory-index"))) for _ in range(2)]
        await self.services.wait_calls(2)
        blockers = [
            asyncio.create_task(self.chat(self.chat_headers("companion")))
            for _ in range(self.global_limit - 2)
        ]
        await self.wait_for(lambda: self.scheduler.inflight == self.global_limit)
        queued = asyncio.create_task(
            self.chat(self.chat_headers("memory-index", request_id="measured-wait"))
        )
        await self.wait_for(lambda: self.scheduler.waiting == 1)
        await asyncio.sleep(0.05)
        await self.release_held([*held, *blockers, queued])
        for task in [*held, *blockers, queued]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        workload_class, outcome, wait_ms = self.gateway.diagnostics.admission(
            "memory-index", "measured-wait"
        )
        self.assertEqual((workload_class, outcome), (BACKGROUND, ADMITTED))
        self.assertGreaterEqual(wait_ms, 40)
        # Private observation only: the authoritative receipt and the upstream bytes are
        # exactly what they would have been without any scheduling.
        receipt = self.gateway.diagnostics.get("memory-index", "measured-wait")
        self.assertEqual(receipt["outcome"], "succeeded")
        self.assertEqual(receipt["caller_service"], "memory-index")
        last_raw, last_headers = self.services.calls[-1]
        self.assertEqual(json.loads(last_raw), self.body)
        self.assertEqual(last_headers["Authorization"], "Bearer " + SECRETS["TS041_TEST_UPSTREAM"])
        await self.wait_for(lambda: self.gateway.idle)

    async def test_cli_subprocess_builds_the_same_pool_from_the_deployment(self):
        document = asdict(self.settings)
        document["diagnostics_path"] = str(Path(self.temp.name) / "cli-scheduled.sqlite")
        path = Path(self.temp.name) / "scheduled-settings.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        program = (
            "import json;from pathlib import Path;"
            "from tianshu_gateway.server import create_app,load_settings;"
            f"settings=load_settings(Path(r'{path}').read_bytes());"
            "app=create_app(settings);"
            "print(json.dumps({'reserve':settings.policy.interactive_reserve,"
            "'class':settings.workload_bindings.for_service('memory-index'),"
            "'pool':settings.policy.max_in_flight,"
            "'provider_pool':settings.policy.max_provider_in_flight,"
            "'chat':any(getattr(r.resource,'canonical','')=='/v1/chat/completions'"
            " for r in app.router.routes()),"
            "'receipt_routes':sum(1 for r in app.router.routes()"
            " if 'model-requests' in getattr(r.resource,'canonical','')),"
            "'routes':len(app.router.routes())}))"
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-B",
            "-c",
            program,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=0x08000000 if sys.platform == "win32" else 0,
        )
        out, error = await asyncio.wait_for(process.communicate(), 15)
        self.assertEqual(process.returncode, 0, error.decode())
        report = json.loads(out)
        # The same deployment document selects the same bounded pool in a fresh process, and
        # the routes the pool wraps are all present. The exact route count is the composition
        # of the Chat, receipt and native tables, so it is only bounded here.
        self.assertEqual(
            {key: report[key] for key in ("reserve", "class", "pool", "provider_pool")},
            {
                "reserve": self.reserve,
                "class": BACKGROUND,
                "pool": self.global_limit,
                "provider_pool": self.provider_limit,
            },
        )
        self.assertTrue(report["chat"])
        self.assertGreaterEqual(report["receipt_routes"], 2)
        self.assertGreaterEqual(report["routes"], 8)
        for secret in SECRETS.values():
            self.assertNotIn(secret.encode(), out + error)
        self.assertGreater(port, 0)


class ScheduledGatewayTests(
    ScheduledFixtures, ScheduledScenarios, unittest.IsolatedAsyncioTestCase
):
    """The production-shaped policy: three in flight, one reserved, four waiting orders."""


class ShortDeadlineGatewayTests(
    ScheduledFixtures, ScheduledScenarios, unittest.IsolatedAsyncioTestCase
):
    """The same scenarios with a short deadline: the refusal really is a deadline."""

    wait_ms = 150


class QueueBoundGatewayTests(ScheduledFixtures, unittest.IsolatedAsyncioTestCase):
    """The same fixture with the smallest useful queue: one waiting order, no more.

    The scenario mixin is deliberately not applied here: a one-slot queue cannot run the
    scenarios that need waiting orders to queue up, so this case carries only the bound check
    it exists for.
    """

    queue_length = 1

    async def test_queue_length_bound_refuses_without_forwarding(self):
        self.services.mode = "hold"
        # Two interactive orders and one background order fill the pool exactly, so the next
        # background order is the only one allowed to wait.
        held = [
            asyncio.create_task(self.chat(self.chat_headers("companion"))),
            asyncio.create_task(self.chat(self.chat_headers("companion"))),
            asyncio.create_task(self.chat(self.chat_headers("memory-index"))),
        ]
        await self.services.wait_calls(3)
        queued = asyncio.create_task(
            self.chat(self.chat_headers("memory-index", request_id="queued-one"))
        )
        await self.wait_for(lambda: self.scheduler.waiting == 1)
        # A refusal is immediate, so this request is never parked at the upstream.
        async with await self.chat(
            self.chat_headers("companion", request_id="over-limit")
        ) as refused:
            self.assertEqual(refused.status, 429)
            self.assertEqual((await refused.json())["code"], "queue_full")
        # A refusal is not a forward: no upstream attempt and no receipt row.
        async with self.client.get(
            self.url + "/internal/v1/model-requests/over-limit",
            headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_OTHER"]},
        ) as receipt:
            self.assertEqual(receipt.status, 404)
        self.assertEqual(self.services.call_count(), 3)
        self.assertEqual(self.scheduler.waiting, 1)
        await self.release_held([*held, queued])
        for task in [*held, queued]:
            async with await task as finished:
                self.assertEqual(finished.status, 200)
        self.assertEqual(self.services.call_count(), 4)
        await self.wait_for(lambda: self.gateway.idle)


if __name__ == "__main__":
    unittest.main()
