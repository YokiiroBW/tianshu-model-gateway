"""Renewal transport negatives over verified loopback TLS; producer here is a test double."""

import asyncio
import hashlib
import json
import os
import ssl
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp
from aiohttp import web

from gateway_fixtures import WORKSPACE
from observability_fixtures import start_tls, write_tls
from tianshu_gateway.config import EnvSecrets, RegisteredTargets, SourceUnavailable
from tianshu_gateway.contracts import Rejected
from tianshu_gateway.origin_renewal import (
    PATH,
    CurrentCache,
    OriginRenewal,
    validate_request,
    validate_response,
    validate_settings,
)

REF = "origin:" + "a" * 32
ENV = {"TS110_ORIGIN": REF, "TS110_PLATFORM": "synthetic-gateway-service-credential-110"}


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


class OriginWireTests(unittest.TestCase):
    def test_pinned_frozen_contract_examples_and_strict_scalar_types(self):
        root = WORKSPACE / "contracts/model-origin-renewal/v1"
        raw = (root / "manifest.json").read_bytes()
        self.assertEqual(
            hashlib.sha256(raw).hexdigest(),
            "c5017724187c1386b647fcc5b41ab3cb1702f27d6192f3a87fcefb23e7a5a61c",
        )
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            for name, expected in json.loads(raw)["files"].items():
                data = (root / name).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), expected)
                (target / name).write_bytes(data)
            examples = json.loads((target / "examples.json").read_bytes())
            validate_request(examples["request"])
            validate_response(examples["response"], examples["request"])
            for item in json.loads((target / "negative-examples.json").read_bytes()):
                with self.subTest(item=item), self.assertRaises(Rejected):
                    {"request": validate_request, "response": validate_response}[item["kind"]](
                        item["document"]
                    )
            for value in (True, 1.0, "1", None):
                with self.assertRaises(Rejected):
                    validate_response({**examples["response"], "schema_version": value})
        for enabled in (1, "true", None):
            with self.assertRaises(ValueError):
                validate_settings(enabled, "https://localhost")
        validate_settings(False, "http://127.0.0.1")
        with self.assertRaises(ValueError):
            validate_settings(True, "http://127.0.0.1")


class OriginTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        env = patch.dict(os.environ, ENV)
        env.start()
        self.addCleanup(env.stop)
        self.status = 200
        self.ttl = 10
        self.calls = []
        self.delay = 0
        self.release = None
        self.raw = None
        self.headers = {}
        self.mutate = lambda body: body
        self.disconnects = 0
        self.stream = False
        self.trap_calls = 0
        app = web.Application()
        app.router.add_post(PATH, self.renew)
        app.router.add_post("/trap", self.trap)
        self.runner, self.url = await start_tls(app, self.temp.name)
        self.addAsyncCleanup(self.runner.cleanup)
        cert, _ = write_tls(self.temp.name)
        self.ssl = ssl.create_default_context(cafile=cert)
        self.client = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=self.ssl), auto_decompress=False, trust_env=False
        )
        self.addAsyncCleanup(self.client.close)
        self.origin = self.manager()

    def manager(self, client=None):
        origin = OriginRenewal(
            client or self.client,
            RegisteredTargets([{"base_url": self.url, "addresses": ["127.0.0.1"]}]),
            EnvSecrets({"secret-ref:test/platform": "TS110_PLATFORM"}),
            self.url,
            "secret-ref:test/platform",
            "TS110_ORIGIN",
        )
        self.addAsyncCleanup(origin.close)
        return origin

    async def trap(self, request):
        self.trap_calls += 1
        return web.Response()

    async def renew(self, request):
        body = await request.json()
        validate_request(body)
        self.assertEqual(request.headers["Authorization"], "Bearer " + ENV["TS110_PLATFORM"])
        self.calls.append(body)
        if self.release:
            await self.release.wait()
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.disconnects:
            self.disconnects -= 1
            request.transport.abort()
            return web.Response()
        if self.status != 200:
            return web.Response(status=self.status, headers={"Location": self.url + "/trap"})
        if self.stream:
            response = web.StreamResponse(headers={"Content-Type": "application/json"})
            await response.prepare(request)
            for _ in range(100):
                await response.write(b" ")
                await asyncio.sleep(0.05)
            return response
        if self.raw is not None:
            return web.Response(body=self.raw, content_type="application/json")
        return web.json_response(
            self.mutate({**body, "expires_at": utc(time.time() + self.ttl)}),
            headers=self.headers,
        )

    async def test_single_flight_waiter_cancel_and_shared_success(self):
        self.release = asyncio.Event()
        tasks = [asyncio.create_task(self.origin.ensure()) for _ in range(20)]
        while not self.calls:
            await asyncio.sleep(0.005)
        for task in tasks[:10]:
            task.cancel()
        await asyncio.gather(*tasks[:10], return_exceptions=True)
        self.assertEqual(len(self.calls), 1)
        self.release.set()
        await asyncio.gather(*tasks[10:])
        self.origin.check()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(os.environ["TS110_ORIGIN"], REF)

    async def test_close_cancels_all_tasks_even_during_start_and_repeated_close(self):
        self.release = asyncio.Event()
        waiter = asyncio.create_task(self.origin.start())
        while not self.calls:
            await asyncio.sleep(0.005)
        await asyncio.wait_for(asyncio.gather(self.origin.close(), self.origin.close()), 0.5)
        await asyncio.gather(waiter, return_exceptions=True)
        self.assertTrue(self.origin._flight.done())
        self.assertIsNone(self.origin._worker)
        with self.assertRaises(Rejected):
            await self.origin.ensure()

    async def test_idle_short_ttl_crosses_multiple_periods_and_closes_worker(self):
        self.ttl = 1
        await self.origin.start()
        initial = self.origin.expires_at
        await asyncio.sleep(2.2)
        self.origin.check()
        self.assertGreaterEqual(len(self.calls), 4)
        self.assertGreater(self.origin.expires_at, initial + 1)
        self.assertEqual({call["assertion_ref"] for call in self.calls}, {REF})
        await self.origin.close()
        self.assertTrue(self.origin._worker.done())

    async def test_auth_denial_is_terminal_without_retry(self):
        for status in (401, 403, 410):
            origin = self.manager()
            self.status = status
            before = len(self.calls)
            with self.subTest(status=status), self.assertRaises(Rejected):
                await origin.start()
            self.status = 200
            with self.assertRaises(Rejected):
                await origin.ensure()
            self.assertEqual(len(self.calls), before + 1)

    async def test_transient_disconnect_recovers_only_within_bounded_round(self):
        self.disconnects = 2
        await self.origin.start()
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(len({call["request_id"] for call in self.calls}), 1)
        self.origin.check()

    async def test_three_service_failures_stop_without_unbounded_retry(self):
        self.status = 503
        with self.assertRaises(Rejected):
            await self.origin.start()
        self.assertEqual(len(self.calls), 3)
        self.status = 200
        with self.assertRaises(Rejected):
            await self.origin.ensure()
        self.assertEqual(len(self.calls), 3)

    async def test_confirmed_origin_recovers_after_failed_round_without_cache_fallback(self):
        for status in (429, 503, "disconnect"):
            with self.subTest(status=status):
                origin = self.manager()
                await origin.ensure()
                initial = origin.expires_at
                origin.renew_at = 0
                before = len(self.calls)
                if status == "disconnect":
                    self.disconnects = 3
                else:
                    self.status = status
                with self.assertRaises(Rejected) as failure:
                    await origin.ensure()
                self.assertEqual(failure.exception.status, 503)
                self.assertNotIsInstance(failure.exception, SourceUnavailable)
                self.assertEqual(len(self.calls), before + 3)
                self.assertIsNone(origin.failure)
                self.assertEqual(origin.expires_at, initial)

                # Read-only probes and a warm cache cannot authorize stale configuration.
                cache = SimpleNamespace(
                    get=AsyncMock(return_value="verified-configuration"),
                    source=SimpleNamespace(fetch=AsyncMock()),
                )
                current = CurrentCache(cache, origin)
                for _ in range(3):
                    with self.assertRaises(Rejected):
                        origin.check()
                    with self.assertRaises(Rejected):
                        await current.get(7)
                cache.get.assert_not_awaited()
                cache.source.source.fetch.assert_not_awaited()
                self.assertEqual(len(self.calls), before + 3)

                self.status = 200
                await asyncio.sleep(0.26)
                self.assertEqual(await current.get(7), "verified-configuration")
                cache.get.assert_awaited_once_with(7)
                self.assertEqual(len(self.calls), before + 4)
                self.assertGreater(origin.expires_at, initial)
                self.assertEqual(origin.ref, REF)
                self.assertEqual(origin.retry_delay, 0.25)
                origin.check()

    async def test_worker_recovers_after_whole_round_timeout_with_same_authority(self):
        await self.origin.start()
        initial = self.origin.expires_at
        self.origin.renew_at = 0
        self.delay = 3.2
        async with asyncio.timeout(4):
            while not (self.origin.unavailable and self.origin._flight.done()):
                await asyncio.sleep(0.005)
        self.assertIsNone(self.origin.failure)
        self.assertEqual(self.origin.expires_at, initial)
        self.assertFalse(self.origin._worker.done())
        with self.assertRaises(Rejected):
            self.origin.check()
        self.delay = 0
        async with asyncio.timeout(1):
            while self.origin.unavailable:
                await asyncio.sleep(0.005)
        self.origin.check()
        self.assertGreater(self.origin.expires_at, initial)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual({call["assertion_ref"] for call in self.calls}, {REF})

    async def test_worker_and_concurrent_business_waiters_share_recovery_flight(self):
        await self.origin.start()
        self.origin.renew_at = 0
        self.status = 503
        async with asyncio.timeout(1.5):
            while not (self.origin.unavailable and self.origin._flight.done()):
                await asyncio.sleep(0.005)
        self.assertEqual(len(self.calls), 4)
        self.status = 200
        self.release = asyncio.Event()
        async with asyncio.timeout(1):
            while len(self.calls) < 5:
                await asyncio.sleep(0.005)
        tasks = [asyncio.create_task(self.origin.ensure()) for _ in range(20)]
        await asyncio.sleep(0.02)
        for task in tasks[:10]:
            task.cancel()
        await asyncio.gather(*tasks[:10], return_exceptions=True)
        self.assertEqual(len(self.calls), 5)
        with self.assertRaises(Rejected):
            self.origin.check()
        self.release.set()
        await asyncio.wait_for(asyncio.gather(*tasks[10:]), 1)
        self.origin.check()
        self.assertEqual(len(self.calls), 5)
        self.assertFalse(self.origin._worker.done())

    async def test_continuous_failures_cross_original_ttl_and_never_revive(self):
        self.ttl = 1.5
        await self.origin.start()
        initial = self.origin.expires_at
        self.origin.renew_at = 0
        self.status = 503
        async with asyncio.timeout(2.5):
            while not self.origin._worker.done():
                await asyncio.sleep(0.005)
        self.assertEqual(self.origin.failure, ("forbidden", 403))
        self.assertEqual(self.origin.expires_at, initial)
        count = len(self.calls)
        self.status = 200
        for _ in range(3):
            with self.assertRaises(Rejected):
                await self.origin.ensure()
        self.assertEqual(len(self.calls), count)

    async def test_close_during_retry_backoff_cancels_worker_promptly(self):
        await self.origin.start()
        self.origin.renew_at = 0
        self.status = 503
        async with asyncio.timeout(1.5):
            while not (self.origin.unavailable and self.origin._flight.done()):
                await asyncio.sleep(0.005)
        count = len(self.calls)
        await asyncio.wait_for(self.origin.close(), 0.5)
        self.assertTrue(self.origin._worker.done() and self.origin._flight.done())
        with self.assertRaises(Rejected):
            await self.origin.ensure()
        self.assertEqual(len(self.calls), count)

    async def test_confirmed_origin_hard_auth_rejection_remains_terminal(self):
        for status in (401, 403, 410):
            with self.subTest(status=status):
                origin = self.manager()
                await origin.ensure()
                initial = origin.expires_at
                origin.renew_at = 0
                self.status = status
                before = len(self.calls)
                with self.assertRaises(Rejected):
                    await origin.ensure()
                self.status = 200
                with self.assertRaises(Rejected):
                    await origin.ensure()
                self.assertEqual(len(self.calls), before + 1)
                self.assertEqual(origin.expires_at, initial)
                self.assertEqual(origin.failure, ("forbidden", 403))

    async def test_maximum_valid_ttl_is_accepted_after_real_server_processing_time(self):
        self.ttl = 3600
        self.delay = 0.05
        await self.origin.ensure()
        self.assertLessEqual(self.origin.deadline - time.monotonic(), 3600)
        self.assertGreater(self.origin.expires_at - time.time(), 3599)

    async def test_response_echo_date_range_unknown_fields_and_redirect_rejected(self):
        changes = (
            lambda b: {**b, "assertion_ref": "origin:" + "b" * 32},
            lambda b: {**b, "request_id": "11111111-2222-4333-8444-555555555555"},
            lambda b: {**b, "expires_at": "2030-02-30T00:00:00Z"},
            lambda b: {**b, "expires_at": "2030-01-01T00:00:00"},
            lambda b: {**b, "expires_at": utc(time.time() + 3601)},
            lambda b: {**b, "expires_at": utc(time.time() - 1)},
            lambda b: {**b, "scope": "new"},
            lambda b: {**b, "schema_version": True},
        )
        for change in changes:
            for confirmed in (False, True):
                with self.subTest(change=change, confirmed=confirmed):
                    self.mutate = lambda body: body
                    origin = self.manager()
                    if confirmed:
                        await origin.ensure()
                        origin.renew_at = 0
                    initial = origin.expires_at
                    self.mutate = change
                    before = len(self.calls)
                    with self.assertRaises(Rejected):
                        await origin.ensure()
                    self.mutate = lambda body: body
                    with self.assertRaises(Rejected):
                        await origin.ensure()
                    self.assertEqual(len(self.calls), before + 1)
                    self.assertEqual(origin.expires_at, initial)
        self.status = 307
        with self.assertRaises(Rejected):
            await self.origin.ensure()
        self.assertEqual(self.trap_calls, 0)

    async def test_duplicate_json_nonfinite_and_response_limit(self):
        for raw in (b'{"schema_version":1,"schema_version":1}', b'{"x":NaN}', b" " * 4097):
            for confirmed in (False, True):
                with self.subTest(raw=raw[:30], confirmed=confirmed):
                    self.raw = None
                    origin = self.manager()
                    if confirmed:
                        await origin.ensure()
                        origin.renew_at = 0
                    self.raw = raw
                    before = len(self.calls)
                    with self.assertRaises(Rejected):
                        await origin.ensure()
                    self.raw = None
                    with self.assertRaises(Rejected):
                        await origin.ensure()
                    self.assertEqual(len(self.calls), before + 1)

    async def test_invalid_encoding_is_terminal_for_confirmed_origin(self):
        await self.origin.ensure()
        self.origin.renew_at = 0
        self.headers = {"Content-Encoding": "gzip"}
        with self.assertRaises(Rejected):
            await self.origin.ensure()
        self.headers = {}
        with self.assertRaises(Rejected):
            await self.origin.ensure()
        self.assertEqual(len(self.calls), 2)

    async def test_continuous_trickle_has_one_total_deadline(self):
        self.stream = True
        start = time.monotonic()
        with self.assertRaises(Rejected):
            await self.origin.ensure()
        self.assertLess(time.monotonic() - start, 3.5)
        self.assertEqual(len(self.calls), 1)

    async def test_delayed_validation_cannot_commit_success_after_round_deadline(self):
        real_validate = validate_response

        def delayed(body, request=None):
            value = real_validate(body, request)
            time.sleep(3.05)
            return value

        with patch("tianshu_gateway.origin_renewal.validate_response", side_effect=delayed):
            with self.assertRaises(Rejected):
                await self.origin.ensure()
        self.assertIsNone(self.origin.expires_at)
        self.assertEqual(len(self.calls), 1)

    async def test_known_expiry_caps_entire_retry_and_late_response_cannot_revive(self):
        self.ttl = 1
        await self.origin.ensure()
        old_expiry = self.origin.expires_at
        self.delay = 1.2
        await asyncio.sleep(0.55)
        start = time.monotonic()
        with self.assertRaises(Rejected):
            await self.origin.ensure()
        self.assertLess(time.monotonic() - start, 0.6)
        self.assertEqual(self.origin.expires_at, old_expiry)
        self.delay = 0
        with self.assertRaises(Rejected):
            await self.origin.ensure()

    async def test_environment_swap_missing_initial_and_clock_rollback_do_not_extend_authority(
        self,
    ):
        await self.origin.ensure()
        with patch.dict(os.environ, {"TS110_ORIGIN": "origin:" + "b" * 32}):
            with self.assertRaises(Rejected):
                await self.origin.ensure()
        with self.assertRaises(Rejected):
            self.origin.check()
        with patch.dict(os.environ, {"TS110_ORIGIN": ""}):
            with self.assertRaises(Rejected):
                self.manager()
        origin = self.manager()
        await origin.ensure()
        with (
            patch("tianshu_gateway.origin_renewal.time.time", return_value=time.time() - 100),
            patch(
                "tianshu_gateway.origin_renewal.time.monotonic", return_value=origin.deadline + 1
            ),
        ):
            with self.assertRaises(Rejected):
                origin.check()

    async def test_untrusted_certificate_never_accepted(self):
        async with aiohttp.ClientSession(trust_env=False) as client:
            origin = self.manager(client)
            with self.assertRaises(Rejected):
                await origin.ensure()
            self.assertEqual(len(self.calls), 0)
