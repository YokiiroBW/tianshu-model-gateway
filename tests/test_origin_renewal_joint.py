"""Actual platform + gateway over verified TLS. Only model upstream/fault injection are doubles.

TS110_PLATFORM_ROOT must name the explicitly allocated producer checkout. No peer checkout,
real account, system trust store or deployment data is written.
"""

import asyncio
import copy
import json
import os
import ssl
import subprocess
import sys
import tempfile
import time
import unittest
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
)
from observability_fixtures import WORKSPACE, observation_settings, read_log, start_tls, write_tls
from tianshu_gateway.config import ClientGrant
from tianshu_gateway.contracts import Rejected
from tianshu_gateway.native import NativeGrant
from tianshu_gateway.origin_renewal import PATH
from tianshu_gateway.observability import Observability
from tianshu_gateway.server import GATEWAY, Settings, create_app

PLATFORM_ROOT = os.environ.get("TS110_PLATFORM_ROOT")
if PLATFORM_ROOT:
    sys.path.insert(0, PLATFORM_ROOT)
    sys.path.insert(0, str(Path(PLATFORM_ROOT) / "tests/backend"))
    import fixtures as pf
    from services.platform.contracts import utc
    from services.platform.server import create_app as platform_app
    from services.platform.service import Platform


@unittest.skipUnless(
    PLATFORM_ROOT, "explicit TS110_PLATFORM_ROOT required; no joint claim without producer"
)
class RealProductRenewalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        env = patch.dict(os.environ, {**SECRETS, **pf.ENV})
        env.start()
        self.addCleanup(env.stop)
        self.cert, self.key = write_tls(self.root)
        self.ssl = ssl.create_default_context(cafile=self.cert)
        # Same CA-file trust as SSL_CERT_FILE before an actual CLI import. No verification disabled.
        trust = patch("aiohttp.connector._SSL_CONTEXT_VERIFIED", self.ssl)
        trust.start()
        self.addCleanup(trust.stop)
        self.services = RecordingServices()
        upstream = web.Application()
        upstream.router.add_post("/v1/chat/completions", self.services.upstream)
        upstream.router.add_post("/v1/responses", self.services.native_upstream)
        runner, self.upstream_url = await start_tls(upstream, self.root)
        self.addAsyncCleanup(runner.cleanup)
        self.services.configure(self.upstream_url)
        self.cfg = pf.settings(self.temp.name, self.upstream_url + "/v1")
        self.cfg.update(
            mode="service_https",
            model_origin_renewal_http=True,
            native_config_http=True,
            tls={"certificate_file": str(self.cert), "private_key_file": str(self.key)},
        )
        self.cfg["entries"]["config-entry"].update(ttl_seconds=2, expires_at=utc(time.time() + 30))
        self.cfg["principals"]["gateway"]["native_config_versions"] = [7]
        for item in self.cfg["providers"].values():
            item["reviewed_addresses"] = ["127.0.0.1"]
        self.p = Platform(self.cfg)
        self.addCleanup(self.p.close)
        self.ref = self.p.origins.issue(pf.bearer("ADMIN"), "config-entry")["assertion_ref"]
        os.environ["TS110_INITIAL_ORIGIN"] = self.ref
        self.p.models.publish(pf.bearer("ADMIN"), pf.config(upstream=self.upstream_url + "/v1"))
        self.p.models.native_publish(
            pf.bearer("ADMIN"), pf.native_config(upstream=self.upstream_url + "/v1")
        )
        self.offline = False
        self.drop_renewals = 0
        self.renewals = []
        self.snapshots = 0

        @web.middleware
        async def network_fault(request, handler):
            if request.path == PATH:
                self.renewals.append(time.monotonic())
            elif "snapshot" in request.path:
                self.snapshots += 1
            if self.offline or (request.path == PATH and self.drop_renewals):
                if self.drop_renewals:
                    self.drop_renewals -= 1
                request.transport.abort()
                return web.Response()
            return await handler(request)

        app = platform_app(self.p)
        app.middlewares.insert(0, network_fault)
        self.prunner, self.platform_url = await start_tls(app, self.root)
        self.addAsyncCleanup(self.prunner.cleanup)
        self.settings = Settings(
            contract_directory=str(CONTRACT),
            diagnostics_path=str(self.root / "gateway.sqlite"),
            platform_base_url=self.platform_url,
            platform_credential_ref="secret-ref:fixture/platform",
            platform_origin_env="TS110_INITIAL_ORIGIN",
            secret_references={
                "secret-ref:fixture/platform": "TS012_GATEWAY",
                "secret-ref:fixture/client": "TS041_TEST_CLIENT",
                "secret-ref:fixture/native": "TS042_TEST_NATIVE",
                "secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM",
            },
            targets=[
                {"base_url": self.platform_url, "addresses": ["127.0.0.1"]},
                {"base_url": self.upstream_url + "/v1", "addresses": ["127.0.0.1"]},
            ],
            clients=[
                ClientGrant("companion", "secret-ref:fixture/client", "provider-fixture", 7, True)
            ],
            platform_origin_renewal=True,
            native_enabled=True,
            native_contract_directory=str(NATIVE_CONTRACT),
            native_clients=[
                NativeGrant(
                    service="caller-fixture",
                    credential_ref="secret-ref:fixture/native",
                    principal_id="principal-fixture",
                    credential_namespace="credential-namespace-fixture",
                    provider_ids=("provider-native",),
                    native_config_versions=(7,),
                    native_config_version=7,
                    internal=True,
                )
            ],
        )
        self.app = create_app(self.settings)
        self.runner, self.url = await start_tls(self.app, self.root)
        self.addAsyncCleanup(self.runner.cleanup)
        self.gateway = self.app[GATEWAY]
        self.client = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=self.ssl),
            trust_env=False,
            timeout=aiohttp.ClientTimeout(total=5),
        )
        self.addAsyncCleanup(self.client.close)
        self.sequence = 0

    async def chat(self, expected=200, request_id=None):
        self.sequence += 1
        request_id = request_id or f"joint-{self.sequence}"
        headers = {
            "Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"],
            "X-Request-ID": request_id,
            "X-Tianshu-Turn-ID": request_id,
            "X-Tianshu-Config-Version": "7",
            "X-Tianshu-Workload": "companion.text",
        }
        body = copy.deepcopy(DOCUMENTS["native_request"])
        body["stream"] = False
        async with self.client.post(
            self.url + "/v1/chat/completions", json=body, headers=headers
        ) as r:
            data = await r.read()
            self.assertEqual(r.status, expected, data)
            self.assertNotIn(self.ref.encode(), data)
        return request_id

    async def native(self, expected=200):
        self.sequence += 1
        headers = {
            "Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"],
            "X-Request-ID": f"native-joint-{self.sequence}",
            "X-Tianshu-Turn-ID": f"native-joint-{self.sequence}",
            "X-Tianshu-Native-Config-Version": "7",
        }
        body = copy.deepcopy(NATIVE_DOCUMENTS["native_request"])
        body["stream"] = False
        async with self.client.post(self.url + "/v1/responses", json=body, headers=headers) as r:
            self.assertEqual(r.status, expected, await r.text())

    async def test_both_products_short_ttl_multiple_cycles_idle_and_exactly_once_model(self):
        initial = self.gateway.origin_renewal.expires_at
        for _ in range(3):
            await self.chat()
            await self.native()
            await asyncio.sleep(1.1)
        await asyncio.sleep(
            1.2
        )  # Renewal also continues with neither product receiving model work.
        self.gateway.origin_renewal.check()
        self.assertGreater(self.gateway.origin_renewal.expires_at, initial + 3)
        self.assertGreaterEqual(len(self.renewals), 5)
        self.assertEqual(len(self.services.calls), 3)
        self.assertEqual(len(self.services.native_calls), 3)
        self.assertGreaterEqual(self.snapshots, 6)
        with self.p.store.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM origins").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT ref FROM origins").fetchone()[0], self.ref)
        self.assertNotIn("origin.issue", self.p.auth.principals["gateway"]["actions"])
        self.assertEqual(os.environ["TS110_INITIAL_ORIGIN"], self.ref)

    async def test_ref_revocation_closes_chat_native_cached_access_and_future_renewals(self):
        await self.chat()
        await self.native()
        self.p.origins.revoke(pf.bearer("ADMIN"), "origin", self.ref)
        await self.chat(403)
        await self.native(403)
        count = len(self.renewals)
        with self.assertRaises(Rejected):
            await self.gateway.origin_renewal.ensure()
        self.assertEqual(len(self.renewals), count)
        self.assertEqual((len(self.services.calls), len(self.services.native_calls)), (1, 1))

    async def test_entry_revocation_denies_cached_native_then_chat(self):
        await self.native()
        await self.chat()
        self.p.origins.revoke(pf.bearer("ADMIN"), "entry", "config-entry")
        await self.native(403)
        await self.chat(403)
        self.assertEqual((len(self.services.calls), len(self.services.native_calls)), (1, 1))

    async def test_gateway_principal_revocation_stops_without_model_attempt(self):
        await self.chat()
        self.p.origins.revoke(pf.bearer("ADMIN"), "principal", "gateway")
        await self.chat(503)  # Existing snapshot's opaque 401 dependency mapping.
        await self.native(403)
        self.assertIsNotNone(self.gateway.origin_renewal.failure)
        self.assertEqual(len(self.services.calls), 1)

    async def test_owner_revocation_stops_both_paths(self):
        await self.chat()
        self.p.origins.revoke(pf.bearer("ADMIN"), "principal", "admin")
        await self.native(403)
        await self.chat(403)

    async def test_two_real_tls_disconnects_recover_without_issuing_new_ref(self):
        self.drop_renewals = 2
        self.gateway.origin_renewal.renew_at = 0
        before = len(self.renewals)
        await self.gateway.origin_renewal.ensure()
        self.assertEqual(len(self.renewals), before + 3)
        await self.chat()
        self.assertEqual(self.gateway.origin_renewal.ref, self.ref)
        self.assertEqual(len(self.services.calls), 1)

    async def test_offline_cross_expiry_no_cache_then_expired_restart_refused_and_rebootstrap_recovers(
        self,
    ):
        await self.chat()
        await self.native()
        self.offline = True
        await self.chat(503)
        await asyncio.sleep(2.2)
        await self.chat(403)
        self.offline = False
        with self.assertRaises(Rejected):
            await self.gateway.cache.get(7)
        await self.runner.cleanup()
        app = create_app(self.settings)
        runner = web.AppRunner(app, access_log=None)
        with self.assertRaises(Rejected):
            await runner.setup()
        await runner.cleanup()
        # The operator's original issue API, explicitly invoked here, is the only recovery.
        os.environ["TS110_INITIAL_ORIGIN"] = self.p.origins.issue(
            pf.bearer("ADMIN"), "config-entry"
        )["assertion_ref"]
        app = create_app(self.settings)
        runner, self.url = await start_tls(app, self.root)
        self.addAsyncCleanup(runner.cleanup)
        self.gateway = app[GATEWAY]
        await self.chat()
        self.assertEqual(len(self.services.calls), 2)
        self.assertEqual(len(self.services.native_calls), 1)

    async def test_wrong_scope_and_missing_initial_ref_fail_actual_startup(self):
        await self.runner.cleanup()
        for ref in (
            "",
            self.p.origins.issue(pf.bearer("CONNECTOR"), "chat-entry")["assertion_ref"],
        ):
            os.environ["TS110_INITIAL_ORIGIN"] = ref
            app = create_app(self.settings)
            runner = web.AppRunner(app, access_log=None)
            with self.assertRaises(Rejected):
                await runner.setup()
            await runner.cleanup()
        self.assertEqual(len(self.services.calls), 0)

    async def test_invalid_bootstrap_records_failed_start_without_started_or_secret(self):
        await self.runner.cleanup()
        self.p.origins.revoke(pf.bearer("ADMIN"), "origin", self.ref)
        log_root = self.root / "logs"
        obs_settings = observation_settings(log_root)
        self.settings.observability = obs_settings
        self.settings.diagnostics_contract_directory = str(WORKSPACE / "contracts/diagnostics/v1")
        observability = Observability(obs_settings)
        app = create_app(self.settings, observability)
        runner = web.AppRunner(app, access_log=None)
        with self.assertRaises(Rejected):
            await runner.setup()
        await runner.cleanup()
        events = [record["event"] for record in read_log(log_root)]
        self.assertIn("runtime.startup_failed", events)
        self.assertNotIn("runtime.started", events)
        corpus = b"".join(path.read_bytes() for path in log_root.iterdir() if path.is_file())
        self.assertNotIn(self.ref.encode(), corpus)
        self.assertTrue(app[GATEWAY].origin_renewal.closed)

    async def test_shutdown_cleans_background_and_does_not_replay_model(self):
        request_id = await self.chat()
        await self.chat(409, request_id=request_id)
        origin = self.gateway.origin_renewal
        await self.runner.cleanup()
        self.assertTrue(origin.closed and origin._worker.done() and origin._flight.done())
        self.assertFalse(
            any(
                t.get_name().startswith("model-origin-renewal") and not t.done()
                for t in asyncio.all_tasks()
            )
        )
        self.assertEqual(len(self.services.calls), 1)

    async def test_actual_gateway_cli_uses_explicit_ca_and_renews_after_initial_ttl(self):
        import socket
        from dataclasses import asdict

        await self.runner.cleanup()
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        cfg = self.root / "gateway-settings.json"
        cfg.write_text(json.dumps(asdict(self.settings)), encoding="utf-8")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-B",
            "-m",
            "tianshu_gateway",
            "--settings",
            str(cfg),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--tls-cert",
            str(self.cert),
            "--tls-key",
            str(self.key),
            env={
                **os.environ,
                "SSL_CERT_FILE": str(self.cert),
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.url = f"https://127.0.0.1:{port}"
        try:
            stop = time.monotonic() + 5
            while True:
                if process.returncode is not None:
                    self.fail("gateway subprocess exited during startup")
                try:
                    async with self.client.get(self.url + "/health/live") as response:
                        if response.status == 200:
                            break
                except aiohttp.ClientError:
                    pass
                if time.monotonic() >= stop:
                    self.fail("gateway subprocess did not become live")
                await asyncio.sleep(0.05)
            await asyncio.sleep(2.3)
            await self.chat()
            await self.native()
            self.assertGreaterEqual(len(self.renewals), 4)
        finally:
            if process.returncode is None:
                process.terminate()
            output, error = await asyncio.wait_for(process.communicate(), 5)
            self.assertNotIn(self.ref.encode(), output + error)
            for value in pf.ENV.values():
                self.assertNotIn(value.encode(), output + error)
