"""Gateway composition preserves real provider IO and module-owned configuration."""

import asyncio
import copy
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web
from gateway_fixtures import (
    CONTRACT,
    DOCUMENTS,
    SECRETS,
    STREAM,
    RecordingServices,
    registration,
    start_http,
)
from tianshu_storage import StorageOptions

from tianshu_gateway.config import ClientGrant, EnvSecrets
from tianshu_gateway.contracts import Rejected
from tianshu_gateway.observability import Observability, ObservabilitySettings
from tianshu_gateway.origin_renewal import OriginRenewal
from tianshu_gateway.server import GATEWAY, Settings, create_app

try:
    from deploy.single_container.exchange import LocalExchange
except ModuleNotFoundError:
    LocalExchange = None


class EnvironmentTests(unittest.TestCase):
    def test_independent_secret_probe_and_renewal_bindings(self):
        first = {"TOKEN": "first-synthetic-service-token", "ORIGIN": "origin:" + "a" * 32}
        second = {"TOKEN": "second-synthetic-service-token", "ORIGIN": "origin:" + "b" * 32}
        a = EnvSecrets({"credential": "TOKEN"}, environ=first)
        b = EnvSecrets({"credential": "TOKEN"}, environ=second)
        self.assertEqual(a.resolve("credential"), first["TOKEN"])
        self.assertEqual(b.known_values(), [second["TOKEN"]])
        with tempfile.TemporaryDirectory() as directory:
            settings = ObservabilitySettings(log_directory=directory, probe_token_env="TOKEN")
            oa = Observability(settings, environ=first)
            ob = Observability(settings, environ=second)
            self.assertEqual(oa.probe_token(), first["TOKEN"])
            self.assertEqual(ob.probe_token(), second["TOKEN"])
            first["TOKEN"] = "rotated-first-synthetic-service-token"
            self.assertEqual(oa.probe_token(), first["TOKEN"])
            self.assertEqual(ob.probe_token(), second["TOKEN"])
        from tianshu_gateway.config import RegisteredTargets

        targets = RegisteredTargets([registration("https://platform:8443")])
        renewal = OriginRenewal(
            None, targets, a, "https://platform:8443", "credential", "ORIGIN", environ=first
        )
        renewal._available()
        first["ORIGIN"] = second["ORIGIN"]
        with self.assertRaises(Rejected):
            renewal._available()


@unittest.skipIf(LocalExchange is None, "joint test needs the unified host on PYTHONPATH")
class InternalGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.services = RecordingServices()
        upstream = web.Application()
        upstream.router.add_post("/v1/chat/completions", self.services.upstream)
        runner, self.upstream_url = await start_http(upstream)
        self.addAsyncCleanup(runner.cleanup)
        self.services.configure(self.upstream_url)
        self.exchange = LocalExchange(buffer_bytes=1024)
        platform = web.Application()

        async def snapshot(request):
            self.assertTrue(self.exchange.owns(request))
            self.assertFalse(request.secure)
            return await self.services.snapshot(request)

        platform.router.add_post("/internal/v1/model-config/snapshot", snapshot)
        self.platform_url = "https://platform:8443"
        self.gateway_url = "https://gateway:8444"
        references = {"secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM"}
        for name in ("CLIENT", "PLATFORM"):
            references["secret-ref:fixture/" + name.lower()] = "TS041_TEST_" + name
        self.settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "diagnostics.sqlite"),
            self.platform_url,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            references,
            [registration(self.platform_url), registration(self.upstream_url + "/v1")],
            [ClientGrant("companion", "secret-ref:fixture/client", "provider-fixture", 7, True)],
        )
        self.app = create_app(
            self.settings,
            internal_clients=self.exchange,
            environ=dict(SECRETS),
            storage_options=StorageOptions(namespace="synthetic-unified-gateway"),
        )
        self.exchange.register("platform", platform, "aiohttp", self.platform_url)
        self.exchange.register("gateway", self.app, "aiohttp", self.gateway_url)
        await self.exchange.open()
        self.addAsyncCleanup(self.exchange.close)
        self.client = self.exchange.session_factory("gateway")(
            timeout=aiohttp.ClientTimeout(total=5),
            auto_decompress=False,
        )
        self.addAsyncCleanup(self.client.close)

    def headers(self, request_id):
        return {
            "Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"],
            "X-Request-ID": request_id,
            "X-Tianshu-Turn-ID": request_id,
            "X-Tianshu-Config-Version": "7",
            "X-Tianshu-Workload": "companion.text",
        }

    async def test_local_config_and_gateway_keep_external_provider_and_receipt(self):
        async with self.client.post(
            self.gateway_url + "/v1/chat/completions",
            json=DOCUMENTS["native_request"],
            headers=self.headers("local-request"),
        ) as response:
            self.assertEqual(response.status, 200, await response.text())
            self.assertEqual(await response.json(), self.services.response)
        gateway = self.app[GATEWAY]
        self.assertIsInstance(gateway.session, aiohttp.ClientSession)
        self.assertIsNot(gateway.session, gateway.platform_session)
        self.assertEqual(len(self.services.calls), 1)
        body, headers = self.services.config_calls[0]
        self.assertEqual(body["query"]["origin"]["assertion_ref"], SECRETS["TS041_TEST_ORIGIN"])
        self.assertEqual(headers["Authorization"], "Bearer " + SECRETS["TS041_TEST_PLATFORM"])
        async with self.client.get(
            self.gateway_url + "/internal/v1/model-requests/local-request",
            headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]},
        ) as receipt:
            self.assertEqual(receipt.status, 200)
            gateway.contracts.validate("model#route_receipt", await receipt.json())
        self.assertTrue(all(not item.runner.sites for item in self.exchange._adapters.values()))
        async with self.client.post(
            self.gateway_url + "/v1/chat/completions",
            json=DOCUMENTS["native_request"],
            headers={**self.headers("denied-request"), "Authorization": "Bearer invalid"},
        ) as denied:
            self.assertEqual(denied.status, 401)
        self.assertEqual(len(self.services.calls), 1)

    async def test_streaming_uses_existing_gateway_path_without_buffering_whole_request(self):
        self.services.mode = "stream"
        body = copy.deepcopy(DOCUMENTS["native_request"])
        body["stream"] = True
        async with self.client.post(
            self.gateway_url + "/v1/chat/completions",
            json=body,
            headers=self.headers("local-stream"),
        ) as response:
            self.assertEqual(
                response.status, 200, await response.text() if response.status != 200 else ""
            )
            self.assertEqual(await response.read(), STREAM)
        async with asyncio.timeout(1):
            while self.app[GATEWAY].active:
                await asyncio.sleep(0)
        self.assertFalse(self.app[GATEWAY].session.connector._acquired)
