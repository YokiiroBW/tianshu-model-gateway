"""Protected dynamic Chat callers over actual local HTTP/TLS; all owners are fixtures."""

import copy
import os
import ssl
import tempfile
import unittest
from dataclasses import replace
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
from observability_fixtures import start_tls
from test_provider_adapter import FixtureTargets
from tianshu_gateway.config import Classification, ClientGrant, SchedulingPolicy, utcnow
from tianshu_gateway.native import NativeGrant
from tianshu_gateway.provider_adapter import OpenAIAdapter
from tianshu_gateway.server import GATEWAY, Settings, create_app

BACKGROUND_TOKEN = "synthetic-ts317-background-token"
DYNAMIC_VERSION = 901


class BackgroundRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        env = patch.dict(os.environ, {**SECRETS, "TS317_BACKGROUND": BACKGROUND_TOKEN})
        env.start()
        self.addCleanup(env.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="ts317-gateway-")
        self.addCleanup(self.temp.cleanup)
        self.services = RecordingServices()
        upstream = web.Application()
        upstream.router.add_post("/v1/chat/completions", self.services.upstream)
        upstream.router.add_post("/v1/responses", self.services.native_upstream)
        runner, self.upstream = await start_tls(upstream, self.temp.name)
        self.addAsyncCleanup(runner.cleanup)
        static_upstream = web.Application()
        static_upstream.router.add_post("/v1/chat/completions", self.services.upstream)
        static_upstream.router.add_post("/v1/responses", self.services.native_upstream)
        runner, self.static_upstream = await start_http(static_upstream)
        self.addAsyncCleanup(runner.cleanup)
        self.services.configure(self.static_upstream)
        self.runtime_calls = []
        self.reject_runtime_at = None
        platform = web.Application()
        platform.router.add_post("/internal/v1/model-config/snapshot", self.services.snapshot)
        platform.router.add_post(
            "/internal/v1/model-config/native/snapshot", self.services.native_snapshot
        )
        platform.router.add_post("/internal/v1/provider-self-service/runtime", self.runtime)
        runner, self.platform = await start_http(platform)
        self.addAsyncCleanup(runner.cleanup)
        references = {"secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM"}
        for name in ("CLIENT", "OTHER", "EXTERNAL", "PLATFORM"):
            references["secret-ref:fixture/" + name.lower()] = "TS041_TEST_" + name
        references.update(
            {
                "secret-ref:fixture/background": "TS317_BACKGROUND",
                "secret-ref:fixture/native": "TS042_TEST_NATIVE",
            }
        )
        self.settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "diagnostics.sqlite"),
            self.platform,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            references,
            [registration(self.platform), registration(self.static_upstream + "/v1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                ),
                ClientGrant(
                    "companion_background",
                    "secret-ref:fixture/background",
                    "provider-fixture",
                    7,
                    True,
                ),
                ClientGrant(
                    "unknown_internal", "secret-ref:fixture/other", "provider-fixture", 7, True
                ),
                ClientGrant("external", "secret-ref:fixture/external", "provider-fixture", 7),
            ],
            native_enabled=True,
            native_contract_directory=str(NATIVE_CONTRACT),
            native_clients=[
                NativeGrant(
                    service="native-caller",
                    credential_ref="secret-ref:fixture/native",
                    principal_id="principal-fixture",
                    credential_namespace="credential-namespace-fixture",
                    provider_ids=("provider-fixture",),
                    native_config_versions=(7,),
                    native_config_version=7,
                    internal=True,
                )
            ],
            scheduling=SchedulingPolicy(3, 3, 1, 4, 1000),
            workload_bindings=Classification({"companion_background": "background"}),
            provider_self_service=True,
        )
        self.document = {
            "config_version": DYNAMIC_VERSION,
            "provider_id": "provider-fixture",
            "provider_revision": 3,
            "protocol": "openai-chat-completions",
            "base_url": self.upstream + "/v1",
            "model_id": DOCUMENTS["native_request"]["model"],
            "api_key": SECRETS["TS041_TEST_UPSTREAM"],
            "usable_until": (utcnow() + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
            "verified_capabilities": ["text", "stream", "tools", "vision", "reasoning"],
            "unsupported_capabilities": [],
        }
        await self.launch(self.settings)
        self.client = aiohttp.ClientSession(trust_env=False)
        self.addAsyncCleanup(self.client.close)
        self.body = copy.deepcopy(DOCUMENTS["native_request"])

    async def launch(self, settings):
        app = create_app(settings)
        runner, self.url = await start_http(app)
        self.addAsyncCleanup(runner.cleanup)
        self.gateway = app[GATEWAY]
        # Only the existing fixture's target policy permits loopback. Actual TLS and routing
        # are used; the production adapter/target policy are not replaced with a fake result.
        self.gateway.provider_adapter = OpenAIAdapter(
            policy=FixtureTargets(),
            tls_context=ssl.create_default_context(cafile=Path(self.temp.name) / "server.crt"),
        )

    async def runtime(self, request):
        self.assertEqual(
            request.headers["Authorization"], "Bearer " + SECRETS["TS041_TEST_PLATFORM"]
        )
        body = await request.json()
        self.runtime_calls.append(body)
        self.assertEqual(set(body), {"config_version", "turn_id", "caller_service", "workload"})
        self.assertEqual(body["workload"], "companion.text")
        if self.reject_runtime_at == len(self.runtime_calls):
            return web.Response(status=403)
        self.assertIn(body["caller_service"], {"companion", "companion_background"})
        self.assertEqual(body["config_version"], DYNAMIC_VERSION)
        return web.json_response(self.document)

    def headers(
        self, *, token=BACKGROUND_TOKEN, request_id="request:background", version=DYNAMIC_VERSION
    ):
        return {
            "Authorization": "Bearer " + token,
            "X-Request-ID": request_id,
            "X-Tianshu-Turn-ID": "turn:" + request_id,
            "X-Tianshu-Config-Version": str(version),
            "X-Tianshu-Workload": "companion.text",
        }

    async def chat(self, headers, *, expected=200, body=None):
        async with self.client.post(
            self.url + "/v1/chat/completions",
            headers=headers,
            json=self.body if body is None else body,
        ) as response:
            answer = await response.json()
            self.assertEqual(response.status, expected, answer)
            return answer

    async def test_background_dynamic_uses_its_authenticated_caller_and_bound_class(self):
        headers = self.headers()
        async with self.client.get(
            self.url + "/internal/v1/model-capabilities", headers=headers
        ) as response:
            self.assertEqual(response.status, 200, await response.text())
        self.assertEqual(self.services.calls, [])
        self.assertEqual(await self.chat(headers), self.services.response)
        self.assertEqual(len(self.services.calls), 1)
        self.assertEqual(len(self.runtime_calls), 4)  # capability + three original fresh checks
        self.assertTrue(
            all(
                call
                == {
                    "caller_service": "companion_background",
                    "config_version": DYNAMIC_VERSION,
                    "turn_id": "turn:request:background",
                    "workload": "companion.text",
                }
                for call in self.runtime_calls
            )
        )
        admission = self.gateway.diagnostics.admission(
            "companion_background", headers["X-Request-ID"]
        )
        self.assertEqual(admission[:2], ("background", "admitted"))
        receipt = self.gateway.diagnostics.get("companion_background", headers["X-Request-ID"])
        self.assertEqual(receipt["caller_service"], "companion_background")
        self.assertEqual(receipt["config_version"], DYNAMIC_VERSION)
        self.assertIsNone(self.gateway.diagnostics.get("companion", headers["X-Request-ID"]))
        sent = self.services.calls[0][1]
        self.assertEqual(sent["Authorization"], "Bearer " + SECRETS["TS041_TEST_UPSTREAM"])
        self.assertNotIn("X-Tianshu-Workload", sent)
        self.assertTrue(self.gateway.idle)

    async def test_companion_dynamic_still_uses_original_identity_and_interactive_class(self):
        headers = self.headers(token=SECRETS["TS041_TEST_CLIENT"], request_id="request:foreground")
        await self.chat(headers)
        self.assertEqual(len(self.runtime_calls), 3)
        self.assertTrue(all(call["caller_service"] == "companion" for call in self.runtime_calls))
        self.assertEqual(
            self.gateway.diagnostics.admission("companion", headers["X-Request-ID"])[:2],
            ("interactive", "admitted"),
        )

    async def test_background_published_version_keeps_original_snapshot_path(self):
        await self.chat(self.headers(version=7))
        self.assertEqual(self.runtime_calls, [])
        self.assertGreaterEqual(len(self.services.config_calls), 1)
        self.assertEqual(len(self.services.calls), 1)

    async def test_unknown_internal_dynamic_rejected_but_its_pinned_registration_is_unchanged(self):
        headers = self.headers(token=SECRETS["TS041_TEST_OTHER"])
        await self.chat(headers, expected=403)
        self.assertEqual(self.runtime_calls, [])
        self.assertEqual(self.services.calls, [])
        await self.chat({**headers, "X-Tianshu-Config-Version": "7"})
        self.assertEqual(len(self.services.calls), 1)

    async def test_noninternal_background_registration_cannot_supply_dynamic_context(self):
        clients = [
            replace(client, internal=False) if client.service == "companion_background" else client
            for client in self.settings.clients
        ]
        await self.launch(
            replace(
                self.settings,
                clients=clients,
                diagnostics_path=str(Path(self.temp.name) / "noninternal.sqlite"),
            )
        )
        await self.chat(self.headers(), expected=403)
        self.assertEqual(self.runtime_calls, [])
        self.assertEqual(self.services.calls, [])
        await self.chat({"Authorization": "Bearer " + BACKGROUND_TOKEN})
        self.assertEqual(self.runtime_calls, [])
        self.assertEqual(len(self.services.calls), 1)

    async def test_unknown_token_or_workload_cannot_gain_background_dynamic_access(self):
        await self.chat(self.headers(token="unregistered-synthetic-token"), expected=401)
        headers = self.headers()
        headers["X-Tianshu-Workload"] = "background"
        await self.chat(headers, expected=400)
        self.assertEqual(self.runtime_calls, [])
        self.assertEqual(self.services.calls, [])

    async def test_runtime_revocation_after_selection_refuses_before_upstream(self):
        self.reject_runtime_at = 2
        answer = await self.chat(self.headers(), expected=403)
        self.assertEqual(answer["code"], "forbidden")
        self.assertEqual(len(self.runtime_calls), 2)
        self.assertEqual(self.services.calls, [])
        self.assertTrue(self.gateway.idle)

    async def test_chat_dynamic_access_does_not_inherit_native_identity_or_version(self):
        native_headers = {
            "Authorization": "Bearer " + BACKGROUND_TOKEN,
            "X-Request-ID": "native:background",
            "X-Tianshu-Turn-ID": "turn:native",
            "X-Tianshu-Native-Config-Version": "7",
        }
        async with self.client.post(
            self.url + "/v1/responses",
            json=NATIVE_DOCUMENTS["native_request"],
            headers=native_headers,
        ) as response:
            self.assertEqual(response.status, 401, await response.text())
        headers = {**self.headers(), "X-Tianshu-Native-Config-Version": "7"}
        await self.chat(headers, expected=400)
        await self.chat(self.headers(token=SECRETS["TS042_TEST_NATIVE"]), expected=401)
        self.assertEqual(self.runtime_calls, [])
        self.assertEqual(self.services.calls, [])
        self.assertEqual(self.services.native_calls, [])
