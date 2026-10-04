import asyncio
import copy
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from datetime import timedelta
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web

from gateway_fixtures import (
    CONTRACT,
    DOCUMENTS,
    SECRETS,
    STREAM,
    WORKSPACE,
    RecordingServices,
    registration,
    start_http,
)
from tianshu_gateway.config import ClientGrant, utcnow
from tianshu_gateway.provider_adapter import PROVIDER_SESSION_HEADER, provider_headers
from tianshu_gateway.server import GATEWAY, Settings, create_app


class GatewayHttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_session_metadata_reaches_only_opencode_header(self):
        def integration(base, session):
            return provider_headers("https://opencode.ai/zen/go/v1", session)

        with patch("tianshu_gateway.server.provider_headers", side_effect=integration):
            for _ in range(2):
                headers = {**self.headers(), PROVIDER_SESSION_HEADER: "synthetic-stable-session"}
                async with await self.post(headers=headers) as response:
                    self.assertEqual(response.status, 200)
                    await response.read()
            async with await self.post() as response:
                self.assertEqual(response.status, 400)
                self.assertEqual((await response.json())["code"], "invalid_input")
        self.assertEqual(len(self.services.calls), 2)
        first = self.services.calls[0][1]
        second = self.services.calls[1][1]
        self.assertEqual(first["x-opencode-session"], second["x-opencode-session"])
        self.assertEqual(first["User-Agent"], "tianshu-model-gateway/0.1.0")
        self.assertNotIn(PROVIDER_SESSION_HEADER, first)

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
        references = {"secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM"}
        for name in ("CLIENT", "OTHER", "EXTERNAL", "PLATFORM"):
            references["secret-ref:fixture/" + name.lower()] = "TS041_TEST_" + name
        self.settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "diagnostics.sqlite"),
            self.platform_url,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            references,
            [registration(self.platform_url), registration(self.upstream_url + "/v1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                ),
                ClientGrant("external", "secret-ref:fixture/external", "provider-fixture", 7),
                ClientGrant("other", "secret-ref:fixture/other", "provider-fixture", 7),
            ],
        )
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.gateway = self.app[GATEWAY]
        self.client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=False)
        self.addAsyncCleanup(self.client.close)
        self.sequence = 0
        self.body = copy.deepcopy(DOCUMENTS["native_request"])
        self.body["vendor_extension"] = {"null": None, "false": False, "nested": [1, {"x": "中文"}]}

    def headers(self, *, external=False, request_id=None, turn=None, version=7):
        self.sequence += 1
        if external:
            return {"Authorization": "Bearer " + SECRETS["TS041_TEST_EXTERNAL"]}
        return {
            "Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"],
            "X-Request-ID": request_id or f"request-{self.sequence}",
            "X-Tianshu-Turn-ID": turn or f"turn-{self.sequence}",
            "X-Tianshu-Config-Version": str(version),
            "X-Tianshu-Workload": "companion.text",
        }

    async def post(self, body=None, headers=None):
        return await self.client.post(
            self.url + "/v1/chat/completions",
            json=self.body if body is None else body,
            headers=self.headers() if headers is None else headers,
        )

    async def receipt(self, request_id, token="CLIENT", expected=200):
        async with self.client.get(
            self.url + "/internal/v1/model-requests/" + request_id,
            headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_" + token]},
        ) as response:
            self.assertEqual(response.status, expected)
            result = await response.json()
        if expected == 200:
            self.gateway.contracts.validate("model#route_receipt", result)
        return result

    async def idle(self):
        async with asyncio.timeout(2):
            while self.gateway.active:
                await asyncio.sleep(0.01)
        self.assertTrue(all(n == 0 for n in self.gateway.provider_active.values()))
        self.assertFalse(self.gateway.session.connector._acquired)

    async def test_native_http_fidelity_and_published_relationship(self):
        headers = self.headers(request_id="native-proof")
        headers.update(
            {
                "OpenAI-Beta": "fixture-beta=1",
                "X-API-Key": "client-key-must-not-forward",
                "Cookie": "session=private",
                "X-Untrusted-Header": "drop",
            }
        )
        raw = json.dumps(self.body, ensure_ascii=False, indent=2).encode()
        async with self.client.post(
            self.url + "/v1/chat/completions",
            data=raw,
            headers={**headers, "Content-Type": "application/json"},
        ) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(await response.json(), self.services.response)
            self.assertNotIn("Set-Cookie", response.headers)
        recorded, sent = self.services.calls[0]
        self.assertEqual(recorded, raw)
        self.assertEqual(sent["Authorization"], "Bearer " + SECRETS["TS041_TEST_UPSTREAM"])
        self.assertEqual(sent["OpenAI-Beta"], "fixture-beta=1")
        self.assertEqual(sent["Accept-Encoding"], "identity")
        for key in (
            "X-Request-ID",
            "X-Tianshu-Config-Version",
            "X-Tianshu-Turn-ID",
            "X-API-Key",
            "Cookie",
            "X-Untrusted-Header",
        ):
            self.assertNotIn(key, sent)
        route = await self.receipt("native-proof")
        self.assertEqual(route["usage"], {"input_tokens": 0})
        self.assertFalse(route["usage_complete"])
        self.assertEqual(route["native_usage"], self.services.response["usage"])
        self.assertEqual(route["outcome"], "succeeded")
        query, source_headers = self.services.config_calls[0]
        self.gateway.contracts.validate("model#config_request", query)
        self.assertEqual(query["config_version"], 7)
        self.assertEqual(
            source_headers["Authorization"], "Bearer " + SECRETS["TS041_TEST_PLATFORM"]
        )
        # Feed actual HTTP input, recorded output and persisted receipt into the published relation.
        spec = importlib.util.spec_from_file_location(
            "published_validator", WORKSPACE / "contracts/validate.py"
        )
        validator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(validator)
        validator.check_gateway(
            {
                "config": self.services.config,
                "receipt": route,
                "incoming": self.body,
                "outgoing": json.loads(recorded),
                "pinned_version": 7,
                "started_at": utcnow().isoformat().replace("+00:00", "Z"),
                "revoked": False,
                "upstream_usage": self.services.response["usage"],
                "reference_binding": None,
            }
        )

    async def test_external_preserve_and_internal_header_spoof_rejected(self):
        self.body["model"] = "exact-client-model"
        self.body["reasoning"] = {"vendor": ["keep", None]}
        async with await self.post(headers=self.headers(external=True)) as response:
            self.assertEqual(response.status, 200)
            request_id = response.headers["X-Request-ID"]
        self.assertEqual(json.loads(self.services.calls[0][0]), self.body)
        route = await self.receipt(request_id, "EXTERNAL")
        self.assertEqual(route["requested_model"], "exact-client-model")
        self.assertEqual(route["applied_policies"], [])
        for extra in (
            {"X-Tianshu-Config-Version": "8"},
            {"X-Request-ID": "fake"},
            {"OpenAI-Organization": "cross-tenant"},
            {"X-Tianshu-Unknown": "fake"},
        ):
            async with await self.post(
                headers={**self.headers(external=True), **extra}
            ) as response:
                self.assertEqual(response.status, 403)
        self.assertEqual(len(self.services.calls), 1)
        await self.receipt(request_id, "OTHER", 404)

    async def test_default_force_missing_null_and_reasoning_values(self):
        provider = self.services.config["providers"][0]
        provider["model_policy"] = {
            "mode": "default_if_absent",
            "fields": {"model": "default-model"},
        }
        provider["reasoning_policy"] = {
            "mode": "default_if_absent",
            "fields": {"reasoning_effort": "default-effort"},
        }
        self.body.pop("model")
        self.body["reasoning_effort"] = None
        async with await self.post(headers=self.headers(external=True)) as response:
            self.assertEqual(response.status, 200)
            route = await self.receipt(response.headers["X-Request-ID"], "EXTERNAL")
        self.assertIsNone(route["requested_model"])
        self.assertIsNone(route["effective_reasoning"]["reasoning_effort"])
        self.assertEqual(
            route["applied_policies"],
            [{"field": "model", "mode": "default_if_absent", "config_version": 7}],
        )
        for value in (None, "", False, 0):
            async with await self.post(
                {**self.body, "model": value}, self.headers(external=True)
            ) as response:
                self.assertEqual(response.status, 400)
        # A new immutable version, explicitly authorized to the same internal service.
        self.services.config["config_version"] = 8
        provider["model_policy"] = {"mode": "force", "fields": {"model": "forced-model"}}
        provider["reasoning_policy"] = {"mode": "force", "fields": {"reasoning_effort": False}}
        async with await self.post(headers=self.headers(version=8)) as response:
            self.assertEqual(response.status, 200)
            route = await self.receipt(response.headers["X-Request-ID"])
        self.assertIsNone(route["requested_model"])
        self.assertEqual(route["resolved_model"], "forced-model")
        self.assertEqual([p["mode"] for p in route["applied_policies"]], ["force", "force"])
        async with await self.post(
            {**self.body, "model": None}, self.headers(version=8)
        ) as response:
            self.assertEqual(response.status, 400)
        self.assertEqual(len(self.services.calls), 2)

    async def test_missing_model_requires_internal_binding_or_explicit_policy(self):
        self.body.pop("model")
        async with await self.post(headers=self.headers(external=True)) as response:
            self.assertEqual(response.status, 400)
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
            route = await self.receipt(response.headers["X-Request-ID"])
        self.assertIsNone(route["requested_model"])
        self.assertEqual(route["applied_policies"][0]["mode"], "workload_binding")

    async def test_real_one_byte_sse_tool_utf8_and_usage_fidelity(self):
        self.services.mode = "one_byte"
        async with await self.post({**self.body, "stream": True}) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(await response.read(), STREAM)
            route = await self.receipt(response.headers["X-Request-ID"])
        self.assertEqual(route["outcome"], "succeeded")
        self.assertEqual(route["usage"], {"input_tokens": 0, "output_tokens": 9})
        self.assertTrue(route["usage_complete"])
        await self.idle()

    async def test_stream_is_delivered_before_upstream_completes(self):
        self.services.mode = "sse_hold"
        async with await self.post({**self.body, "stream": True}) as response:
            async with asyncio.timeout(0.8):
                first = await response.content.readany()
            self.assertTrue(first)
            self.assertFalse(self.services.release.is_set())
            self.services.release.set()
            self.assertEqual(first + await response.read(), STREAM)
        await self.idle()

    async def test_incomplete_clean_eof_and_transport_drop_never_succeed(self):
        for mode, data in (
            ("stream", STREAM.replace(b"data: [DONE]\r\n\r\n", b"")),
            ("drop", STREAM[:300]),
            ("stream", b"data: [DONE]\n\n"),
            ("stream", b'data: {"error":{"message":"fixture"}}\n\ndata: [DONE]\n\n'),
        ):
            with self.subTest(mode=mode, size=len(data)):
                self.services.mode, self.services.stream = mode, data
                async with await self.post({**self.body, "stream": True}) as response:
                    request_id = response.headers["X-Request-ID"]
                    with self.assertRaises(aiohttp.ClientPayloadError):
                        await response.read()
                await self.idle()
                route = await self.receipt(request_id)
                self.assertEqual(route["outcome"], "unknown")
                self.assertFalse(route["usage_complete"])
        self.assertEqual(len(self.services.calls), 4)

    async def test_http_errors_status_and_secrets_are_separate_from_receipts(self):
        self.services.mode = "error"
        with self.assertLogs("tianshu_gateway", level="INFO") as logs:
            for status in (401, 429, 500, 307):
                self.services.http_status = status
                async with await self.post() as response:
                    self.assertEqual(response.status, status)
                    raw = await response.text()
                    self.assertNotIn("Location", response.headers)
                    self.assertNotIn("Set-Cookie", response.headers)
                    route = await self.receipt(response.headers["X-Request-ID"])
                    for token in SECRETS.values():
                        self.assertNotIn(token, raw + json.dumps(route))
                self.assertFalse(route["fallback_used"])
        for token in SECRETS.values():
            self.assertNotIn(token, " ".join(logs.output))
        self.assertEqual(len(self.services.calls), 4)

    async def test_sse_event_error_field_variants_persist_unknown_without_byte_rewriting(self):
        completed = b'data:{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}'
        self.services.mode = "sse_hold"
        for newline in (b"\n", b"\r\n", b"\r"):
            for field in (b"event:error", b"event: error"):
                with self.subTest(newline=newline, field=field):
                    self.services.release.clear()
                    error_prefix = newline.join(
                        (
                            completed,
                            b"",
                            field,
                            b'data:{"message":',
                            b'data: "provider failed"}',
                            b"",
                            b"",
                        )
                    )
                    # Deliver the whole error event before EOF; the padding also drains the
                    # existing secret-check tail without changing the forwarding implementation.
                    self.services.stream = (
                        error_prefix
                        + b":"
                        + b"x" * 350
                        + newline * 2
                        + b"data:[DONE]"
                        + newline * 2
                    )
                    received = b""
                    async with await self.post({**self.body, "stream": True}) as response:
                        request_id = response.headers["X-Request-ID"]
                        async with asyncio.timeout(1):
                            received = await response.content.readexactly(len(error_prefix))
                        self.assertEqual(received, error_prefix)
                        self.services.release.set()
                        with self.assertRaises(aiohttp.ClientPayloadError):
                            async for chunk in response.content.iter_any():
                                received += chunk
                    self.assertEqual(received, self.services.stream[: len(received)])
                    await self.idle()
                    route = await self.receipt(request_id)
                    self.assertEqual(route["outcome"], "unknown")
                    self.assertFalse(route["usage_complete"])
                    self.assertFalse(route["fallback_used"])
                    reason = self.gateway.diagnostics.connection.execute(
                        "SELECT reason FROM requests WHERE request_id=?", (request_id,)
                    ).fetchone()[0]
                    self.assertEqual(reason, "incomplete_stream")
        self.assertEqual(len(self.services.calls), 6)

    async def test_reflected_secret_split_across_real_http_chunks_is_blocked(self):
        for mode in ("secret_json", "secret_stream"):
            self.services.mode = mode
            received = b""
            async with await self.post(
                {**self.body, "stream": mode == "secret_stream"}
            ) as response:
                try:
                    async for chunk in response.content.iter_any():
                        received += chunk
                except aiohttp.ClientPayloadError:
                    pass
                request_id = response.headers["X-Request-ID"]
            await self.idle()
            self.assertNotIn(SECRETS["TS041_TEST_UPSTREAM"].encode(), received)
            self.assertEqual((await self.receipt(request_id))["outcome"], "unknown")

    async def test_timeout_cancels_upstream_and_releases_capacity(self):
        self.services.config["bindings"][0]["timeout_ms"] = 100
        self.services.mode = "hold"
        async with await self.post() as response:
            self.assertEqual(response.status, 502)
            request_id = response.headers["X-Request-ID"]
            error = await response.json()
        self.assertEqual(error["execution_state"], "unknown")
        self.assertFalse(error["retryable"])
        await self.idle()
        await asyncio.wait_for(self.services.disconnected.wait(), 1)
        self.assertEqual((await self.receipt(request_id))["outcome"], "unknown")
        self.services.mode = "json"
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(len(self.services.calls), 2)

    async def test_client_cancel_before_headers_closes_upstream_and_persists_unknown(self):
        self.services.mode = "hold"
        task = asyncio.create_task(
            self.post(headers=self.headers(request_id="cancelled-before-headers"))
        )
        await asyncio.wait_for(self.services.started.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.idle()
        await asyncio.wait_for(self.services.disconnected.wait(), 1)
        self.assertEqual((await self.receipt("cancelled-before-headers"))["outcome"], "unknown")

    async def test_stream_cancel_and_provider_limit_release(self):
        self.settings.max_provider_concurrent = 1
        self.services.mode = "sse_hold"
        response = await self.post({**self.body, "stream": True})
        request_id = response.headers["X-Request-ID"]
        await response.content.readany()
        async with await self.post() as blocked:
            self.assertEqual(blocked.status, 429)
        response.close()
        await self.idle()
        await asyncio.wait_for(self.services.disconnected.wait(), 1)
        self.assertEqual((await self.receipt(request_id))["outcome"], "unknown")
        self.assertEqual(len(self.services.calls), 1)

    async def test_config_cache_fixed_version_offline_and_expired(self):
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
        self.services.source_status = 503
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(len(self.services.config_calls), 1)
        self.gateway.cache.refresh_seconds = 0
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
        async with await self.post(headers=self.headers(version=8)) as response:
            self.assertEqual(response.status, 503)
        self.gateway.cache.entries[7][0]["usable_until"] = (
            (utcnow() - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        )
        async with await self.post() as response:
            self.assertEqual(response.status, 503)
        self.assertEqual(len(self.services.calls), 3)

    async def test_revocation_and_credential_resolution_fail_closed(self):
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
        del os.environ["TS041_TEST_UPSTREAM"]
        async with await self.post() as response:
            self.assertEqual(response.status, 503)
        os.environ["TS041_TEST_UPSTREAM"] = SECRETS["TS041_TEST_UPSTREAM"]
        self.gateway.cache.refresh_seconds = 0
        self.services.source_status = 410
        async with await self.post() as response:
            self.assertEqual(response.status, 403)
        self.services.source_status = 200
        async with await self.post() as response:
            self.assertEqual(response.status, 403)
        self.assertEqual(len(self.services.calls), 1)

    async def test_ambiguous_mismatched_and_unregistered_configuration_rejected(self):
        original = copy.deepcopy(self.services.config)
        variants = []
        duplicate = copy.deepcopy(original)
        duplicate["bindings"].append(copy.deepcopy(duplicate["bindings"][0]))
        variants.append(duplicate)
        wrong = copy.deepcopy(original)
        wrong["bindings"][0]["provider_id"] = "different-provider"
        variants.append(wrong)
        wrong_model = copy.deepcopy(original)
        wrong_model["bindings"][0]["model_id"] = "different-model"
        variants.append(wrong_model)
        arbitrary = copy.deepcopy(original)
        arbitrary["providers"][0]["base_url"] = "http://127.0.0.1:1/v1"
        variants.append(arbitrary)
        for config in variants:
            self.services.config = config
            async with await self.post() as response:
                self.assertIn(response.status, (400, 503))
        self.assertEqual(self.services.calls, [])

    async def test_config_content_is_immutable_and_turn_and_request_ids_are_pinned(self):
        async with await self.post(
            headers=self.headers(request_id="fixed-request", turn="fixed-turn")
        ) as response:
            self.assertEqual(response.status, 200)
        async with await self.post(headers=self.headers(request_id="fixed-request")) as response:
            self.assertEqual(response.status, 409)
        self.gateway.cache.refresh_seconds = 0
        self.services.config["providers"][0]["model_policy"] = {
            "mode": "force",
            "fields": {"model": "changed"},
        }
        async with await self.post() as response:
            self.assertEqual(response.status, 409)
        self.services.config["config_version"] = 8
        async with await self.post(headers=self.headers(version=8, turn="fixed-turn")) as response:
            self.assertEqual(response.status, 409)
        async with await self.post(headers=self.headers(version=8)) as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(len(self.services.calls), 2)

    async def test_input_limits_auth_unsupported_and_state_references(self):
        for key in ("previous_response_id", "conversation_id", "file_id"):
            async with await self.post(
                {**self.body, "vendor": {key: "unresolved-state"}}
            ) as response:
                self.assertEqual(response.status, 400)
        for path in ("/v1/responses", "/v1/messages", "/v1/embeddings"):
            async with self.client.post(
                self.url + path, headers=self.headers(), json={}
            ) as response:
                self.assertEqual(response.status, 501)
        async with await self.post(
            headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_UPSTREAM"]}
        ) as response:
            self.assertEqual(response.status, 401)
        for raw in (b'{"messages":[],"messages":[]}', b'{"messages":NaN}', b"[]", b"{"):
            async with self.client.post(
                self.url + "/v1/chat/completions",
                data=raw,
                headers={**self.headers(), "Content-Type": "application/json"},
            ) as response:
                self.assertEqual(response.status, 400)
        self.settings.max_request_bytes = 32
        async with await self.post() as response:
            self.assertEqual(response.status, 413)
        self.assertEqual(self.services.calls, [])

    async def test_unknown_usage_invalid_response_and_response_limit(self):
        self.services.response["usage"] = None
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
            route = await self.receipt(response.headers["X-Request-ID"])
        self.assertIsNone(route["usage"])
        self.assertIsNone(route["native_usage"])
        self.assertFalse(route["usage_complete"])
        for mode in ("invalid_json", "encoded"):
            self.services.mode = mode
            async with await self.post() as response:
                self.assertEqual(response.status, 502)
        self.services.mode = "json"
        self.settings.max_response_bytes = 5
        async with await self.post() as response:
            self.assertEqual(response.status, 502)
        self.assertEqual(len(self.services.calls), 4)

    async def test_sqlite_restart_preserves_receipt_revocation_and_turn_version(self):
        async with await self.post(
            headers=self.headers(request_id="durable", turn="durable-turn")
        ) as response:
            self.assertEqual(response.status, 200)
        self.gateway.cache.revoke(7)
        await self.runner.cleanup()
        self.assertTrue(self.gateway.session.closed)
        app = create_app(self.settings)
        runner, self.url = await start_http(app)
        self.addAsyncCleanup(runner.cleanup)
        self.gateway = app[GATEWAY]
        self.assertEqual((await self.receipt("durable"))["outcome"], "succeeded")
        async with await self.post() as response:
            self.assertEqual(response.status, 403)
        self.services.config["config_version"] = 8
        async with await self.post(
            headers=self.headers(version=8, turn="durable-turn")
        ) as response:
            self.assertEqual(response.status, 409)

    async def test_field_policy_keeps_unknown_json_number_bytes_and_tool_schema(self):
        self.services.config["providers"][0]["model_policy"] = {
            "mode": "force",
            "fields": {"model": "forced-fixture"},
        }
        raw = (
            b'{"model":"old", "messages":[{"role":"user","content":"fixture"}], '
            b'"vendor_float": 0.12345678901234567890123456789, "vendor_int":9999999999999999999999,'
            b'"tools":[{"type":"function","function":{"name":"fixture",'
            b'"parameters":{"type":"object","properties":{"file_id":{"type":"string"}}}}}]}'
        )
        async with self.client.post(
            self.url + "/v1/chat/completions",
            data=raw,
            headers={**self.headers(), "Content-Type": "application/json"},
        ) as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(
            self.services.calls[0][0], raw.replace(b'"model":"old"', b'"model":"forced-fixture"')
        )

    async def test_invalid_platform_response_never_hides_behind_cache(self):
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
        self.gateway.cache.refresh_seconds = 0
        self.services.source_wrong_request = True
        async with await self.post() as response:
            self.assertEqual(response.status, 400)
        self.services.source_wrong_request = False
        self.services.config["config_version"] = 8
        async with await self.post() as response:
            self.assertEqual(response.status, 400)
        self.services.config["config_version"] = 7
        self.services.config["usable_until"] = "2020-01-01T00:00:00Z"
        async with await self.post() as response:
            self.assertEqual(response.status, 503)
        self.services.source_status = 404
        async with await self.post() as response:
            self.assertEqual(response.status, 400)
        self.assertEqual(len(self.services.calls), 1)

    async def test_revoke_while_config_fetch_in_flight_prevents_upstream_send(self):
        self.services.source_release = asyncio.Event()
        task = asyncio.create_task(self.post())
        async with asyncio.timeout(1):
            while not self.services.config_calls:
                await asyncio.sleep(0.01)
        self.gateway.cache.revoke(7)
        self.services.source_release.set()
        async with await task as response:
            self.assertEqual(response.status, 403)
        self.assertEqual(self.services.calls, [])

    async def test_stream_timeout_and_global_limit(self):
        self.settings.max_concurrent = 1
        self.services.config["bindings"][0]["timeout_ms"] = 150
        self.services.mode = "sse_hold"
        async with await self.post({**self.body, "stream": True}) as response:
            request_id = response.headers["X-Request-ID"]
            await response.content.readany()
            async with await self.post() as blocked:
                self.assertEqual(blocked.status, 429)
            with self.assertRaises(aiohttp.ClientPayloadError):
                await response.read()
        await self.idle()
        route = await self.receipt(request_id)
        self.assertEqual(route["outcome"], "unknown")
        reason = self.gateway.diagnostics.connection.execute(
            "SELECT reason FROM requests WHERE request_id=?", (request_id,)
        ).fetchone()[0]
        self.assertEqual(reason, "timeout_unknown")

    async def test_chunked_request_limit_and_read_timeout_before_upstream(self):
        self.settings.max_request_bytes = 32

        async def oversized():
            yield b" " * 20
            yield b" " * 20

        async with self.client.post(
            self.url + "/v1/chat/completions",
            data=oversized(),
            headers={**self.headers(), "Content-Type": "application/json"},
        ) as response:
            self.assertEqual(response.status, 413)
        self.settings.request_read_timeout = 0.05

        async def slow():
            yield b"{"
            await asyncio.sleep(1)
            yield b"}"

        async with self.client.post(
            self.url + "/v1/chat/completions",
            data=slow(),
            headers={**self.headers(), "Content-Type": "application/json"},
        ) as response:
            self.assertEqual(response.status, 408)
        await self.idle()
        self.assertEqual(self.services.calls, [])

    async def test_cli_subprocess_serves_http_then_stops(self):
        settings = asdict(self.settings)
        settings["diagnostics_path"] = str(Path(self.temp.name) / "cli.sqlite")
        path = Path(self.temp.name) / "settings.json"
        path.write_text(json.dumps(settings), encoding="utf-8")
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        command = [
            sys.executable,
            "-B",
            "-m",
            "tianshu_gateway",
            "--settings",
            str(path),
            "--port",
            str(port),
        ]
        flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        invalid = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=flags,
        )
        _, error = await asyncio.wait_for(invalid.communicate(), 3)
        self.assertEqual(invalid.returncode, 2)
        self.assertIn(b"TLS certificate and key required", error)
        process = await asyncio.create_subprocess_exec(
            *command,
            "--local-test",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=flags,
        )
        try:
            async with asyncio.timeout(3):
                while True:
                    try:
                        async with self.client.post(
                            f"http://127.0.0.1:{port}/v1/chat/completions",
                            json=self.body,
                            headers=self.headers(),
                        ) as response:
                            self.assertEqual(response.status, 200)
                            self.assertEqual(await response.json(), self.services.response)
                            break
                    except aiohttp.ClientConnectorError:
                        await asyncio.sleep(0.03)
        finally:
            if process.returncode is None:
                process.terminate()
            out, error = await asyncio.wait_for(process.communicate(), 3)
        for secret in SECRETS.values():
            self.assertNotIn(secret.encode(), out + error)
        with socket.socket() as probe:
            probe.settimeout(0.05)
            self.assertNotEqual(probe.connect_ex(("127.0.0.1", port)), 0)

    async def test_diagnostics_write_failure_after_send_remains_unknown(self):
        def disk_failure(*args):
            raise OSError("fixture failure " + SECRETS["TS041_TEST_UPSTREAM"])

        with patch.object(self.gateway.diagnostics, "finish", side_effect=disk_failure):
            with self.assertLogs("tianshu_gateway", level="ERROR") as logs:
                async with await self.post(
                    headers=self.headers(request_id="disk-fault")
                ) as response:
                    self.assertEqual(response.status, 503)
                    error = await response.json()
        self.assertEqual(error["execution_state"], "unknown")
        self.assertNotIn(SECRETS["TS041_TEST_UPSTREAM"], " ".join(logs.output))
        self.assertEqual((await self.receipt("disk-fault"))["outcome"], "unknown")
        await self.idle()

    async def test_no_configuration_and_source_credential_failure_reject(self):
        self.services.source_status = 503
        async with await self.post() as response:
            self.assertEqual(response.status, 503)
        self.assertEqual(self.services.calls, [])
        self.services.source_status = 200
        async with await self.post() as response:
            self.assertEqual(response.status, 200)
        self.gateway.cache.refresh_seconds = 0
        del os.environ["TS041_TEST_PLATFORM"]
        async with await self.post() as response:
            self.assertEqual(response.status, 503)
        self.assertEqual(len(self.services.calls), 1)


if __name__ == "__main__":
    unittest.main()
