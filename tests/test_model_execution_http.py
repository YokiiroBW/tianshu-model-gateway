"""C2 real loopback HTTP: native deltas, explicit cancellation and owned receipts."""

import asyncio
import json
import unittest
from unittest.mock import patch

import aiohttp

import test_gateway_http as chat_fixture
import test_responses_native as native_fixture
from gateway_fixtures import SECRETS, STREAM, start_http
from test_ts044_scheduling import ScheduledFixtures
from tianshu_gateway.server import GATEWAY, create_app


class ModelExecutionHttpTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = chat_fixture.GatewayHttpTests.asyncSetUp
    headers = chat_fixture.GatewayHttpTests.headers
    post = chat_fixture.GatewayHttpTests.post
    receipt = chat_fixture.GatewayHttpTests.receipt
    idle = chat_fixture.GatewayHttpTests.idle

    async def cancel(self, request_id, token="CLIENT", body=None):
        async with self.client.post(
            self.url + "/internal/v1/model-requests/" + request_id + "/cancel",
            json={} if body is None else body,
            headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_" + token]},
        ) as response:
            return response.status, await response.json()

    async def test_capability_read_is_exact_and_does_not_call_provider_or_claim_fixture_verified(
        self,
    ):
        async with self.client.get(
            self.url + "/internal/v1/model-capabilities", headers=self.headers()
        ) as response:
            self.assertEqual(response.status, 200)
            result = await response.json()
        self.gateway.contracts.validate("model#capability_response", result)
        self.assertEqual(result["verification_source"], "fixture_only")
        self.assertTrue(
            all(
                c == {"native": True, "verification": "unverified"}
                for c in result["capabilities"].values()
            )
        )
        self.assertEqual(len(self.services.calls), 0)
        self.assertTrue(self.gateway.idle)
        self.assertNotIn("base_url", result)

    async def test_unverified_tool_vision_followup_keeps_native_body_and_tools_complete(self):
        self.services.config["providers"][0]["verified_capabilities"] = ["text"]
        call = {
            "id": "call-source",
            "type": "function",
            "function": {"name": "read", "arguments": "{}"},
        }
        self.services.response["choices"][0].update(
            message={"role": "assistant", "content": None, "tool_calls": [call]},
            finish_reason="tool_calls",
        )
        body = {
            **self.body,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Describe the actual image"},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "https://example.invalid/original.png",
                                "detail": "low",
                            },
                        },
                    ],
                },
                {"role": "assistant", "content": None, "tool_calls": [call]},
                {"role": "tool", "tool_call_id": "call-source", "content": "bounded source result"},
            ],
            "tools": [
                {"type": "function", "function": {"name": "read", "parameters": {"type": "object"}}}
            ],
        }
        async with await self.post(body) as response:
            self.assertEqual(response.status, 200)
            result = await response.json()
            request_id = response.headers["X-Request-ID"]
        self.assertEqual(result, self.services.response)
        self.assertEqual(json.loads(self.services.calls[0][0]), body)
        receipt = await self.receipt(request_id)
        self.assertEqual(receipt["execution"]["state"], "completed")
        self.assertEqual(receipt["execution"]["finish_reasons"], ["tool_calls"])
        self.assertEqual(receipt["execution"]["forwarded_bytes"], 0)

    async def test_explicit_unsupported_tools_and_vision_fail_before_call(self):
        provider = self.services.config["providers"][0]
        provider["verified_capabilities"] = ["text"]
        provider["unsupported_capabilities"] = ["tools", "vision"]
        for addition in (
            {"tools": [{"type": "function", "function": {"name": "read"}}]},
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": "https://example.invalid/a"}}
                        ],
                    }
                ]
            },
        ):
            async with await self.post({**self.body, **addition}) as response:
                self.assertEqual(response.status, 422)
                error = await response.json()
                self.assertEqual(error["code"], "capability_unsupported")
                self.assertEqual(error["execution_state"], "not_started")
                self.gateway.contracts.validate("common#error", error)
        self.assertEqual(self.services.calls, [])

    async def test_progress_is_incremental_and_cancel_is_owned_partial_and_not_retried(self):
        self.services.mode = "sse_hold"
        response = await self.post(
            {**self.body, "stream": True}, headers=self.headers(request_id="partial-live")
        )
        try:
            first = await response.content.readany()
            self.assertTrue(first)
            receipt = await self.receipt("partial-live")
            execution = receipt["execution"]
            self.assertEqual(execution["state"], "running")
            self.assertTrue(execution["upstream_started"])
            self.assertGreater(execution["received_bytes"], 0)
            self.assertGreater(execution["forwarded_bytes"], 0)
            self.assertNotIn("tool_calls", json.dumps(execution))
            status, _ = await self.cancel("partial-live", "OTHER")
            self.assertEqual(status, 404)
            self.assertEqual(self.gateway.active, 1)
            status, result = await self.cancel("partial-live")
            self.assertEqual(
                (status, result["state"], result["upstream_outcome"]), (200, "requested", "unknown")
            )
            try:
                rest = await response.read()
            except aiohttp.ClientError:
                rest = b""
            self.assertNotIn(b"[DONE]", first + rest)
        finally:
            response.close()
        await self.idle()
        await asyncio.wait_for(self.services.disconnected.wait(), 1)
        receipt = await self.receipt("partial-live")
        self.assertEqual(receipt["outcome"], "unknown")
        self.assertEqual(receipt["execution"]["state"], "cancelled")
        self.assertTrue(receipt["execution"]["cancel_requested"])
        self.assertFalse(receipt["usage_complete"])
        self.assertEqual((await self.cancel("partial-live"))[1]["state"], "terminal")
        self.assertEqual(len(self.services.calls), 1)

    async def test_stream_completion_records_events_usage_and_fixed_finish_reasons(self):
        self.services.mode = "one_byte"
        async with await self.post({**self.body, "stream": True}) as response:
            self.assertEqual(await response.read(), STREAM)
            request_id = response.headers["X-Request-ID"]
        receipt = await self.receipt(request_id)
        execution = receipt["execution"]
        self.assertEqual(execution["state"], "completed")
        self.assertEqual(execution["received_bytes"], len(STREAM))
        self.assertEqual(execution["forwarded_bytes"], len(STREAM))
        self.assertGreater(execution["event_count"], 1)
        self.assertTrue(execution["output_observed"])
        self.assertTrue(receipt["usage_complete"])
        self.assertIsNotNone(receipt["usage"])

    async def test_drop_and_timeout_keep_unknown_partial_receipt(self):
        self.services.mode = "drop"
        async with await self.post(
            {**self.body, "stream": True}, headers=self.headers(request_id="dropped")
        ) as response:
            try:
                await response.read()
            except aiohttp.ClientError:
                pass
        await self.idle()
        receipt = await self.receipt("dropped")
        self.assertEqual(receipt["execution"]["state"], "unknown")
        self.assertFalse(receipt["usage_complete"])
        self.services.mode = "hold"
        self.settings.max_timeout_ms = 50
        async with await self.post(headers=self.headers(request_id="timed-out")) as response:
            self.assertEqual(response.status, 502)
        await self.idle()
        receipt = await self.receipt("timed-out")
        self.assertEqual(receipt["execution"]["error_code"], "timeout")
        self.assertEqual(len(self.services.calls), 2)

    async def test_restart_preserves_progress_and_usage_and_does_not_restart_request(self):
        async with await self.post(headers=self.headers(request_id="interrupted")) as response:
            self.assertEqual(response.status, 200)
            await response.read()
        receipt = await self.receipt("interrupted")
        receipt["outcome"] = "unknown"
        receipt["execution"]["state"] = "running"
        with self.gateway.diagnostics.connection:
            self.gateway.diagnostics.connection.execute(
                "UPDATE requests SET receipt=?,reason='in_flight' WHERE request_id=?",
                (json.dumps(receipt), "interrupted"),
            )
        await self.runner.cleanup()
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.gateway = self.app[GATEWAY]
        restored = await self.receipt("interrupted")
        self.assertEqual(restored["execution"]["error_code"], "interrupted")
        self.assertEqual(restored["execution"]["state"], "unknown")
        self.assertEqual(restored["usage"], receipt["usage"])
        self.assertFalse(restored["usage_complete"])
        self.assertEqual((await self.cancel("interrupted"))[1]["upstream_outcome"], "unknown")
        async with await self.post(
            headers=self.headers(request_id="interrupted", turn="new-turn")
        ) as response:
            self.assertEqual(response.status, 409)
        self.assertEqual(len(self.services.calls), 1)


class NativeExecutionHttpTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = native_fixture.NativeHttpTests.asyncSetUp
    native_headers = native_fixture.NativeHttpTests.native_headers
    post_native = native_fixture.NativeHttpTests.post_native
    read_receipt = native_fixture.NativeHttpTests.read_receipt

    async def test_native_session_metadata_reaches_only_provider_header(self):
        headers = self.native_headers()
        headers["X-Tianshu-Provider-Session"] = "opaque-role-conversation"

        def integration(base, session):
            self.assertEqual(session, "opaque-role-conversation")
            return {
                "User-Agent": "tianshu-model-gateway/0.1.0",
                "x-opencode-session": "opaque-session-fixture",
            }

        with patch("tianshu_gateway.server.provider_headers", side_effect=integration):
            async with await self.post_native(
                native_fixture.native_body(stream=False), headers
            ) as response:
                self.assertEqual(response.status, 200)
                await response.read()
        sent = {name.lower(): value for name, value in self.services.native_calls[0][1].items()}
        self.assertEqual(sent["x-opencode-session"], "opaque-session-fixture")
        self.assertEqual(sent["user-agent"], "tianshu-model-gateway/0.1.0")
        self.assertNotIn("x-tianshu-provider-session", sent)

    async def test_native_stream_progress_and_api_cancel_do_not_fabricate_terminal(self):
        self.services.native_mode = "sse_hold"
        response = await self.post_native(headers=self.native_headers(request_id="native-live"))
        try:
            first = await response.content.readany()
            receipt = await self.read_receipt("native-live")
            self.assertEqual(receipt["execution"]["state"], "running")
            self.assertGreater(receipt["execution"]["forwarded_bytes"], 0)
            async with self.client.post(
                self.url + "/internal/v1/native-model-requests/native-live/cancel",
                json={},
                headers=self.native_headers(),
            ) as cancellation:
                self.assertEqual(cancellation.status, 200)
            try:
                rest = await response.read()
            except aiohttp.ClientError:
                rest = b""
            self.assertNotIn(b"response.completed", first + rest)
        finally:
            response.close()
        await asyncio.wait_for(self.services.native_disconnected.wait(), 1)
        receipt = await self.read_receipt("native-live")
        self.assertEqual(receipt["execution"]["state"], "cancelled")
        self.assertEqual(receipt["outcome"], "unknown")
        self.assertFalse(receipt["usage_complete"])
        self.assertEqual(len(self.services.native_calls), 1)

    async def test_native_inline_image_and_tool_followup_preserve_source_body(self):
        body = native_fixture.native_body(
            stream=False,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Describe this source"},
                        {
                            "type": "input_image",
                            "image_url": "https://example.invalid/original.png",
                            "detail": "low",
                        },
                    ],
                },
                {
                    "type": "function_call",
                    "call_id": "call-source",
                    "name": "read",
                    "arguments": "{}",
                },
                {
                    "type": "function_call_output",
                    "call_id": "call-source",
                    "output": "source result",
                },
            ],
        )
        async with await self.post_native(body) as response:
            self.assertEqual(response.status, 200)
            await response.read()
            request_id = response.headers["X-Request-ID"]
        self.assertEqual(json.loads(self.services.native_calls[0][0]), body)
        receipt = await self.read_receipt(request_id)
        self.assertEqual(receipt["execution"]["state"], "completed")
        self.assertTrue(receipt["execution"]["output_observed"])
        self.assertEqual(receipt["protocol"], "openai-responses")

    async def test_native_capabilities_and_explicit_unsupported_vision(self):
        provider = self.services.native_config["providers"][0]
        provider["verified_capabilities"] = ["text"]
        provider["unsupported_capabilities"] = ["vision"]
        async with self.client.get(
            self.url + "/internal/v1/native-model-capabilities", headers=self.native_headers()
        ) as response:
            self.assertEqual(response.status, 200)
            caps = await response.json()
        self.assertEqual(caps["protocol"], "openai-responses")
        self.assertEqual(caps["capabilities"]["vision"]["verification"], "unsupported")
        self.assertEqual(self.services.native_calls, [])
        body = native_fixture.native_body(
            input=[
                {
                    "role": "user",
                    "content": [{"type": "input_image", "image_url": "https://example.invalid/a"}],
                }
            ]
        )
        async with await self.post_native(body) as response:
            self.assertEqual(response.status, 422)
            error = await response.json()
        self.gateway.contracts.validate("native#error", error)
        self.assertEqual(error["code"], "capability_unsupported")
        self.assertEqual(self.services.native_calls, [])

    async def test_native_explicit_cancel_preserves_identity_and_unknown_outcome(self):
        self.services.native_mode = "hold"
        task = asyncio.create_task(
            self.post_native(headers=self.native_headers(request_id="native-cancel"))
        )
        self.addCleanup(task.cancel)
        await asyncio.wait_for(self.services.native_started.wait(), 1)
        async with self.client.post(
            self.url + "/internal/v1/native-model-requests/native-cancel/cancel",
            json={},
            headers=self.native_headers(service="other-fixture"),
        ) as response:
            self.assertEqual(response.status, 404)
        async with self.client.post(
            self.url + "/internal/v1/native-model-requests/native-cancel/cancel",
            json={},
            headers=self.native_headers(),
        ) as response:
            self.assertEqual(response.status, 200)
            result = await response.json()
        self.assertEqual(result["upstream_outcome"], "unknown")
        async with await task as response:
            self.assertEqual(response.status, 502)
        await asyncio.wait_for(self.services.native_disconnected.wait(), 1)
        receipt = await self.read_receipt("native-cancel")
        self.assertEqual(receipt["execution"]["state"], "cancelled")
        self.assertTrue(receipt["execution"]["cancel_requested"])
        self.assertEqual(len(self.services.native_calls), 1)


class QueuedExecutionHttpTests(ScheduledFixtures, unittest.IsolatedAsyncioTestCase):
    global_limit = provider_limit = 2
    reserve = 1

    async def test_cancel_queued_chat_and_native_is_durable_and_never_started(self):
        self.services.mode = "hold"
        holders = [
            asyncio.create_task(self.chat(self.chat_headers(request_id=f"holder-{i}")))
            for i in range(2)
        ]
        self.addAsyncCleanup(self.release_held, holders)
        await self.services.wait_calls(2)
        for native in (False, True):
            request_id = "queued-native" if native else "queued-chat"
            task = asyncio.create_task(
                self.native(self.native_headers(request_id=request_id))
                if native
                else self.chat(self.chat_headers(request_id=request_id))
            )
            self.addCleanup(task.cancel)
            await self.wait_for(lambda: self.scheduler.waiting == 1)
            prefix = (
                "/internal/v1/native-model-requests/" if native else "/internal/v1/model-requests/"
            )
            headers = self.native_headers() if native else self.chat_headers()
            async with self.client.post(
                self.url + prefix + request_id + "/cancel", json={}, headers=headers
            ) as response:
                self.assertEqual(response.status, 200)
                result = await response.json()
            self.assertEqual(result["upstream_outcome"], "not_started")
            async with await task as response:
                self.assertEqual(response.status, 409)
                self.assertEqual((await response.json())["code"], "request_cancelled")
            async with self.client.get(self.url + prefix + request_id, headers=headers) as response:
                self.assertEqual(response.status, 200)
                receipt = await response.json()
            self.assertEqual(receipt["execution"]["state"], "cancelled")
            self.assertFalse(receipt["execution"]["upstream_started"])
            self.assertEqual(receipt["execution"]["received_bytes"], 0)
            self.assertEqual(self.services.native_calls, [])
            self.assertEqual(len(self.services.calls), 2)
        await self.release_held(holders)
        for task in holders:
            response = await task
            await response.read()
            response.close()


if __name__ == "__main__":
    unittest.main()
