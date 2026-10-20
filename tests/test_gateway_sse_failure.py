"""Typed SSE failures keep provider bytes and the existing owned failure receipt."""

import asyncio
import json
import unittest

import aiohttp

import test_gateway_http as fixture
from gateway_fixtures import SECRETS
from tianshu_gateway.routing import StreamObserver
from tianshu_gateway.usage import build_report


def event(value, newline=b"\n"):
    return b"data: " + json.dumps(value, ensure_ascii=False).encode() + newline * 2


def rate_limit(newline=b"\n"):
    return event({"error": {"type": "rate_limit_error", "code": None}}, newline)


def content(text):
    return event({"choices": [{"index": 0, "delta": {"content": text}, "finish_reason": None}]})


class TypedFailureObserverTests(unittest.TestCase):
    def test_complete_typed_error_needs_no_choices_or_success_terminal(self):
        for newline in (b"\n", b"\r\n", b"\r"):
            for suffix in (b"", b"data: [DONE]" + newline * 2):
                with self.subTest(newline=newline, done=bool(suffix)):
                    observer = StreamObserver()
                    for byte in rate_limit(newline) + suffix:
                        observer.feed(bytes([byte]))
                    observer.end()
                    self.assertTrue(observer.rate_limit_failure)
                    self.assertFalse(observer.complete)
                    self.assertEqual(observer.finished, set())
                    self.assertEqual(observer.error_types, {"rate_limit_error"})

    def test_typed_failure_does_not_assert_that_no_output_was_observed(self):
        for prefix in (content(" \n"), content("\u200b"), content("short pending text")):
            with self.subTest(prefix_size=len(prefix)):
                observer = StreamObserver()
                observer.feed(prefix + rate_limit())
                observer.end()
                self.assertTrue(observer.rate_limit_failure)
                self.assertIsNotNone(observer.first_output_ms)

    def test_other_or_malformed_errors_cannot_be_overridden_by_one_typed_frame(self):
        ambiguous = (
            event({"error": {"message": "rate_limit_error"}}),
            event({"error": {"type": "Rate_Limit_Error"}}),
            event({"error": "rate_limit_error"}),
            event({"error": {"code": "rate_limit_error"}}),
            b'event:error\ndata:{"message":"provider failed"}\n\n',
            b'data: {"error":{"type":"rate_limit_error"},"choices":null}\n\n',
            event({"error": {"type": "rate_limit_error"}, "choices": []}),
            event({"error": {"type": "rate_limit_error"}, "content": ""}),
            event({"error": {"type": "rate_limit_error"}, "tool_calls": []}),
            event({"error": {"type": "rate_limit_error"}, "finish_reason": None}),
            b"event:error\ndata:[DONE]\n\n",
            b"data: {invalid}\n\n",
            b'data: {"error":{"type":"rate_limit_error"}}',
        )
        for tail in ambiguous:
            with self.subTest(tail_size=len(tail)):
                observer = StreamObserver()
                observer.feed(rate_limit() + tail)
                observer.end()
                self.assertFalse(observer.rate_limit_failure)
                self.assertFalse(observer.complete)
                self.assertLessEqual(len(observer.error_types), 2)

    def test_error_after_done_stays_invalid_and_duplicate_typed_errors_stay_bounded(self):
        observer = StreamObserver()
        observer.feed(b"data: [DONE]\n\n" + rate_limit())
        observer.end()
        self.assertTrue(observer.invalid)
        self.assertFalse(observer.rate_limit_failure)
        observer = StreamObserver()
        observer.feed(rate_limit() * 100)
        observer.end()
        self.assertTrue(observer.rate_limit_failure)
        self.assertEqual(observer.error_types, {"rate_limit_error"})


class TypedFailureHttpTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixture.GatewayHttpTests.asyncSetUp
    headers = fixture.GatewayHttpTests.headers
    post = fixture.GatewayHttpTests.post
    receipt = fixture.GatewayHttpTests.receipt
    idle = fixture.GatewayHttpTests.idle

    async def test_exact_typed_failures_preserve_http_bytes_and_owned_terminal_receipts(self):
        self.services.mode = "stream"
        cases = [
            rate_limit(),
            rate_limit() + b"data: [DONE]\n\n",
            content(" \n") + rate_limit(),
            content("\u200b") + rate_limit(),
            content("short pending text") + rate_limit(),
            b"event:error\n" + rate_limit(),
        ]
        for body in cases:
            with self.subTest(size=len(body)):
                self.services.stream = body
                async with await self.post({**self.body, "stream": True}) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(await response.read(), body)
                    request_id = response.headers["X-Request-ID"]
                await self.idle()
                receipt = await self.receipt(request_id)
                self.gateway.contracts.validate("model#route_receipt", receipt)
                self.assertEqual(receipt["request_id"], request_id)
                self.assertEqual(receipt["config_version"], 7)
                self.assertEqual(receipt["caller_service"], "companion")
                self.assertEqual(receipt["outcome"], "failed")
                self.assertEqual(receipt["execution"]["state"], "failed")
                self.assertEqual(receipt["execution"]["error_code"], "upstream_http_error")
                self.assertTrue(receipt["execution"]["upstream_started"])
                self.assertFalse(receipt["execution"]["cancel_requested"])
                self.assertEqual(receipt["execution"]["finish_reasons"], [])
                self.assertEqual(receipt["execution"]["forwarded_bytes"], len(body))
                self.assertFalse(receipt["usage_complete"])
                self.assertFalse(receipt["fallback_used"])
                row = self.gateway.diagnostics.connection.execute(
                    "SELECT reason,upstream_status FROM requests WHERE service=? AND request_id=?",
                    ("companion", request_id),
                ).fetchone()
                self.assertEqual(tuple(row), ("response_failed", 200))
                async with self.client.get(
                    self.url + "/internal/v1/model-requests/" + request_id,
                    headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_OTHER"]},
                ) as other:
                    self.assertEqual(other.status, 404)
        self.assertEqual(len(self.services.calls), len(cases))
        report = build_report(
            self.gateway.diagnostics.connection,
            "chat",
            {"service": "companion"},
            {},
        )
        self.assertEqual(report["counts"]["failed"], len(cases))
        self.assertEqual(report["counts"]["unknown"], 0)

    async def test_one_byte_crlf_error_stream_keeps_utf8_and_no_synthetic_done(self):
        self.services.mode = "one_byte"
        raw = content("合成片段") + rate_limit(b"\r\n")
        self.services.stream = raw
        async with await self.post({**self.body, "stream": True}) as response:
            actual = await response.read()
            self.assertEqual(actual, raw)
            self.assertNotIn(b"[DONE]", actual)
            request_id = response.headers["X-Request-ID"]
        await self.idle()
        self.assertEqual((await self.receipt(request_id))["outcome"], "failed")
        self.assertEqual(len(self.services.calls), 1)

    async def test_mixed_invalid_unterminated_and_secret_errors_stay_unknown(self):
        self.services.mode = "stream"
        tails = (
            event({"error": {"message": "provider failed"}}),
            event({"error": {"type": "different_error"}}),
            b'data: {"choices":null}\n\n',
            event({"error": {"type": "rate_limit_error"}, "choices": []}),
            event({"error": {"type": "rate_limit_error"}, "content": ""}),
            event({"error": {"type": "rate_limit_error"}, "tool_calls": []}),
            event({"error": {"type": "rate_limit_error"}, "finish_reason": None}),
            b"event:error\ndata:[DONE]\n\n",
            b"data: unfinished",
            b"data: [DONE]\n\n" + rate_limit(),
            event(
                {"error": {"type": "rate_limit_error", "message": SECRETS["TS041_TEST_UPSTREAM"]}}
            ),
        )
        for tail in tails:
            with self.subTest(size=len(tail)):
                self.services.stream = rate_limit() + tail
                actual = bytearray()
                async with await self.post({**self.body, "stream": True}) as response:
                    request_id = response.headers["X-Request-ID"]
                    try:
                        async for chunk in response.content.iter_any():
                            actual.extend(chunk)
                    except aiohttp.ClientPayloadError:
                        pass
                await self.idle()
                receipt = await self.receipt(request_id)
                self.assertEqual(receipt["outcome"], "unknown")
                self.assertEqual(receipt["execution"]["state"], "unknown")
                self.assertNotIn(SECRETS["TS041_TEST_UPSTREAM"].encode(), actual)
                self.assertFalse(receipt["usage_complete"])
        self.assertEqual(len(self.services.calls), len(tails))

    async def test_transport_drop_after_typed_error_never_claims_a_terminal_failure(self):
        self.services.mode = "drop"
        self.services.stream = rate_limit()
        async with await self.post({**self.body, "stream": True}) as response:
            request_id = response.headers["X-Request-ID"]
            with self.assertRaises(aiohttp.ClientPayloadError):
                await response.read()
        await self.idle()
        receipt = await self.receipt(request_id)
        self.assertEqual(receipt["outcome"], "unknown")
        self.assertEqual(receipt["execution"]["state"], "unknown")
        self.assertEqual(len(self.services.calls), 1)

    async def test_cancel_after_typed_error_stays_cancelled_unknown(self):
        self.services.mode = "sse_hold"
        prefix = rate_limit() + b":" + b"x" * 400 + b"\n\n"
        self.services.stream = prefix
        response = await self.post(
            {**self.body, "stream": True}, headers=self.headers(request_id="typed-cancel")
        )
        try:
            await asyncio.wait_for(response.content.readany(), 1)
            running = await self.receipt("typed-cancel")
            self.assertEqual(running["execution"]["state"], "running")
            async with self.client.post(
                self.url + "/internal/v1/model-requests/typed-cancel/cancel",
                json={},
                headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]},
            ) as cancelled:
                self.assertEqual(cancelled.status, 200)
                self.assertEqual((await cancelled.json())["state"], "requested")
            with self.assertRaises(aiohttp.ClientPayloadError):
                await response.read()
        finally:
            response.close()
        await self.idle()
        receipt = await self.receipt("typed-cancel")
        self.assertEqual(receipt["outcome"], "unknown")
        self.assertEqual(receipt["execution"]["state"], "cancelled")
        self.assertTrue(receipt["execution"]["cancel_requested"])
        self.assertEqual(len(self.services.calls), 1)
