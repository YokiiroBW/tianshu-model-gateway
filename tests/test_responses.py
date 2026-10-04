"""Offline/loopback-only Responses transport tests; the gateway route is covered elsewhere."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web

from gateway_fixtures import start_http, registration
from tianshu_gateway.config import RegisteredTargets
from tianshu_gateway.contracts import Rejected
from tianshu_gateway.diagnostics import Diagnostics
from tianshu_gateway.responses import ResponsesObserver, send_responses, validate_request

TOKEN = "fixture-responses-token-042"
SCOPE = ("model-protocol/v1", "fixture-principal", "fixture-service", "credential-namespace-x")
NATIVE = {
    "object": "response",
    "id": "resp_fixture",
    "status": "completed",
    "error": None,
    "output": [
        {
            "type": "function_call",
            "call_id": "call_x",
            "name": "read",
            "arguments": '{"file_id":"business-data"}',
        }
    ],
    "usage": {
        "input_tokens": 0,
        "output_tokens": 8,
        "output_tokens_details": {"reasoning_tokens": 3},
        "vendor": {"x": None},
    },
}


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def event(kind, **fields):
    return (
        b"event: " + kind.encode() + b"\r\ndata: " + encode({"type": kind, **fields}) + b"\r\n\r\n"
    )


STREAM = (
    b": keepalive\r\n\r\n"
    + event("response.created", response={**NATIVE, "status": "in_progress", "usage": None})
    + event("response.output_text.delta", delta="你好", sequence_number=1)
    + event("response.function_call_arguments.delta", delta='{"q":', sequence_number=2)
    + event("response.vendor_future", unknown={"unchanged": True})
    + event("response.completed", response=NATIVE)
)
BODY = {
    "model": "exact-fixture-model",
    "instructions": "原样指令",
    "input": "hello",
    "tools": [
        {
            "type": "function",
            "name": "read",
            "parameters": {"type": "object", "properties": {"file_id": {"type": "string"}}},
        }
    ],
    "reasoning": {"effort": "medium", "vendor": False},
    "store": False,
    "vendor_extension": {"value": None},
}


class ResponsesObserverTests(unittest.TestCase):
    def test_every_split_and_single_byte_preserve_terminal_and_usage(self):
        for i in range(len(STREAM) + 1):
            observer = ResponsesObserver()
            observer.feed(STREAM[:i])
            observer.feed(STREAM[i:])
            observer.end()
            self.assertTrue(observer.complete, i)
            self.assertEqual(observer.native_usage, NATIVE["usage"])
        observer = ResponsesObserver()
        for byte in STREAM:
            observer.feed(bytes([byte]))
        observer.end()
        self.assertTrue(observer.complete)

    def test_native_failed_incomplete_error_and_no_chat_done(self):
        for status in ("completed", "failed", "incomplete"):
            observer = ResponsesObserver()
            observer.feed(event("response." + status, response={**NATIVE, "status": status}))
            self.assertTrue(observer.complete)
            self.assertEqual(observer.status, status)
        observer = ResponsesObserver()
        observer.feed(event("error", code="server_error", message="fixture"))
        self.assertTrue(observer.complete)
        self.assertEqual(observer.status, "failed")
        for raw in (
            b"data: [DONE]\n\n",
            STREAM[:-2],
            STREAM + event("response.output_text.delta", delta="x"),
            b'event: error\ndata: {"type":"response.completed"}\n\n',
            event("response.completed", response={**NATIVE, "status": "in_progress"}),
            event("response.created", response=NATIVE),
        ):
            observer = ResponsesObserver()
            observer.feed(raw)
            observer.end()
            self.assertFalse(observer.complete)

    def test_bounded_observation_id_conflicts_and_lone_cr(self):
        observer = ResponsesObserver(limit=32)
        observer.feed(b"data: " + b"x" * 100)
        self.assertTrue(observer.invalid)
        self.assertLessEqual(len(observer.buffer), 32)
        observer = ResponsesObserver()
        observer.feed(event("response.created", response=NATIVE))
        observer.feed(event("response.completed", response={**NATIVE, "id": "resp_other"}))
        self.assertFalse(observer.complete)
        observer = ResponsesObserver()
        observer.feed(STREAM.replace(b"\r\n", b"\r"))
        observer.end()
        self.assertTrue(observer.complete)

    def test_native_request_fidelity_and_fail_closed_state_scope(self):
        self.assertEqual(validate_request(encode(BODY)), BODY)
        for key in ("previous_response_id", "conversation", "prompt", "response_id"):
            with self.subTest(key=key), self.assertRaises(Rejected):
                validate_request(encode({**BODY, key: "state-fixture"}))
        cases = [
            {"prompt_cache_options": {"comparison_response_id": "resp_x"}},
            {"input": [{"id": "item_x"}]},
            {"background": True},
            {"tools": [{"type": "file_search", "vector_store_ids": ["vs_x"]}]},
            {"input": [{"type": "item_reference", "id": "item_x"}]},
            {"input": [{"role": "user", "content": [{"type": "input_file", "file_id": "file_x"}]}]},
            {"input": [{"type": "input_audio", "data": "unsupported"}]},
            {"stream": 1},
            {"store": None},
            {"model": ""},
            {"input": {}},
        ]
        for update in cases:
            with self.subTest(update=update), self.assertRaises(Rejected):
                validate_request(encode({**BODY, **update}))
        with self.assertRaises(Rejected):
            validate_request(b'{"model":"a","model":"b","input":"x"}')
        inline = {
            **BODY,
            "input": [
                {"type": "reasoning", "id": "rs_x", "encrypted_content": "opaque", "summary": []},
                {"type": "function_call", "call_id": "call_x", "name": "read", "arguments": "{}"},
                {
                    "type": "function_call_output",
                    "call_id": "call_x",
                    "output": '{"file_id":"local"}',
                },
            ],
        }
        self.assertEqual(validate_request(encode(inline)), inline)


class ResponsesHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.diagnostics = Diagnostics(str(Path(self.temp.name) / "responses.sqlite"))
        self.addCleanup(self.diagnostics.close)
        self.calls = []
        self.mode = "json"
        self.raw_response = encode(NATIVE)
        self.stream = STREAM
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.disconnected = asyncio.Event()
        self.status = 429
        app = web.Application()
        app.router.add_post("/v1/responses", self.upstream)
        runner, self.base = await start_http(app)
        self.addAsyncCleanup(runner.cleanup)
        self.targets = RegisteredTargets([registration(self.base + "/v1")])
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(resolver=self.targets),
            trust_env=False,
            auto_decompress=False,
            cookie_jar=aiohttp.DummyCookieJar(),
        )
        self.addAsyncCleanup(self.session.close)
        self.headers = []
        self.chunks = []
        self.counter = 0

    async def upstream(self, request):
        self.calls.append((await request.read(), dict(request.headers), request.path))
        self.started.set()
        try:
            if self.mode == "hold":
                await self.release.wait()
            if self.mode == "error":
                return web.Response(
                    status=self.status,
                    body=self.raw_response,
                    content_type="application/json",
                    headers={"Location": self.base + "/v1/responses", "Set-Cookie": "x=y"},
                )
            if self.mode in {"stream", "one_byte", "drop", "sse_hold"}:
                response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
                await response.prepare(request)
                if self.mode == "one_byte":
                    for byte in self.stream:
                        await response.write(bytes([byte]))
                        await asyncio.sleep(0)
                elif self.mode == "sse_hold":
                    await response.write(self.stream[:350])
                    await self.release.wait()
                else:
                    await response.write(self.stream)
                if self.mode == "drop":
                    request.transport.abort()
                else:
                    await response.write_eof()
                return response
            return web.Response(
                body=self.raw_response,
                content_type="application/json",
                headers={"x-request-id": "upstream-042"},
            )
        except asyncio.CancelledError:
            self.disconnected.set()
            raise
        except (ConnectionError, aiohttp.ClientConnectionError):
            self.disconnected.set()
            return web.Response(status=499)

    async def start_response(self, status, content_type):
        self.headers.append((status, content_type))

    async def write(self, chunk):
        self.chunks.append(chunk)

    async def send(self, body=None, **kwargs):
        self.counter += 1
        self.request_id = f"fixture-{self.counter}"
        payload = encode(BODY if body is None else body)
        receipt = {
            "contract": "model-protocol/v1",
            "caller_service": "fixture-service",
            "principal_id": "fixture-principal",
            "credential_namespace": "credential-namespace-x",
            "request_id": self.request_id,
            "protocol": "openai-responses",
            "native_config_version": 7,
            "resolved_model": "exact-fixture-model",
            "outcome": "unknown",
        }
        args = dict(
            payload=payload,
            base_url=self.base + "/v1",
            credential=TOKEN,
            receipt=receipt,
            start_response=self.start_response,
            write=self.write,
            timeout=2,
        )
        args.update(kwargs)
        if "receipt" not in kwargs:
            # The route records the receipt before the wire attempt; revocation is native.
            self.diagnostics.native_begin(receipt, None)
        await send_responses(self.session, self.targets, self.diagnostics, **args)

    def saved(self):
        return self.diagnostics.native_get(SCOPE, self.request_id)

    def reason(self):
        return self.diagnostics.connection.execute(
            "SELECT reason FROM native_requests WHERE request_id=?", (self.request_id,)
        ).fetchone()[0]

    async def test_http_raw_request_response_exact_and_no_parameter_defaults(self):
        raw = encode(BODY)[:-1] + b', "precise":0.12345678901234567890123456789}'
        await self.send(payload=raw)
        self.assertEqual(self.calls[0][0], raw)
        self.assertEqual(self.calls[0][2], "/v1/responses")
        self.assertEqual(self.calls[0][1]["Authorization"], "Bearer " + TOKEN)
        self.assertEqual(b"".join(self.chunks), self.raw_response)
        self.assertEqual(self.saved()["usage"], {"input_tokens": 0, "output_tokens": 8})
        self.assertTrue(self.saved()["usage_complete"])
        self.assertEqual(self.saved()["outcome"], "succeeded")
        self.assertEqual(self.saved()["native_usage"], NATIVE["usage"])
        await self.send({"model": "exact-fixture-model", "input": "x"})
        self.assertEqual(
            json.loads(self.calls[-1][0]), {"model": "exact-fixture-model", "input": "x"}
        )

    async def test_http_single_byte_stream_exact_with_native_terminal(self):
        self.mode = "one_byte"
        await self.send({**BODY, "stream": True})
        self.assertEqual(b"".join(self.chunks), STREAM)
        self.assertEqual(self.headers, [(200, "text/event-stream")])
        self.assertTrue(self.saved()["usage_complete"])
        self.assertEqual(self.saved()["outcome"], "succeeded")

    async def test_http_native_errors_are_preserved_and_never_retried(self):
        self.mode = "error"
        self.raw_response = b'{"error":{"code":"rate_limit_exceeded","message":"fixture"}}'
        for status in (400, 429, 503, 307):
            self.status = status
            await self.send()
            self.assertEqual(self.chunks[-1], self.raw_response)
            self.assertEqual(self.headers[-1][0], status)
            self.assertEqual(
                self.saved()["outcome"], "failed" if status < 500 and status >= 400 else "unknown"
            )
            self.assertIsNone(self.saved()["usage"])
            self.assertFalse(self.saved()["usage_complete"])
        self.assertEqual(len(self.calls), 4)

    async def test_http_sse_failed_incomplete_error_and_missing_usage(self):
        self.mode = "stream"
        for status, outcome in (
            ("failed", "failed"),
            ("incomplete", "unknown"),
            ("completed", "succeeded"),
        ):
            self.stream = event(
                "response." + status, response={**NATIVE, "status": status, "usage": None}
            )
            await self.send({**BODY, "stream": True})
            self.assertEqual(self.saved()["outcome"], outcome)
            self.assertIsNone(self.saved()["usage"])
            self.assertFalse(self.saved()["usage_complete"])
        self.stream = event("error", code="server_error", message="fixture")
        await self.send({**BODY, "stream": True})
        self.assertEqual(self.saved()["outcome"], "failed")

    async def test_unparseable_body_fails_closed_and_unobservable_body_is_unknown(self):
        self.raw_response = encode({**NATIVE, "usage": {"input_tokens": 0, "output_tokens": True}})
        await self.send()
        self.assertEqual(self.saved()["usage"], {"input_tokens": 0})
        self.assertFalse(self.saved()["usage_complete"])
        # A body that cannot even be parsed is a wire-level failure: the attempt fails and
        # the caller gets a local error instead of unverified bytes.
        self.raw_response = b'{"id":'
        with self.assertRaises(Rejected):
            await self.send()
        self.assertEqual(self.saved()["outcome"], "unknown")
        # A body received in full but not confirmable as a native response is delivered
        # unchanged, with an unknown (never successful) recorded result.
        for raw in (b"{}", b"[]", encode({**NATIVE, "status": "in_progress"})):
            with self.subTest(raw=raw):
                self.raw_response = raw
                self.chunks.clear()
                await self.send()
                self.assertEqual(b"".join(self.chunks), raw)
                self.assertEqual(self.saved()["outcome"], "unknown")
                self.assertFalse(self.saved()["usage_complete"])

    async def test_closed_stream_with_unobserved_terminal_is_delivered(self):
        self.mode = "stream"
        self.stream = STREAM[:-2]
        await self.send({**BODY, "stream": True})
        # The upstream closed by itself, so the side-channel observer may not truncate it.
        self.assertEqual(b"".join(self.chunks), self.stream)
        self.assertEqual(self.saved()["outcome"], "unknown")
        self.assertFalse(self.saved()["usage_complete"])
        self.assertEqual(len(self.calls), 1)

    async def test_real_upstream_drop_still_fails_the_attempt(self):
        self.mode = "drop"
        self.stream = STREAM
        with self.assertRaises(Rejected):
            await self.send({**BODY, "stream": True})
        self.assertEqual(self.saved()["outcome"], "unknown")
        self.assertFalse(self.saved()["usage_complete"])
        self.assertEqual(len(self.calls), 1)

    async def test_timeout_cancellation_and_sink_failure_close_upstream(self):
        self.mode = "sse_hold"
        with self.assertRaises(Rejected):
            await self.send({**BODY, "stream": True}, timeout=0.05)
        self.assertEqual(self.reason(), "timeout_unknown")
        await asyncio.wait_for(self.disconnected.wait(), 1)
        self.disconnected.clear()
        self.started.clear()
        task = asyncio.create_task(self.send({**BODY, "stream": True}))
        await self.started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.reason(), "cancelled_unknown")
        await asyncio.wait_for(self.disconnected.wait(), 1)
        self.assertEqual(self.saved()["outcome"], "unknown")
        self.assertEqual(len(self.calls), 2)

    async def test_sink_backpressure_is_awaited_and_disconnect_not_replayed(self):
        self.mode = "stream"
        entered, release = asyncio.Event(), asyncio.Event()

        async def blocked(chunk):
            entered.set()
            await release.wait()
            raise ConnectionResetError("fixture downstream closed")

        task = asyncio.create_task(self.send({**BODY, "stream": True}, write=blocked))
        await asyncio.wait_for(entered.wait(), 1)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(Rejected):
            await task
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.saved()["outcome"], "unknown")
        self.assertFalse(self.saved()["usage_complete"])

    async def test_reflected_secret_http_sse_and_diagnostics_are_not_leaked(self):
        self.mode = "one_byte"
        self.stream = event("response.output_text.delta", delta=TOKEN) + event(
            "response.completed", response=NATIVE
        )
        with self.assertRaises(Rejected):
            await self.send({**BODY, "stream": True})
        self.assertNotIn(TOKEN.encode(), b"".join(self.chunks))
        self.assertNotIn(TOKEN, json.dumps(self.saved()))
        self.mode = "error"
        self.raw_response = encode({"error": {"message": TOKEN}})
        with self.assertRaises(Rejected):
            await self.send()
        self.assertNotIn(TOKEN.encode(), b"".join(self.chunks))

    async def test_references_revocation_limits_and_route_mismatch_prevent_send(self):
        for body in ({**BODY, "previous_response_id": "resp_x"}, {**BODY, "background": True}):
            with self.assertRaises(Rejected):
                await self.send(body)
        with self.assertRaises(Rejected):
            await self.send(max_request_bytes=10)
        with self.assertRaises(Rejected):
            await self.send(base_url=self.base + "/unregistered")
        with self.assertRaises(Rejected):
            await self.send(
                receipt={
                    "contract": "model-protocol/v1",
                    "caller_service": "fixture-service",
                    "principal_id": "fixture-principal",
                    "credential_namespace": "credential-namespace-x",
                    "request_id": "fixture-chat",
                    "protocol": "openai-chat-completions",
                    "native_config_version": 7,
                    "resolved_model": BODY["model"],
                }
            )
        with self.assertRaises(Rejected):
            # A Chat-shaped receipt (legacy version field only) never reaches the wire.
            await self.send(
                receipt={
                    "caller_service": "fixture-service",
                    "request_id": "fixture-legacy",
                    "protocol": "openai-responses",
                    "config_version": 7,
                    "resolved_model": BODY["model"],
                }
            )
        self.diagnostics.native_revoke(SCOPE, 7)
        with self.assertRaises(Rejected):
            await self.send()
        self.assertEqual(self.calls, [])

    async def test_actual_downstream_http_disconnect_cancels_internal_transport(self):
        self.mode = "sse_hold"
        finished = asyncio.Event()

        async def bridge(request):
            response = None

            async def begin(status, content_type):
                nonlocal response
                response = web.StreamResponse(status=status, headers={"Content-Type": content_type})
                await response.prepare(request)

            async def write(chunk):
                await response.write(chunk)

            try:
                await self.send({**BODY, "stream": True}, start_response=begin, write=write)
                await response.write_eof()
                return response
            except Rejected:
                request.transport.abort()
                return response
            finally:
                finished.set()

        app = web.Application()
        # Test-only bridge; production create_app remains unchanged and returns 501.
        app.router.add_post("/fixture-responses", bridge)
        runner, url = await start_http(app)
        self.addAsyncCleanup(runner.cleanup)
        async with aiohttp.ClientSession() as client:
            response = await client.post(url + "/fixture-responses")
            await response.content.readany()
            response.close()
            await asyncio.wait_for(finished.wait(), 1)
        await asyncio.wait_for(self.disconnected.wait(), 1)
        self.assertEqual(self.reason(), "cancelled_unknown")
        self.assertEqual(self.saved()["outcome"], "unknown")
        self.assertEqual(len(self.calls), 1)

    async def test_response_limit_zero_usage_and_diagnostic_failure_are_honest(self):
        self.raw_response = encode({**NATIVE, "usage": {"input_tokens": 0, "output_tokens": 0}})
        await self.send()
        self.assertEqual(self.saved()["usage"], {"input_tokens": 0, "output_tokens": 0})
        self.assertTrue(self.saved()["usage_complete"])
        with self.assertRaises(Rejected):
            await self.send(max_response_bytes=10)
        self.assertEqual(self.saved()["outcome"], "unknown")
        with patch.object(
            self.diagnostics, "native_finish", side_effect=OSError("fixture disk error")
        ):
            with self.assertRaises(OSError):
                await self.send()
        self.assertEqual(self.saved()["outcome"], "unknown")
        self.assertEqual(self.reason(), "in_flight")


if __name__ == "__main__":
    unittest.main()
