"""Safe wait hints through real loopback Chat and native Responses routes."""

import json
import os
import unittest
from unittest.mock import patch

import test_gateway_http as chat_fixture
import test_responses_native as native_fixture
from gateway_fixtures import SECRETS
from tianshu_gateway.routing import retry_after_headers

HTTP_DATE = "Wed, 21 Oct 2015 07:28:00 GMT"
NATIVE_ERROR = b'{"error": {"code": "rate_limit_exceeded", "message": "fixture"}}'


class RetryAfterBoundaryTests(unittest.TestCase):
    def test_integer_and_explicit_timezone_dates_are_bounded_and_preserved(self):
        for value in (
            "0",
            "00120",
            "9" * 128,
            HTTP_DATE,
            "Wed, 21 Oct 2015 07:28:00 +0800",
            "Wednesday, 21-Oct-15 07:28:00 GMT",
        ):
            for status in (429, 503):
                with self.subTest(value=value, status=status):
                    self.assertEqual(retry_after_headers(status, [value]), {"Retry-After": value})
        self.assertEqual(retry_after_headers(429, [" 120 "]), {"Retry-After": "120"})

    def test_malformed_nonfinite_unbounded_ambiguous_and_control_values_are_omitted(self):
        for value in (
            "",
            " ",
            "-1",
            "+1",
            "0.5",
            "1e3",
            "NaN",
            "Infinity",
            "1e999",
            "\u0661",
            "9" * 129,
            "1\r\nSet-Cookie: secret",
            "1\n",
            "1\r",
            "1\x00",
            "1\x7f",
            "\t1",
            "1, 2",
            "Wed, 21 Oct 2015 07:28:00",
            "Wed Oct 21 07:28:00 2015",
            "Wed, 32 Oct 2015 07:28:00 GMT",
            "Wed, 21 Oct 2015 25:28:00 GMT",
            "Wed, 21 Oct 2015 07:28:00 +0060",
            HTTP_DATE + " ignored",
            None,
        ):
            with self.subTest(value=value):
                self.assertEqual(retry_after_headers(429, [value]), {})
        for values in ([], ["1", "2"], ["1", "1"]):
            self.assertEqual(retry_after_headers(503, values), {})
        for status in (200, 201, 307, 400, 401, 500, 502, 504):
            self.assertEqual(retry_after_headers(status, ["120"]), {})

    def test_even_valid_numeric_or_date_headers_cannot_reflect_known_secrets(self):
        for value, secret in (("1234567890123456", "1234567890123456"), (HTTP_DATE, "2015")):
            with self.subTest(value=value):
                self.assertEqual(retry_after_headers(429, [value], [secret]), {})


class RetryAfterChatHttpTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = chat_fixture.GatewayHttpTests.asyncSetUp
    headers = chat_fixture.GatewayHttpTests.headers
    post = chat_fixture.GatewayHttpTests.post
    receipt = chat_fixture.GatewayHttpTests.receipt
    idle = chat_fixture.GatewayHttpTests.idle

    async def test_wait_hints_preserve_owned_terminal_receipts_without_retry(self):
        self.services.mode = "error"
        for status, value in ((429, "120"), (503, HTTP_DATE)):
            with self.subTest(status=status):
                self.services.http_status = status
                self.services.response_headers = {
                    "Retry-After": value,
                    "X-Provider-Private": SECRETS["TS041_TEST_UPSTREAM"],
                }
                async with await self.post() as response:
                    self.assertEqual(response.status, status)
                    self.assertEqual(response.headers["Retry-After"], value)
                    self.assertEqual(response.headers["Cache-Control"], "no-store")
                    self.assertNotIn("X-Provider-Private", response.headers)
                    self.assertNotIn("Set-Cookie", response.headers)
                    self.assertNotIn("Location", response.headers)
                    request_id = response.headers["X-Request-ID"]
                    raw = await response.text()
                route = await self.receipt(request_id)
                self.assertEqual(route["outcome"], "failed" if status == 429 else "unknown")
                self.assertEqual(route["execution"]["error_code"], "upstream_http_error")
                self.assertTrue(route["execution"]["upstream_started"])
                self.assertFalse(route["execution"]["cancel_requested"])
                self.assertFalse(route["usage_complete"])
                self.assertNotIn("retry_after", route)
                self.assertNotIn("Retry-After", json.dumps(route))
                for secret in SECRETS.values():
                    self.assertNotIn(secret, raw + json.dumps(route))
                await self.receipt(request_id, token="OTHER", expected=404)
        self.assertEqual(len(self.services.calls), 2)

    async def test_invalid_secret_and_other_status_hints_are_not_forwarded(self):
        self.services.mode = "error"
        for status, value in (
            (429, "NaN"),
            (503, "9" * 129),
            (429, "0.5"),
            (429, SECRETS["TS041_TEST_UPSTREAM"]),
            (500, "120"),
        ):
            with self.subTest(status=status, value=value):
                self.services.http_status = status
                self.services.response_headers = {"Retry-After": value}
                async with await self.post() as response:
                    self.assertEqual(response.status, status)
                    self.assertNotIn("Retry-After", response.headers)
                    self.assertNotIn(SECRETS["TS041_TEST_UPSTREAM"], await response.text())
        self.assertEqual(len(self.services.calls), 5)

    async def test_syntactically_valid_secret_hint_is_blocked_by_real_route(self):
        self.services.mode = "error"
        self.services.response_headers = {"Retry-After": "1234567890123456"}
        self.gateway.secrets.references["secret-ref:fixture/retry-hint"] = "TS261_TEST_HINT"
        with patch.dict(os.environ, {"TS261_TEST_HINT": "1234567890123456"}):
            async with await self.post() as response:
                self.assertEqual(response.status, 429)
                self.assertNotIn("Retry-After", response.headers)
                self.assertNotIn("1234567890123456", await response.text())
        self.assertEqual(len(self.services.calls), 1)

    async def test_success_json_and_sse_bytes_do_not_gain_a_wait_hint(self):
        self.services.response_headers = {"Retry-After": "120"}
        for stream in (False, True):
            self.services.mode = "stream" if stream else "json"
            async with await self.post({**self.body, "stream": stream}) as response:
                self.assertEqual(response.status, 200)
                self.assertNotIn("Retry-After", response.headers)
                expected = (
                    self.services.stream if stream else json.dumps(self.services.response).encode()
                )
                self.assertEqual(await response.read(), expected)
        self.assertEqual(len(self.services.calls), 2)


class RetryAfterNativeHttpTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = native_fixture.NativeHttpTests.asyncSetUp
    native_headers = native_fixture.NativeHttpTests.native_headers
    post_native = native_fixture.NativeHttpTests.post_native
    read_receipt = native_fixture.NativeHttpTests.read_receipt

    async def test_wait_hints_keep_native_error_bytes_and_receipts_exact(self):
        self.services.native_mode = "error_json"
        for status, value in ((429, "120"), (503, HTTP_DATE)):
            with self.subTest(status=status):
                self.services.native_http_status = status
                self.services.native_response_headers = {
                    "Retry-After": value,
                    "X-Provider-Private": SECRETS["TS042_TEST_NATIVE_UPSTREAM"],
                }
                async with await self.post_native() as response:
                    self.assertEqual(response.status, status)
                    self.assertEqual(response.headers["Retry-After"], value)
                    self.assertEqual(await response.read(), NATIVE_ERROR)
                    self.assertNotIn("X-Provider-Private", response.headers)
                    self.assertNotIn("Location", response.headers)
                    self.assertNotIn("Set-Cookie", response.headers)
                    request_id = response.headers["X-Request-ID"]
                route = await self.read_receipt(request_id)
                self.assertEqual(route["outcome"], "failed" if status == 429 else "unknown")
                self.assertEqual(route["execution"]["error_code"], "upstream_http_error")
                self.assertTrue(route["execution"]["upstream_started"])
                self.assertFalse(route["execution"]["cancel_requested"])
                self.assertFalse(route["usage_complete"])
                self.assertNotIn("Retry-After", json.dumps(route))
                self.assertNotIn("retry_after", route)
                await self.read_receipt(
                    request_id, token="TS042_TEST_NATIVE_EXTERNAL", expected=404
                )
        self.assertEqual(len(self.services.native_calls), 2)

    async def test_invalid_and_other_status_hints_are_omitted_without_changing_native_bytes(self):
        self.services.native_mode = "error_json"
        for status, value in (
            (429, "Infinity"),
            (503, "9" * 129),
            (429, "0.5"),
            (429, SECRETS["TS042_TEST_NATIVE_UPSTREAM"]),
            (400, "120"),
        ):
            with self.subTest(status=status, value=value):
                self.services.native_http_status = status
                self.services.native_response_headers = {"Retry-After": value}
                async with await self.post_native() as response:
                    self.assertEqual(response.status, status)
                    self.assertNotIn("Retry-After", response.headers)
                    self.assertEqual(await response.read(), NATIVE_ERROR)
        self.assertEqual(len(self.services.native_calls), 5)

    async def test_syntactically_valid_secret_hint_cannot_escape_native_route(self):
        self.services.native_mode = "error_json"
        self.services.native_response_headers = {"Retry-After": "1234567890123456"}
        self.gateway.secrets.references["secret-ref:fixture/retry-hint"] = "TS261_TEST_HINT"
        with patch.dict(os.environ, {"TS261_TEST_HINT": "1234567890123456"}):
            async with await self.post_native() as response:
                self.assertEqual(response.status, 429)
                self.assertNotIn("Retry-After", response.headers)
                self.assertEqual(await response.read(), NATIVE_ERROR)
        self.assertEqual(len(self.services.native_calls), 1)

    async def test_native_json_and_sse_bytes_do_not_gain_a_wait_hint(self):
        self.services.native_response_headers = {"Retry-After": "120"}
        for stream in (False, True):
            self.services.native_mode = "stream" if stream else "json"
            body = native_fixture.native_body(stream=stream)
            async with await self.post_native(body=body) as response:
                self.assertEqual(response.status, 200)
                self.assertNotIn("Retry-After", response.headers)
                expected = (
                    self.services.native_stream if stream else self.services.native_response_bytes
                )
                self.assertEqual(await response.read(), expected)
        self.assertEqual(len(self.services.native_calls), 2)
