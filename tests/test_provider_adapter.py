"""Recorded local HTTP/TLS only; loopback permission exists in the fixture, not runtime."""

import asyncio
import ipaddress
import ssl
import tempfile
import unittest

from aiohttp import web

from observability_fixtures import start_tls, write_tls
from tianshu_gateway.provider_adapter import (
    ExecutionContext,
    OpenAIAdapter,
    ProviderFailure,
    TargetPolicy,
    checked_base,
)


class FixtureTargets:
    def permits(self, address, connection_type):
        return address == "127.0.0.1"


class AddressTests(unittest.TestCase):
    def test_production_addresses(self):
        policy = TargetPolicy(("10.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16"))
        for address in (
            "127.0.0.1",
            "::1",
            "169.254.169.254",
            "0.0.0.0",
            "224.0.0.1",
            "::ffff:127.0.0.1",
            "10.1.2.3",
            "100.64.0.1",
            "168.63.129.16",
            "100.100.100.200",
            "2002:7f00:1::",
        ):
            self.assertFalse(policy.permits(address, "public"), address)
        for address in ("127.0.0.1", "169.254.169.254", "8.8.8.8"):
            self.assertFalse(policy.permits(address, "local"), address)
        self.assertTrue(policy.permits("10.1.2.3", "local"))
        self.assertFalse(TargetPolicy().permits("10.1.2.3", "local"))
        self.assertTrue(policy.permits("8.8.8.8", "public"))

    def test_urls(self):
        for base in (
            "http://example.com",
            "https://u:p@example.com",
            "https://x/?key=x",
            "https://x/#fragment",
            "https://x/%2e",
            "https://x/../a",
            "https://x:bad",
            "https://x\\a",
            "https://x\r\n",
        ):
            with self.assertRaises(ProviderFailure):
                checked_base(base, "public")
        self.assertEqual(
            checked_base("https://example.com/v1/", "public")[0], "https://example.com/v1"
        )

    def test_nat64_targets_recheck_embedded_ipv4(self):
        policy = TargetPolicy()
        for address in (
            "64:ff9b::a9fe:a9fe",  # 169.254.169.254 metadata
            "64:ff9b::a00:102",  # 10.0.1.2 private
            "64:ff9b::7f00:1",  # loopback
        ):
            self.assertFalse(policy.permits(address, "public"), address)
        self.assertTrue(policy.permits("64:ff9b::808:808", "public"))
        prefix = ipaddress.IPv6Network("2001:4860:abcd:ef01::/64")
        packed = (
            prefix.network_address.packed[:8]
            + b"\x00"
            + ipaddress.IPv4Address("169.254.169.254").packed
            + b"\x00" * 3
        )
        mapped = str(ipaddress.IPv6Address(packed))
        self.assertTrue(TargetPolicy().permits(mapped, "public"))
        custom = TargetPolicy(nat64_prefixes=("64:ff9b::/96", str(prefix)))
        self.assertFalse(custom.permits(mapped, "public"))
        for cidr, address in (
            ("2001:db8::/32", "2001:db8:c000:221::"),
            ("2001:db8:100::/40", "2001:db8:1c0:2:21::"),
            ("2001:db8:122::/48", "2001:db8:122:c000:2:2100::"),
            ("2001:db8:122:300::/56", "2001:db8:122:3c0:0:221::"),
            ("2001:db8:122:344::/64", "2001:db8:122:344:c0:2:2100::"),
            ("2001:db8:122:344::/96", "2001:db8:122:344::192.0.2.33"),
        ):
            self.assertEqual(
                "192.0.2.33",
                str(
                    TargetPolicy._embedded_v4(
                        ipaddress.IPv6Address(address), ipaddress.IPv6Network(cidr)
                    )
                ),
            )
        with self.assertRaises(ValueError):
            TargetPolicy(nat64_prefixes=("2001:4860::/65",))


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.requests = []
        self.mode = "normal"
        self.seen = asyncio.Event()
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self.handle)
        self.runner, self.base = await start_tls(app, self.temp.name)
        cert, _ = write_tls(self.temp.name)
        self.tls = ssl.create_default_context(cafile=str(cert))
        self.resolutions = 0

        async def resolver(host, port):
            self.resolutions += 1
            return ("127.0.0.1",)

        self.resolver = resolver
        self.adapter = self.make_adapter()

    def make_adapter(self, **kwargs):
        return OpenAIAdapter(
            policy=FixtureTargets(), resolver=self.resolver, tls_context=self.tls, **kwargs
        )

    def context(self, key="fixture-key-one"):
        return ExecutionContext("provider-a", 3, self.base + "/v1", key, "fixture-model")

    async def asyncTearDown(self):
        await self.runner.cleanup()
        self.temp.cleanup()

    async def handle(self, request):
        body = await request.json() if request.method == "POST" else None
        self.requests.append((request.method, request.path, request.headers["Authorization"], body))
        self.seen.set()
        if self.mode == "slow":
            await asyncio.sleep(5)
        if self.mode == "drop":
            request.transport.close()
            return web.Response()
        if self.mode == "redirect":
            return web.Response(status=302, headers={"Location": self.base + "/stolen"})
        if isinstance(self.mode, int):
            return web.json_response(
                {"error": {"message": request.headers["Authorization"]}}, status=self.mode
            )
        if self.mode == "large":
            return web.Response(body=b"x" * 3000)
        if self.mode == "missing-model":
            return web.json_response({"error": {"code": "model_not_found"}}, status=404)
        if self.mode == "empty-reply":
            return web.json_response({"choices": []})
        if self.mode == "bad-finish":
            return web.json_response(
                {"choices": [{"message": {"content": "OK"}, "finish_reason": []}]}
            )
        if self.mode in {"reasoning-budget", "reasoning-only"}:
            enough = self.mode == "reasoning-budget" and body["max_tokens"] > 16
            return web.json_response(
                {
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "reasoning_content": "Synthetic reasoning before the final answer.",
                                "content": "OK" if enough else "",
                            },
                            "finish_reason": "stop"
                            if enough or self.mode == "reasoning-only"
                            else "length",
                        }
                    ],
                    "usage": {
                        "completion_tokens": 20 if enough else 16,
                        "completion_tokens_details": {"reasoning_tokens": 17 if enough else 16},
                    },
                }
            )
        if self.mode == "compressed":
            return web.Response(body=b"unused", headers={"Content-Encoding": "gzip"})
        if self.mode == "raw-reflection":
            response = web.StreamResponse()
            await response.prepare(request)
            secret = request.headers["Authorization"].removeprefix("Bearer ").encode()
            await response.write(b'{"data":[{"id":"' + secret[:5])
            await response.write(secret[5:] + b'"}]}')
            await response.write_eof()
            return response
        if self.mode == "reflection":
            secret = request.headers["Authorization"].removeprefix("Bearer ")
            return web.Response(
                text='{"data":[{"id":"' + "".join("\\u%04x" % ord(c) for c in secret) + '"}]}'
            )
        if request.path.endswith("/models"):
            return web.json_response(
                {
                    "data": [{"id": "model-b"}, {"id": "model-a"}, {"id": "model-b"}],
                    "secret_extra": "ignored",
                }
            )
        return web.json_response(
            {
                "choices": [
                    {"message": {"role": "assistant", "content": "OK"}, "finish_reason": "stop"}
                ]
            }
        )

    async def test_models_and_test_are_distinct_and_keys_do_not_mix(self):
        self.assertEqual(await self.adapter.models(self.context()), ("model-b", "model-a"))
        self.assertEqual(len(self.requests), 1)
        result = await self.adapter.test_reply(self.context("fixture-key-two"))
        self.assertTrue(result["reply_verified"])
        self.assertEqual(
            [r[2] for r in self.requests], ["Bearer fixture-key-one", "Bearer fixture-key-two"]
        )
        self.assertEqual(
            self.requests[1][3],
            {
                "model": "fixture-model",
                "stream": False,
                "messages": [{"role": "user", "content": "Reply with OK."}],
                "max_tokens": 256,
            },
        )
        self.assertNotIn("fixture-key", repr(self.context()))

    async def test_reasoning_budget_reaches_a_final_reply_in_one_request(self):
        self.mode = "reasoning-budget"
        result = await self.adapter.test_reply(self.context())
        self.assertTrue(result["reply_verified"])
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.requests[0][3]["max_tokens"], 256)
        self.assertEqual(set(self.requests[0][3]), {"model", "stream", "messages", "max_tokens"})

    async def test_reasoning_budget_fixture_refuses_16_token_truncation(self):
        self.mode = "reasoning-budget"
        with self.assertRaises(ProviderFailure) as caught:
            await self.adapter.complete(
                self.context(),
                {"messages": [{"role": "user", "content": "Reply with OK."}], "max_tokens": 16},
            )
        self.assertEqual(
            (caught.exception.code, caught.exception.outcome), ("invalid_response", "unknown")
        )
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.requests[0][3]["max_tokens"], 16)

    async def test_reasoning_without_final_content_is_still_unknown(self):
        self.mode = "reasoning-only"
        with self.assertRaises(ProviderFailure) as caught:
            await self.adapter.test_reply(self.context())
        self.assertEqual(
            (caught.exception.code, caught.exception.outcome), ("invalid_response", "unknown")
        )
        self.assertEqual(len(self.requests), 1)

    async def test_dns_pinned_once(self):
        # DNS host does not resolve normally; registered resolver owns the only connection.
        # The TLS fixture is for loopback, so use HTTP here to prove fixed DNS/Host behavior.
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self.handle)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            context = ExecutionContext("p", 1, f"http://fixture.invalid:{port}/v1", "fixture-key")
            await self.adapter.models(context, connection_type="local")
            self.assertEqual(self.resolutions, 1)
        finally:
            await runner.cleanup()

    async def test_mixed_dns_rejected_before_send(self):
        async def mixed(host, port):
            return ("8.8.8.8", "127.0.0.1")

        adapter = OpenAIAdapter(resolver=mixed, tls_context=self.tls)
        with self.assertRaisesRegex(ProviderFailure, "target_forbidden"):
            await adapter.models(self.context())
        self.assertFalse(self.requests)

    async def test_default_policy_does_not_allow_test_loopback(self):
        with self.assertRaisesRegex(ProviderFailure, "target_forbidden"):
            await OpenAIAdapter(tls_context=self.tls).models(self.context())

    async def test_tls_verification_is_required(self):
        adapter = OpenAIAdapter(policy=FixtureTargets(), resolver=self.resolver)
        with self.assertRaises(ProviderFailure):
            await adapter.models(self.context())
        self.assertFalse(self.requests)
        with self.assertRaises(ValueError):
            OpenAIAdapter(tls_context=ssl._create_unverified_context())

    async def test_redirect_is_not_followed(self):
        self.mode = "redirect"
        with self.assertRaisesRegex(ProviderFailure, "redirect_rejected"):
            await self.adapter.models(self.context())
        self.assertEqual(len(self.requests), 1)

    async def test_error_classification_without_reflection(self):
        for status, reason in (
            (401, "invalid_credential"),
            (403, "invalid_credential"),
            (404, "models_unsupported"),
            (405, "models_unsupported"),
            (429, "rate_limited"),
            (500, "upstream_failed"),
        ):
            self.mode = status
            with self.assertRaises(ProviderFailure) as caught:
                await self.adapter.models(self.context())
            self.assertEqual(caught.exception.code, reason)
            self.assertNotIn("fixture-key", str(caught.exception))

    async def test_reflection_and_bounded_response(self):
        for mode in ("reflection", "raw-reflection", "large", "compressed"):
            self.mode = mode
            with self.assertRaises(ProviderFailure) as caught:
                await self.make_adapter(response_limit=2048).models(self.context())
            self.assertNotIn("fixture-key", str(caught.exception))

    async def test_escaped_credential_is_blocked_after_decoding(self):
        self.mode = "reflection"
        with self.assertRaisesRegex(ProviderFailure, "credential_reflected"):
            await self.adapter.models(self.context('key-with-"quote'))

    async def test_concurrent_keys_stay_request_local(self):
        await asyncio.gather(
            *(self.adapter.test_reply(self.context(f"fixture-key-{i}")) for i in range(5))
        )
        self.assertEqual(
            {r[2] for r in self.requests}, {f"Bearer fixture-key-{i}" for i in range(5)}
        )
        self.assertEqual(len(self.requests), 5)

    async def test_endpoint_and_model_errors_and_empty_reply_are_distinct(self):
        for mode, reason in (
            (404, "endpoint_not_found"),
            ("missing-model", "model_not_found"),
            ("empty-reply", "invalid_response"),
            ("bad-finish", "invalid_response"),
        ):
            self.mode = mode
            with self.assertRaisesRegex(ProviderFailure, reason):
                await self.adapter.test_reply(self.context())

    async def test_dns_timeout_never_sends_request(self):
        async def delayed(host, port):
            await asyncio.sleep(5)

        adapter = OpenAIAdapter(resolver=delayed, timeout_seconds=0.05)
        with self.assertRaises(ProviderFailure) as caught:
            await adapter.models(self.context())
        self.assertEqual(caught.exception.outcome, "not_started")
        self.assertFalse(self.requests)

    async def test_timeout_and_cancel_never_retry(self):
        self.mode = "slow"
        with self.assertRaises(ProviderFailure) as caught:
            await self.make_adapter(timeout_seconds=0.1).test_reply(self.context())
        self.assertEqual((caught.exception.code, caught.exception.outcome), ("timeout", "unknown"))
        self.seen.clear()
        task = asyncio.create_task(self.adapter.test_reply(self.context()))
        await self.seen.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(self.requests), 2)

    async def test_disconnect_never_retry(self):
        self.mode = "drop"
        for operation in (self.adapter.models, self.adapter.test_reply):
            before = len(self.requests)
            with self.assertRaises(ProviderFailure):
                await operation(self.context())
            self.assertEqual(len(self.requests), before + 1)

    async def test_complete_cannot_change_selected_model(self):
        with self.assertRaisesRegex(ProviderFailure, "invalid_request"):
            await self.adapter.complete(self.context(), {"model": "other"})
        self.assertFalse(self.requests)


if __name__ == "__main__":
    unittest.main()
