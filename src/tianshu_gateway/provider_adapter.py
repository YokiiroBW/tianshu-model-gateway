"""Private, per-execution OpenAI adapter. No routes, persistence or authorization grant.

Only authenticated platform/runtime integration may construct ExecutionContext. The caller
must first authorize its exact provider revision/version and keep credentials off projections.
TargetPolicy is deployment-owned; provider input cannot supply it or a TLS context.
"""

import asyncio
import ipaddress
import json
import socket
import ssl
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

import aiohttp

from .config import RegisteredTargets, read_limited
from .contracts import Rejected, loads
from .routing import SecretGuard


class ProviderFailure(Exception):
    """Fixed public reason only; upstream bodies, addresses and keys never escape."""

    def __init__(self, code, outcome="not_started"):
        self.code, self.outcome = code, outcome
        super().__init__(code)


@dataclass(frozen=True)
class ExecutionContext:
    provider_id: str
    revision: int
    base_url: str = field(repr=False)
    api_key: str = field(repr=False)
    model_id: str = field(default="", repr=False)
    protocol: str = "openai-chat-completions"


@dataclass(frozen=True)
class TargetPolicy:
    # CIDRs are supplied by deployment, never by a provider-management request.
    local_networks: tuple[str, ...] = ()
    nat64_prefixes: tuple[str, ...] = ("64:ff9b::/96",)

    def __post_init__(self):
        if len(self.nat64_prefixes) > 16:
            raise ValueError("too many NAT64 prefixes")
        for value in self.nat64_prefixes:
            prefix = ipaddress.ip_network(value, strict=True)
            if not isinstance(prefix, ipaddress.IPv6Network) or prefix.prefixlen not in {
                32,
                40,
                48,
                56,
                64,
                96,
            }:
                raise ValueError("invalid NAT64 prefix")

    @staticmethod
    def _embedded_v4(ip, prefix):
        """RFC 6052: remove the reserved u-octet for prefixes shorter than /96."""
        if ip not in prefix:
            return None
        packed = ip.packed
        if prefix.prefixlen == 96:
            return ipaddress.IPv4Address(packed[12:16])
        if packed[8] != 0:
            return False  # malformed translation address must not be treated as global
        compact = packed[:8] + packed[9:]
        offset = prefix.prefixlen // 8
        return ipaddress.IPv4Address(compact[offset : offset + 4])

    def permits(self, address, connection_type):
        ip = ipaddress.ip_address(address)
        if isinstance(ip, ipaddress.IPv6Address):
            for value in self.nat64_prefixes:
                embedded = self._embedded_v4(ip, ipaddress.ip_network(value))
                if embedded is False:
                    return False
                if embedded is not None:
                    return self.permits(embedded, connection_type)
        if ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_unspecified:
            return False
        if ip.is_reserved or getattr(ip, "ipv4_mapped", None):
            return False
        if getattr(ip, "sixtofour", None) or getattr(ip, "teredo", None):
            return False
        if str(ip) in {"168.63.129.16", "100.100.100.200"}:
            return False
        if connection_type == "public":
            return ip.is_global
        if connection_type != "local":
            return False
        private = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7")
        return any(ip in ipaddress.ip_network(n) for n in private) and any(
            ip in ipaddress.ip_network(n) for n in self.local_networks
        )


async def resolve_addresses(host, port):
    records = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return tuple(dict.fromkeys(record[4][0] for record in records))


def checked_base(base, connection_type):
    try:
        parsed = urlsplit(base)
        if (
            connection_type not in {"public", "local"}
            or parsed.scheme
            not in ({"https"} if connection_type == "public" else {"https", "http"})
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or "%" in base
            or "\\" in base
            or any(ord(c) <= 32 or ord(c) > 126 for c in base)
            or any(p in {".", ".."} for p in parsed.path.split("/"))
            or len(base) > 2048
        ):
            raise ValueError()
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        return base.rstrip("/"), parsed.hostname, port
    except (ValueError, TypeError, AttributeError):
        raise ProviderFailure("invalid_address") from None


class OpenAIAdapter:
    def __init__(
        self,
        *,
        policy=None,
        resolver=resolve_addresses,
        tls_context=None,
        timeout_seconds=15,
        response_limit=1_048_576,
    ):
        self.policy = policy or TargetPolicy()
        self.resolver = resolver
        self.tls_context = tls_context or ssl.create_default_context()
        if not self.tls_context.check_hostname or self.tls_context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("verified TLS required")
        if not 0 < timeout_seconds <= 60 or not 1 <= response_limit <= 1_048_576:
            raise ValueError("bounded execution required")
        self.timeout_seconds, self.response_limit = timeout_seconds, response_limit

    @asynccontextmanager
    async def runtime_session(self, context):
        """One validated DNS set and one non-retrying session for the existing route path."""
        base, host, port = checked_base(context.base_url, "public")
        if context.protocol != "openai-chat-completions" or not context.api_key:
            raise ProviderFailure("invalid_request")
        try:
            async with asyncio.timeout(self.timeout_seconds):
                addresses = tuple(await self.resolver(host, port))
            if not addresses or not all(self.policy.permits(ip, "public") for ip in addresses):
                raise ProviderFailure("target_forbidden")
            targets = RegisteredTargets(
                [{"base_url": base, "addresses": addresses, "allow_private_http": False}]
            )
            connector = aiohttp.TCPConnector(
                resolver=targets, use_dns_cache=False, force_close=True, ssl=self.tls_context
            )
            async with aiohttp.ClientSession(
                connector=connector,
                trust_env=False,
                cookie_jar=aiohttp.DummyCookieJar(),
                auto_decompress=False,
            ) as session:
                session._retry_connection = False
                yield session
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, OSError, ValueError, TimeoutError):
            raise ProviderFailure("upstream_failed") from None

    async def models(self, context, *, connection_type="public"):
        document = await self._request(context, "models", None, connection_type)
        data = document.get("data")
        if not isinstance(data, list) or len(data) > 4096:
            raise ProviderFailure("invalid_response", "unknown")
        identifiers = []
        for model in data:
            value = model.get("id") if isinstance(model, dict) else None
            if not self._identifier(value):
                raise ProviderFailure("invalid_response", "unknown")
            if value not in identifiers:
                identifiers.append(value)
        return tuple(identifiers)

    async def test_reply(self, context, *, connection_type="public"):
        # Fixed short content: no caller history or arbitrary prompt is accepted here.
        result = await self.complete(
            context,
            {
                "messages": [{"role": "user", "content": "Reply with OK."}],
                "max_tokens": 16,
            },
            connection_type=connection_type,
        )
        # Test response is intentionally only a verdict, not provider-generated prose.
        return {
            "provider_id": context.provider_id,
            "revision": context.revision,
            "model_id": context.model_id,
            "reply_verified": bool(result),
        }

    @staticmethod
    def _identifier(value):
        return (
            isinstance(value, str)
            and 0 < len(value) <= 512
            and all(32 < ord(c) < 127 for c in value)
        )

    async def complete(self, context, payload, *, connection_type="public"):
        """Non-streaming seam; runtime authorization/receipts remain the server's duty."""
        if not self._identifier(context.model_id) or not isinstance(payload, dict):
            raise ProviderFailure("invalid_request")
        if payload.get("stream") or payload.get("model", context.model_id) != context.model_id:
            raise ProviderFailure("invalid_request")
        body = dict(payload, model=context.model_id, stream=False)
        try:
            if len(json.dumps(body, allow_nan=False).encode()) > 1_048_576:
                raise ValueError()
        except (ValueError, TypeError, RecursionError):
            raise ProviderFailure("invalid_request") from None
        document = await self._request(context, "chat/completions", body, connection_type)
        choices = document.get("choices")
        if (
            not isinstance(choices, list)
            or not choices
            or not all(
                isinstance(c, dict)
                and isinstance(c.get("message"), dict)
                and isinstance(c["message"].get("content"), str)
                and c["message"]["content"]
                and isinstance(c.get("finish_reason"), str)
                and c.get("finish_reason") in {"stop", "length"}
                for c in choices
            )
        ):
            raise ProviderFailure("invalid_response", "unknown")
        return document

    async def _request(self, context, operation, body, connection_type):
        if context.protocol != "openai-chat-completions":
            raise ProviderFailure("unsupported_protocol")
        key = context.api_key
        if (
            not isinstance(key, str)
            or not 1 <= len(key) <= 4096
            or any(not 33 <= ord(c) <= 126 for c in key)
        ):
            raise ProviderFailure("invalid_credential")
        base, host, port = checked_base(context.base_url, connection_type)
        started = False
        try:
            async with asyncio.timeout(self.timeout_seconds):
                addresses = tuple(await self.resolver(host, port))
                if not addresses or not all(
                    self.policy.permits(ip, connection_type) for ip in addresses
                ):
                    raise ProviderFailure("target_forbidden")
                # Resolver is called exactly once. Every socket uses this immutable address set;
                # original hostname remains available for Host and TLS certificate validation.
                targets = RegisteredTargets(
                    [
                        {
                            "base_url": base,
                            "addresses": addresses,
                            "allow_private_http": connection_type == "local",
                        }
                    ]
                )
                connector = aiohttp.TCPConnector(
                    resolver=targets, use_dns_cache=False, force_close=True, ssl=self.tls_context
                )
                async with aiohttp.ClientSession(
                    connector=connector,
                    trust_env=False,
                    cookie_jar=aiohttp.DummyCookieJar(),
                    auto_decompress=False,
                ) as session:
                    # aiohttp 3.14 may retry idempotent methods after stale socket failure.
                    # This adapter promises exactly one attempt, including model enumeration.
                    session._retry_connection = False
                    started = True
                    async with session.request(
                        "GET" if body is None else "POST",
                        base + "/" + operation,
                        json=body,
                        headers={"Authorization": "Bearer " + key, "Accept-Encoding": "identity"},
                        allow_redirects=False,
                    ) as response:
                        status = response.status
                        if (
                            response.headers.get("Content-Encoding", "identity").lower()
                            != "identity"
                        ):
                            raise ProviderFailure("invalid_response", "unknown")
                        if status != 200:
                            code = (
                                "invalid_credential"
                                if status in {401, 403}
                                else "models_unsupported"
                                if operation == "models" and status in {404, 405, 501}
                                else "endpoint_not_found"
                                if status == 404
                                else "redirect_rejected"
                                if 300 <= status < 400
                                else "rate_limited"
                                if status == 429
                                else "upstream_failed"
                            )
                            if body is not None and status in {400, 404}:
                                # Only an explicit machine code proves a missing model.
                                # An arbitrary 404 may instead mean a wrong base path.
                                try:
                                    error = loads(
                                        await read_limited(response.content, self.response_limit)
                                    )
                                    if isinstance(error, dict) and isinstance(
                                        error.get("error"), dict
                                    ):
                                        if error["error"].get("code") == "model_not_found":
                                            code = "model_not_found"
                                except Rejected:
                                    pass
                            raise ProviderFailure(
                                code, "unknown" if body is not None else "not_started"
                            )
                        raw = await read_limited(response.content, self.response_limit)
                        SecretGuard((key,)).feed(raw, final=True)
                        document = loads(raw)
                        # Catch equivalent JSON escapes too, not only exact wire encodings.
                        pending = [document]
                        while pending:
                            value = pending.pop()
                            if isinstance(value, str) and key in value:
                                raise ProviderFailure("credential_reflected", "unknown")
                            if isinstance(value, dict):
                                pending.extend(value.keys())
                                pending.extend(value.values())
                            elif isinstance(value, list):
                                pending.extend(value)
                        if not isinstance(document, dict) or "error" in document:
                            raise ProviderFailure("invalid_response", "unknown")
                        return document
        except asyncio.CancelledError:
            # Propagate cancellation; never retry or assert that the upstream did not execute.
            raise
        except TimeoutError:
            raise ProviderFailure("timeout", "unknown" if started else "not_started") from None
        except (aiohttp.ClientError, OSError, ValueError, Rejected):
            raise ProviderFailure(
                "upstream_failed", "unknown" if started else "not_started"
            ) from None
