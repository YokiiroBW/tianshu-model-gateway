"""Actual aiohttp transport with one attempt, cancellation and bounded streaming."""

import asyncio
import hmac
import logging
import time
import uuid
from dataclasses import dataclass, field

import aiohttp
from aiohttp import web

from .config import (
    ClientGrant,
    ConfigCache,
    EnvSecrets,
    HttpConfigSource,
    RegisteredTargets,
    read_limited,
    utcnow,
)
from .contracts import Contracts, Rejected, loads
from .diagnostics import Diagnostics, redact
from .routing import PROTOCOL, SecretGuard, StreamObserver, apply_fields, prepare, record_usage

LOG = logging.getLogger("tianshu_gateway")
INTERNAL_HEADERS = {"x-tianshu-config-version", "x-tianshu-workload", "x-tianshu-turn-id"}
REQUEST_ID = web.RequestKey("request_id", str)
FORWARD_STARTED = web.RequestKey("forward_started", bool)
STREAM_RESPONSE = web.RequestKey("stream_response", web.StreamResponse)


@dataclass
class Settings:
    contract_directory: str
    diagnostics_path: str
    platform_base_url: str
    platform_credential_ref: str
    platform_origin_env: str
    secret_references: dict[str, str]
    targets: list[dict]
    clients: list[ClientGrant]
    max_request_bytes: int = 1_048_576
    max_response_bytes: int = 8_388_608
    max_concurrent: int = 16
    max_provider_concurrent: int = 4
    max_timeout_ms: int = 180_000
    request_read_timeout: float = 5.0
    config_refresh_seconds: float = 30.0
    revoked_versions: list[int] = field(default_factory=list)

    def validate(self):
        if not self.clients:
            raise ValueError("authenticated client registration required")
        if (
            min(
                self.max_request_bytes,
                self.max_response_bytes,
                self.max_concurrent,
                self.max_provider_concurrent,
                self.max_timeout_ms,
                self.request_read_timeout,
            )
            <= 0
        ):
            raise ValueError("positive limits required")
        if self.config_refresh_seconds < 0:
            raise ValueError("nonnegative refresh interval required")
        if len({c.service for c in self.clients}) != len(self.clients):
            raise ValueError("one explicit grant per service required")
        if any(type(c.config_version) is not int or c.config_version < 1 for c in self.clients):
            raise ValueError("pinned configuration version required")
        if any(
            type(c.internal) is not bool
            or any(type(v) is not int or v < 1 for v in c.allowed_versions)
            for c in self.clients
        ):
            raise ValueError("invalid client grant")


class Gateway:
    def __init__(self, settings, contracts, targets, session, diagnostics):
        self.settings, self.contracts, self.targets = settings, contracts, targets
        self.session, self.diagnostics = session, diagnostics
        self.secrets = EnvSecrets(settings.secret_references)
        self.cache = ConfigCache(
            HttpConfigSource(
                session,
                contracts,
                targets,
                self.secrets,
                settings.platform_base_url,
                settings.platform_credential_ref,
                settings.platform_origin_env,
            ),
            contracts,
            targets,
            diagnostics,
            settings.config_refresh_seconds,
        )
        self.active = 0
        self.provider_active = {}

    def authenticate(self, request):
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if request.headers.get("Authorization", "") != "Bearer " + token or not token:
            raise Rejected("unauthorized", 401)
        matches = []
        for client in self.settings.clients:
            try:
                expected = self.secrets.resolve(client.credential_ref)
            except Rejected:
                continue  # Unset/revoked service credentials never authenticate.
            if hmac.compare_digest(token.encode(), expected.encode()):
                matches.append(client)
        if len(matches) != 1:
            raise Rejected("unauthorized", 401)
        return matches[0]

    def context(self, request, grant):
        if any(
            len(request.headers.getall(h, [])) > 1
            for h in INTERNAL_HEADERS | {"x-request-id", "authorization", "openai-beta"}
        ):
            raise Rejected()
        if any(h in request.headers for h in ("openai-organization", "openai-project")):
            raise Rejected("forbidden", 403)
        if not grant.internal:
            if any(
                h.lower().startswith("x-tianshu-") or h.lower() == "x-request-id"
                for h in request.headers
            ):
                raise Rejected("forbidden", 403)
            return str(uuid.uuid4()), grant.config_version, None
        try:
            version = request.headers["x-tianshu-config-version"]
            if not version.isascii() or not version.isdecimal() or str(int(version)) != version:
                raise ValueError()
            context = {
                "request_id": request.headers["x-request-id"],
                "turn_id": request.headers["x-tianshu-turn-id"],
                "config_version": int(version),
                "workload": request.headers["x-tianshu-workload"],
                "protocol": PROTOCOL,
            }
        except (KeyError, ValueError):
            raise Rejected() from None
        self.contracts.validate("model#route_context", context)
        if any(secret in context["request_id"] for secret in self.secrets.known_values()):
            raise Rejected()
        if context["config_version"] not in (grant.config_version, *grant.allowed_versions):
            raise Rejected("forbidden", 403)
        return context["request_id"], context["config_version"], context["turn_id"]

    async def receipt(self, request):
        grant = self.authenticate(request)
        result = self.diagnostics.get(grant.service, request.match_info["request_id"])
        if result is None:
            raise Rejected("not_found", 404)
        return web.json_response(
            redact(result, self.secrets.known_values()), headers={"Cache-Control": "no-store"}
        )

    async def chat(self, request):
        grant = self.authenticate(request)
        request_id, version, turn_id = self.context(request, grant)
        request[REQUEST_ID] = request_id
        if (
            request.query_string
            or request.content_type != "application/json"
            or request.headers.get("Content-Encoding")
        ):
            raise Rejected()
        beta = request.headers.get("OpenAI-Beta", "")
        if len(beta) > 4096 or any(ord(c) < 32 or ord(c) > 126 for c in beta):
            raise Rejected()
        if self.active >= self.settings.max_concurrent:
            raise Rejected("queue_full", 429)
        self.active += 1
        provider_id = None
        try:
            async with asyncio.timeout(self.settings.request_read_timeout):
                raw = await read_limited(request.content, self.settings.max_request_bytes)
            body = loads(raw)
            self.contracts.validate("model#native_request", body)
            config = await self.cache.get(version)
            effective, provider, binding, receipt = prepare(
                self.contracts, body, config, grant, request_id, grant.internal
            )
            selected_provider = provider["provider_id"]
            if (
                self.provider_active.get(selected_provider, 0)
                >= self.settings.max_provider_concurrent
            ):
                raise Rejected("queue_full", 429)
            credential = self.secrets.resolve(provider["credential_ref"])
            self.targets.check(provider["base_url"])
            # Recheck after awaited config access and before sending.
            if self.diagnostics.is_revoked(version):
                raise Rejected("forbidden", 403)
            secrets = self.secrets.known_values()
            receipt = redact(receipt, secrets)
            self.contracts.validate("model#route_receipt", receipt)
            self.diagnostics.begin(receipt, turn_id)
            request[FORWARD_STARTED] = True
            provider_id = selected_provider
            self.provider_active[provider_id] = self.provider_active.get(provider_id, 0) + 1
            payload = apply_fields(raw, effective, receipt["applied_policies"])
            return await self.forward(
                request, payload, effective, provider, binding, credential, receipt, secrets
            )
        except TimeoutError:
            raise Rejected("timeout", 408) from None
        finally:
            self.active -= 1
            if provider_id is not None:
                self.provider_active[provider_id] -= 1

    async def forward(
        self, request, payload, body, provider, binding, credential, receipt, secrets
    ):
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + credential,
            "Accept": "text/event-stream" if body.get("stream") else "application/json",
            "Accept-Encoding": "identity",
        }
        if "OpenAI-Beta" in request.headers:
            beta = request.headers["OpenAI-Beta"]
            headers["OpenAI-Beta"] = beta
        response = None
        upstream_status = None
        observer = None
        reason = "transport_unknown"
        timeout = min(binding["timeout_ms"], self.settings.max_timeout_ms) / 1000
        started = time.monotonic()
        try:
            # Includes reading and downstream backpressure; no retry or redirect middleware.
            async with asyncio.timeout(timeout):
                async with self.session.post(
                    provider["base_url"] + "/chat/completions",
                    data=payload,
                    headers=headers,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as upstream:
                    upstream_status = upstream.status
                    upstream_id = upstream.headers.get("x-request-id")
                    if upstream_id and len(upstream_id) <= 256:
                        receipt["upstream_request_id"] = redact(upstream_id, secrets)
                    if not 200 <= upstream.status < 300:
                        receipt["outcome"] = "failed" if 400 <= upstream.status < 500 else "unknown"
                        reason = "upstream_http_error"
                        # Preserve status, never echo the raw provider error or headers.
                        return error_response(
                            Rejected("dependency_unavailable", upstream.status, "unknown"),
                            receipt["request_id"],
                        )
                    if upstream.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise Rejected("result_unknown", 502, "unknown")
                    content_type = upstream.content_type
                    if body.get("stream"):
                        if content_type != "text/event-stream":
                            raise Rejected("result_unknown", 502, "unknown")
                        observer = StreamObserver(body.get("n", 1))
                        guard = SecretGuard(secrets)
                        response = web.StreamResponse(
                            status=upstream.status,
                            headers={
                                "Content-Type": "text/event-stream",
                                "Cache-Control": "no-store",
                                "X-Accel-Buffering": "no",
                                "X-Request-ID": receipt["request_id"],
                            },
                        )
                        request[STREAM_RESPONSE] = response
                        await response.prepare(request)
                        async for chunk in upstream.content.iter_any():
                            observer.feed(chunk)
                            safe = guard.feed(chunk)
                            if safe:
                                await response.write(safe)
                        observer.end()
                        if not observer.complete:
                            reason = "incomplete_stream"
                            raise Rejected("result_unknown", 502, "unknown")
                        tail = guard.feed(b"", final=True)
                        if tail:
                            await response.write(tail)
                        await response.write_eof()
                        record_usage(receipt, observer.native_usage, True)
                    else:
                        if content_type != "application/json":
                            raise Rejected("result_unknown", 502, "unknown")
                        raw = await read_limited(upstream.content, self.settings.max_response_bytes)
                        SecretGuard(secrets).feed(raw, final=True)
                        native = loads(raw)
                        self.contracts.validate("model#native_response", native)
                        if "error" in native:
                            raise Rejected("result_unknown", 502, "unknown")
                        choices = native["choices"]
                        if (
                            len(choices) != body.get("n", 1)
                            or {c.get("index") for c in choices if type(c.get("index")) is int}
                            != set(range(body.get("n", 1)))
                            or any(
                                not isinstance(c.get("finish_reason"), str)
                                or not c["finish_reason"]
                                for c in choices
                            )
                        ):
                            raise Rejected("result_unknown", 502, "unknown")
                        response = web.Response(
                            body=raw,
                            status=upstream.status,
                            headers={
                                "Content-Type": "application/json",
                                "Cache-Control": "no-store",
                                "X-Request-ID": receipt["request_id"],
                            },
                        )
                        record_usage(receipt, native.get("usage"), True)
                    receipt["outcome"], reason = "succeeded", "completed"
                    return response
        except asyncio.CancelledError:
            reason = "cancelled_unknown"
            if observer:
                record_usage(receipt, observer.native_usage, False)
            raise
        except (aiohttp.ClientError, TimeoutError, ConnectionError, Rejected) as exc:
            if isinstance(exc, TimeoutError):
                reason = "timeout_unknown"
            if observer:
                record_usage(receipt, observer.native_usage, False)
            if response is not None and response.prepared:
                # Do not fabricate [DONE] or close a truncated SSE body as a successful HTTP message.
                if request.transport:
                    request.transport.abort()
                return response
            return error_response(Rejected("result_unknown", 502, "unknown"), receipt["request_id"])
        finally:
            receipt["observed_at"] = utcnow().isoformat().replace("+00:00", "Z")
            safe_receipt = redact(receipt, secrets)
            self.contracts.validate("model#route_receipt", safe_receipt)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            self.diagnostics.finish(safe_receipt, reason, elapsed_ms, upstream_status)
            LOG.info(
                "model_request outcome=%s elapsed_ms=%d",
                receipt["outcome"],
                elapsed_ms,
            )


def error_response(exc, request_id):
    return web.json_response(
        {
            "schema_version": 1,
            "request_id": request_id,
            "code": exc.code,
            "execution_state": exc.state,
            "retryable": False,
        },
        status=exc.status,
        headers={"Cache-Control": "no-store", "X-Request-ID": request_id},
    )


@web.middleware
async def errors(request, handler):
    request[REQUEST_ID] = str(uuid.uuid4())
    try:
        return await handler(request)
    except Rejected as exc:
        return error_response(exc, request[REQUEST_ID])
    except web.HTTPException as exc:
        return error_response(Rejected("not_found", exc.status), request[REQUEST_ID])
    except asyncio.CancelledError:
        raise
    except Exception:
        # No exception text/stack/request URL: third-party errors can contain credentials or body.
        LOG.error("gateway_internal_error")
        stream = request.get(STREAM_RESPONSE)
        if stream is not None and stream.prepared:
            if request.transport:
                request.transport.abort()
            return stream
        if request.get(FORWARD_STARTED):
            return error_response(Rejected("result_unknown", 503, "unknown"), request[REQUEST_ID])
        return error_response(Rejected("dependency_unavailable", 503), request[REQUEST_ID])


GATEWAY = web.AppKey("gateway", Gateway)


def create_app(settings):
    settings.validate()
    contracts = Contracts(settings.contract_directory)
    for client in settings.clients:
        contracts.validate("common#id", client.service)
        contracts.validate("common#id", client.provider_id)
    targets = RegisteredTargets(settings.targets)
    targets.check(settings.platform_base_url)
    app = web.Application(middlewares=[errors], client_max_size=settings.max_request_bytes)

    async def resources(app):
        diagnostics = Diagnostics(settings.diagnostics_path)
        try:
            for version in settings.revoked_versions:
                diagnostics.revoke(version)
            connector = aiohttp.TCPConnector(
                resolver=targets, limit=settings.max_concurrent + 1, ttl_dns_cache=0
            )
            async with aiohttp.ClientSession(
                connector=connector,
                trust_env=False,
                auto_decompress=False,
                cookie_jar=aiohttp.DummyCookieJar(),
            ) as session:
                app[GATEWAY] = Gateway(settings, contracts, targets, session, diagnostics)
                yield
        finally:
            diagnostics.close()

    async def chat(request):
        return await request.app[GATEWAY].chat(request)

    async def receipt(request):
        return await request.app[GATEWAY].receipt(request)

    async def unsupported(request):
        request.app[GATEWAY].authenticate(request)
        return error_response(Rejected("invalid_input", 501), request[REQUEST_ID])

    app.cleanup_ctx.append(resources)
    app.router.add_post("/v1/chat/completions", chat)
    app.router.add_get("/internal/v1/model-requests/{request_id}", receipt)
    for path in ("/v1/responses", "/v1/messages", "/v1/embeddings"):
        app.router.add_route("*", path, unsupported)
    return app
