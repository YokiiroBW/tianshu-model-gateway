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
from .contracts import (
    NATIVE_CONTRACT,
    NATIVE_RECEIPT_PATH,
    NATIVE_ROUTE_PATH,
    NATIVE_VERSION_HEADER,
    Contracts,
    Rejected,
    loads,
)
from .diagnostics import Diagnostics, redact
from .native import (
    NativeConfigCache,
    NativeConfigSource,
    NativeGrant,
    authorize,
    requested_version,
    route_context,
    route_receipt,
    select_native_route,
)
from .responses import send_responses, validate_request
from .routing import PROTOCOL, SecretGuard, StreamObserver, apply_fields, prepare, record_usage

LOG = logging.getLogger("tianshu_gateway")
INTERNAL_HEADERS = {"x-tianshu-config-version", "x-tianshu-workload", "x-tianshu-turn-id"}
NATIVE_HEADERS = {
    NATIVE_VERSION_HEADER.lower(),
    "x-tianshu-turn-id",
}
NATIVE_ROUTE_HEADERS = {"x-request-id"} | NATIVE_HEADERS | {"authorization"}
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
    native_enabled: bool = False
    native_contract_directory: str | None = None
    native_clients: list[NativeGrant] = field(default_factory=list)
    revoked_native_versions: list[int] = field(default_factory=list)
    max_request_bytes: int = 1_048_576
    max_response_bytes: int = 8_388_608
    max_concurrent: int = 16
    max_provider_concurrent: int = 4
    max_timeout_ms: int = 180_000
    request_read_timeout: float = 5.0
    config_refresh_seconds: float = 30.0
    revoked_versions: list[int] = field(default_factory=list)

    def validate(self):
        if not self.clients and not self.native_clients:
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
        if type(self.native_enabled) is not bool:
            raise ValueError("invalid native switch")
        native_material = bool(
            self.native_clients
            or self.native_contract_directory is not None
            or self.revoked_native_versions
        )
        if native_material and not self.native_enabled:
            # Native stays off by default; half-configured material must not look absent.
            raise ValueError("native material requires explicit enable")
        if any(type(v) is not int or v < 1 for v in self.revoked_native_versions):
            raise ValueError("invalid revoked native version")
        if not self.native_enabled:
            return
        if not self.native_contract_directory:
            raise ValueError("published native contract directory required")
        if not self.native_clients:
            raise ValueError("authenticated native client registration required")
        if len({g.service for g in self.native_clients}) != len(self.native_clients):
            raise ValueError("one explicit native grant per service required")
        for grant in self.native_clients:
            grant.validate()


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
        # The native ledger is the same local database but a different key space.
        self.native_ledger = diagnostics
        self.native_cache = None
        if settings.native_enabled:
            self.native_cache = NativeConfigCache(
                NativeConfigSource(
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

    def authenticate_native(self, request):
        """Native clients are their own registration; Chat credentials never open them."""
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if request.headers.get("Authorization", "") != "Bearer " + token or not token:
            raise Rejected("unauthorized", 401)
        matches = []
        for grant in self.settings.native_clients:
            try:
                expected = self.secrets.resolve(grant.credential_ref)
            except Rejected:
                continue  # Unset/revoked service credentials never authenticate.
            if hmac.compare_digest(token.encode(), expected.encode()):
                matches.append(grant)
        if len(matches) != 1:
            raise Rejected("unauthorized", 401)
        return matches[0]

    def native_context(self, request, grant):
        """Trusted request identity and native version selection for one native call.

        The Chat version header is rejected here and the native header is rejected on the
        Chat route, so equal integers can never mean the same configuration.
        """
        if any(len(request.headers.getall(h, [])) > 1 for h in NATIVE_ROUTE_HEADERS):
            raise Rejected()
        if any(h in request.headers for h in ("openai-organization", "openai-project")):
            raise Rejected("forbidden", 403)
        if "x-tianshu-config-version" in request.headers or "x-tianshu-workload" in request.headers:
            raise Rejected()
        if not grant.internal:
            if any(
                h.lower().startswith("x-tianshu-") or h.lower() == "x-request-id"
                for h in request.headers
            ):
                raise Rejected("forbidden", 403)
            request_id, turn_id = str(uuid.uuid4()), str(uuid.uuid4())
        else:
            if any(
                h.lower().startswith("x-tianshu-") and h.lower() not in NATIVE_HEADERS
                for h in request.headers
            ):
                raise Rejected()
            try:
                request_id = request.headers["x-request-id"]
                turn_id = request.headers["x-tianshu-turn-id"]
            except KeyError:
                raise Rejected() from None
            for value in (request_id, turn_id):
                self.contracts.validate("common#id", value)
                if any(secret in value for secret in self.secrets.known_values()):
                    raise Rejected()
        authorize(grant)
        version = requested_version(grant, request.headers.get(NATIVE_VERSION_HEADER))
        return request_id, version, turn_id

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
            if NATIVE_VERSION_HEADER.lower() in request.headers:
                # Native versions are a different space; never reinterpret them as Chat.
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

    async def native_read(self, request):
        """Receipt read bound to the trusted identity, never to the request ID alone.

        A registration that is revoked, expired, unpermitted or carries no native version
        allowlist is not a usable identity, so it cannot read even its own history. Native
        *version* revocation is a different concept: it stops new routing and never erases
        the audit rows, so it is deliberately not consulted here.
        """
        grant = self.authenticate_native(request)
        authorize(grant)
        result = self.native_ledger.native_get(grant.identity(), request.match_info["request_id"])
        if result is None:
            raise Rejected("not_found", 404)
        return web.json_response(
            redact(result, self.secrets.known_values()), headers={"Cache-Control": "no-store"}
        )

    async def native_responses(self, request):
        grant = self.authenticate_native(request)
        request_id, version, turn_id = self.native_context(request, grant)
        request[REQUEST_ID] = request_id
        if (
            request.query_string
            or request.content_type != "application/json"
            or request.headers.get("Content-Encoding")
        ):
            raise Rejected()
        if self.active >= self.settings.max_concurrent:
            raise Rejected("queue_full", 429)
        self.active += 1
        provider_id = None
        try:
            async with asyncio.timeout(self.settings.request_read_timeout):
                try:
                    raw = await read_limited(request.content, self.settings.max_request_bytes)
                except Rejected:
                    raise Rejected("payload_too_large", 413) from None
            # Native scope/state rejection precedes published shape checks.
            body = validate_request(raw)
            self.contracts.validate("native#native_request", body)
            config = await self.native_cache.get(grant, version)
            binding, provider = select_native_route(config, grant, body)
            selected_version = config["native_config_version"]
            selected_provider = provider["provider_id"]
            if (
                self.provider_active.get(selected_provider, 0)
                >= self.settings.max_provider_concurrent
            ):
                raise Rejected("queue_full", 429)
            credential = self.secrets.resolve(provider["credential_ref"])
            self.targets.check(provider["base_url"])
            # Recheck after awaited config access and before sending.
            if self.native_ledger.native_is_revoked(grant.identity(), selected_version):
                raise Rejected("forbidden", 403)
            secrets = self.secrets.known_values()
            context = route_context(grant, provider, request_id, turn_id, selected_version)
            self.contracts.validate("native#route_context", context)
            receipt = route_receipt(
                grant,
                provider,
                body,
                request_id,
                selected_version,
                utcnow().isoformat().replace("+00:00", "Z"),
            )
            receipt = redact(receipt, (*secrets, credential))
            self.contracts.validate("native#route_receipt", receipt)
            # Only a caller-supplied turn can be pinned; an external caller's turn id is a
            # per-request correlation value, not a repeated client turn.
            self.native_ledger.native_begin(receipt, turn_id if grant.internal else None)
            request[FORWARD_STARTED] = True
            provider_id = selected_provider
            self.provider_active[provider_id] = self.provider_active.get(provider_id, 0) + 1
            # preserve_client: the client's own native bytes are the upstream payload.
            return await self.forward_native(
                request, raw, binding, provider, credential, receipt, secrets
            )
        except TimeoutError:
            raise Rejected("timeout", 408) from None
        finally:
            self.active -= 1
            if provider_id is not None:
                self.provider_active[provider_id] -= 1

    async def forward_native(
        self, request, payload, binding, provider, credential, receipt, secrets
    ):
        """Stream native bytes downstream; never rewrite, retry or fabricate a terminal."""
        state = {"response": None, "pending": None, "stream": False}

        async def start_response(status, content_type):
            headers = {
                "Content-Type": content_type,
                "Cache-Control": "no-store",
                "X-Request-ID": receipt["request_id"],
            }
            if content_type == "text/event-stream":
                response = web.StreamResponse(
                    status=status, headers={**headers, "X-Accel-Buffering": "no"}
                )
                state["stream"] = True
                state["response"] = response
                request[STREAM_RESPONSE] = response
                await response.prepare(request)
            else:
                state["pending"] = (status, headers)

        async def write(chunk):
            response = state["response"]
            if response is None:
                status, headers = state["pending"]
                state["response"] = web.Response(body=chunk, status=status, headers=headers)
                return
            await response.write(chunk)

        try:
            await send_responses(
                self.session,
                self.targets,
                self.native_ledger,
                payload=payload,
                base_url=provider["base_url"],
                credential=credential,
                receipt=receipt,
                start_response=start_response,
                write=write,
                timeout=min(binding["timeout_ms"], self.settings.max_timeout_ms) / 1000,
                max_response_bytes=self.settings.max_response_bytes,
                max_request_bytes=self.settings.max_request_bytes,
                secrets=secrets,
                validate_response=lambda document: self.contracts.validate(
                    "native#native_response", document
                ),
                validate_receipt=lambda document: self.contracts.validate(
                    "native#route_receipt", document
                ),
            )
        except Rejected:
            response = state["response"]
            if response is not None and response.prepared:
                # Headers are already on the wire: no local error frame can be appended.
                if request.transport:
                    request.transport.abort()
                return response
            raise
        response = state["response"]
        if state["stream"]:
            await response.write_eof()
        return response

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


def error_response(exc, request_id, contract=None):
    """Chat errors use the shared envelope; native paths use the published native one."""
    body = {
        "schema_version": 1,
        "request_id": request_id,
        "code": exc.code,
        "execution_state": exc.state,
        "retryable": False,
    }
    if contract is not None:
        body["contract"] = contract
    return web.json_response(
        body,
        status=exc.status,
        headers={"Cache-Control": "no-store", "X-Request-ID": request_id},
    )


def native_route(request):
    return request.path == NATIVE_ROUTE_PATH or request.path.startswith(NATIVE_RECEIPT_PATH)


@web.middleware
async def errors(request, handler):
    request[REQUEST_ID] = str(uuid.uuid4())
    native = native_route(request)
    contract = NATIVE_CONTRACT if native else None
    try:
        return await handler(request)
    except Rejected as exc:
        return error_response(exc, request[REQUEST_ID], contract)
    except web.HTTPException as exc:
        if native:
            # The published native table has no 404/405 pair; an unknown method or path on
            # a native interface stays a fixed not_found rather than an unmapped status.
            return error_response(Rejected("not_found", 404), request[REQUEST_ID], contract)
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
            if native:
                return error_response(
                    Rejected("result_unknown", 502, "unknown"), request[REQUEST_ID], contract
                )
            return error_response(Rejected("result_unknown", 503, "unknown"), request[REQUEST_ID])
        return error_response(
            Rejected("dependency_unavailable", 503), request[REQUEST_ID], contract
        )


GATEWAY = web.AppKey("gateway", Gateway)


def create_app(settings):
    settings.validate()
    contracts = Contracts(
        settings.contract_directory,
        settings.native_contract_directory if settings.native_enabled else None,
    )
    for client in settings.clients:
        contracts.validate("common#id", client.service)
        contracts.validate("common#id", client.provider_id)
    for grant in settings.native_clients:
        for value in (*grant.provider_ids, grant.service, grant.principal_id):
            contracts.validate("common#id", value)
        contracts.validate("common#id", grant.credential_namespace)
    targets = RegisteredTargets(settings.targets)
    targets.check(settings.platform_base_url)
    app = web.Application(middlewares=[errors], client_max_size=settings.max_request_bytes)

    async def resources(app):
        diagnostics = Diagnostics(settings.diagnostics_path)
        try:
            for version in settings.revoked_versions:
                diagnostics.revoke(version)
            for grant in settings.native_clients:
                for version in settings.revoked_native_versions:
                    # Independent key space: a native revocation never touches Chat rows.
                    diagnostics.native_revoke(grant.identity(), version)
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

    async def native_responses(request):
        return await request.app[GATEWAY].native_responses(request)

    async def native_receipt(request):
        return await request.app[GATEWAY].native_read(request)

    async def unsupported(request):
        request.app[GATEWAY].authenticate(request)
        return error_response(Rejected("invalid_input", 501), request[REQUEST_ID])

    async def native_disabled(request):
        request.app[GATEWAY].authenticate(request)
        return error_response(
            Rejected("unsupported_operation", 501), request[REQUEST_ID], NATIVE_CONTRACT
        )

    app.cleanup_ctx.append(resources)
    app.router.add_post("/v1/chat/completions", chat)
    app.router.add_get("/internal/v1/model-requests/{request_id}", receipt)
    for path in ("/v1/messages", "/v1/embeddings"):
        app.router.add_route("*", path, unsupported)
    if settings.native_enabled:
        app.router.add_post(NATIVE_ROUTE_PATH, native_responses)
        app.router.add_get(NATIVE_RECEIPT_PATH + "{request_id}", native_receipt)
    else:
        # Native stays closed until a deployment explicitly enables it.
        app.router.add_route("*", NATIVE_ROUTE_PATH, native_disabled)
    return app
