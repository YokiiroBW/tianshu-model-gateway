"""Memory-only retrieval execution; Platform supplies config, never memory text."""

import asyncio
import hmac

import aiohttp
from aiohttp import web

from .config import read_limited
from .contracts import Rejected, loads
from .provider_adapter import ExecutionContext, ProviderFailure
from .retrieval_models import execute

PATH = "/internal/v1/memory-retrieval/execute"


async def configuration(gateway, payload):
    token = gateway.secrets.resolve(gateway.settings.platform_credential_ref)
    try:
        async with asyncio.timeout(5):
            async with gateway.platform_session.post(
                gateway.settings.platform_base_url + PATH,
                json=payload,
                headers={"Authorization": "Bearer " + token, "Accept-Encoding": "identity"},
                allow_redirects=False,
            ) as response:
                raw = await read_limited(response.content, 16384)
                if response.status != 200:
                    raise Rejected("dependency_unavailable", 503)
                value = loads(raw)
                if not isinstance(value, dict):
                    raise Rejected("dependency_unavailable", 503)
                return value
    except (aiohttp.ClientError, TimeoutError, OSError, ValueError):
        raise Rejected("dependency_unavailable", 503) from None


async def handle(gateway, request):
    ref = gateway.settings.memory_retrieval_credential_ref
    if ref is None:
        raise Rejected("not_found", 404)
    expected = "Bearer " + gateway.secrets.resolve(ref)
    if not hmac.compare_digest(
        request.headers.get("Authorization", "").encode(), expected.encode()
    ):
        raise Rejected("unauthorized", 401)
    if (
        request.content_type != "application/json"
        or request.query_string
        or request.headers.get("Origin")
        or request.headers.get("Cookie")
    ):
        raise Rejected()
    body = loads(await read_limited(request.content, 1048576))
    if not isinstance(body, dict):
        raise Rejected()
    operation = body.get("operation")
    if operation == "settings" and set(body) == {"operation"}:
        answer = await configuration(gateway, body)
    elif operation in {"embed", "rerank"}:
        revision = body.get("revision")
        if not isinstance(revision, str) or not 1 <= len(revision) <= 256:
            raise Rejected()
        model = await configuration(
            gateway, {"operation": "runtime", "kind": operation, "revision": revision}
        )
        try:
            context = ExecutionContext(**model["connection"])
            payload = {
                key: value for key, value in body.items() if key not in {"operation", "revision"}
            }
            async with asyncio.timeout(15):
                async with gateway.retrieval_slots:
                    answer = await execute(gateway.provider_adapter, context, operation, payload)
            current = await configuration(gateway, {"operation": "settings"})
            if current.get("revision") != revision:
                raise Rejected("dependency_unavailable", 503)
        except (ProviderFailure, KeyError, TypeError, TimeoutError):
            raise Rejected("dependency_unavailable", 503) from None
    else:
        raise Rejected()
    return web.json_response(answer, headers={"Cache-Control": "no-store"})
