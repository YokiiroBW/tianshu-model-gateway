"""Private platform authorization read for one exact dynamic version."""

import asyncio

import aiohttp

from .config import read_limited
from .contracts import Rejected, loads


class ProviderRuntimeSource:
    def __init__(self, session, base_url, secrets, credential_ref):
        self.session, self.base_url = session, base_url
        self.secrets, self.credential_ref = secrets, credential_ref

    async def fetch(self, version, turn_id, caller_service):
        token = self.secrets.resolve(self.credential_ref)
        payload = {
            "config_version": version,
            "turn_id": turn_id,
            "caller_service": caller_service,
            "workload": "companion.text",
        }
        try:
            async with asyncio.timeout(5):
                async with self.session.post(
                    self.base_url + "/internal/v1/provider-self-service/runtime",
                    json=payload,
                    headers={"Authorization": "Bearer " + token, "Accept-Encoding": "identity"},
                    allow_redirects=False,
                ) as response:
                    raw = await read_limited(response.content, 12288)
                    if response.status in {401, 403, 410}:
                        raise Rejected("forbidden", 403)
                    if (
                        response.status != 200
                        or response.headers.get("Content-Encoding", "identity").lower()
                        != "identity"
                    ):
                        raise Rejected("dependency_unavailable", 503)
                    document = loads(raw)
            if (
                not isinstance(document, dict)
                or set(document) - {"verified_capabilities", "unsupported_capabilities"}
                != {
                    "config_version",
                    "provider_id",
                    "provider_revision",
                    "protocol",
                    "base_url",
                    "model_id",
                    "api_key",
                    "usable_until",
                }
                or document["config_version"] != version
                or not isinstance(document["provider_id"], str)
                or type(document["provider_revision"]) is not int
                or not isinstance(document["api_key"], str)
                or not isinstance(document["base_url"], str)
                or not isinstance(document["model_id"], str)
            ):
                raise Rejected("dependency_unavailable", 503)
            for field in ("verified_capabilities", "unsupported_capabilities"):
                values = document.get(field, [])
                if (
                    not isinstance(values, list)
                    or len(values) > 5
                    or any(
                        not isinstance(value, str)
                        or value not in {"text", "stream", "tools", "vision", "reasoning"}
                        for value in values
                    )
                    or len(set(values)) != len(values)
                ):
                    raise Rejected("dependency_unavailable", 503)
            if set(document.get("verified_capabilities", [])) & set(
                document.get("unsupported_capabilities", [])
            ):
                raise Rejected("dependency_unavailable", 503)
            return document
        except asyncio.CancelledError:
            raise
        except Rejected:
            raise
        except (aiohttp.ClientError, TimeoutError, OSError, ValueError):
            raise Rejected("dependency_unavailable", 503) from None
