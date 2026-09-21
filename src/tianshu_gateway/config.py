"""Read-only platform snapshots, explicit target registration and secret references."""

import asyncio
import copy
import ipaddress
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlsplit

import aiohttp
from aiohttp.abc import AbstractResolver

from .contracts import Rejected, loads
from .observability import CORRELATION_HEADER, current_correlation

LOG = logging.getLogger("tianshu.gateway")


def utcnow():
    return datetime.now(timezone.utc)


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class EnvSecrets:
    """Deployment-owned allowlist; config documents contain references only."""

    def __init__(self, references):
        self.references = dict(references)

    def resolve(self, reference):
        name = self.references.get(reference)
        value = os.environ.get(name, "") if name else ""
        if len(value) < 16 or len(value) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in value):
            raise Rejected("dependency_unavailable", 503)
        return value

    def known_values(self):
        return [os.environ[n] for n in self.references.values() if os.environ.get(n)]


class RegisteredTargets(AbstractResolver):
    """Exact base URLs plus operator-reviewed IPs; no DNS or redirect expansion."""

    def __init__(self, registrations):
        self.urls = set()
        self.hosts = {}
        for entry in registrations:
            base = entry["base_url"]
            parsed = urlsplit(base)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
                or base.endswith("/")
                or "%" in base
                or "\\" in base
                or any(ord(c) <= 32 or ord(c) > 126 for c in base)
                or any(p in {".", ".."} for p in parsed.path.split("/"))
            ):
                raise ValueError("invalid registered base URL")
            addresses = tuple(str(ipaddress.ip_address(ip)) for ip in entry["addresses"])
            if not addresses:
                raise ValueError("reviewed IP addresses required")
            if parsed.scheme == "http":
                if entry.get("allow_private_http") is not True or not all(
                    ipaddress.ip_address(ip).is_private for ip in addresses
                ):
                    raise ValueError("HTTP requires explicit registered private target")
            try:
                literal = str(ipaddress.ip_address(parsed.hostname))
            except ValueError:
                literal = None
            if literal and addresses != (literal,):
                raise ValueError("literal target differs from approved address")
            key = (parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80))
            if key in self.hosts and self.hosts[key] != addresses:
                raise ValueError("conflicting IP registration")
            self.hosts[key] = addresses
            self.urls.add(base)

    def check(self, base):
        if base not in self.urls:
            raise Rejected("dependency_unavailable", 503)

    async def resolve(self, host, port=0, family=socket.AF_INET):
        addresses = self.hosts.get((host, port), ())
        if not addresses:
            raise OSError("unregistered destination")
        return [
            {
                "hostname": host,
                "host": ip,
                "port": port,
                "family": socket.AF_INET6 if ":" in ip else socket.AF_INET,
                "proto": 0,
                "flags": socket.AI_NUMERICHOST,
            }
            for ip in addresses
        ]

    async def close(self):
        pass


def validate_snapshot(contracts, document, version, targets, now=None):
    """Publication/consumption checks; this function does not publish anything."""
    contracts.validate("model#config_response", document)
    now = now or utcnow()
    if document["config_version"] != version:
        raise Rejected("unsupported_version", 400)
    if not timestamp(document["published_at"]) <= now < timestamp(document["usable_until"]):
        raise Rejected("dependency_unavailable", 503)
    providers = {p["provider_id"]: p for p in document["providers"]}
    if len(providers) != len(document["providers"]):
        raise Rejected()
    workloads = set()
    for provider in providers.values():
        targets.check(provider["base_url"])
    for binding in document["bindings"]:
        if binding["workload"] in workloads or binding["provider_id"] not in providers:
            raise Rejected()
        workloads.add(binding["workload"])
        if binding["model_id"] != providers[binding["provider_id"]]["model_id"]:
            raise Rejected()


@dataclass(frozen=True)
class ClientGrant:
    service: str
    credential_ref: str
    provider_id: str
    config_version: int
    internal: bool = False
    allowed_versions: tuple[int, ...] = ()


WORKLOAD_CLASSES = ("interactive", "background")


@dataclass(frozen=True)
class SchedulingPolicy:
    """Local runtime policy for bounded admission; never part of a published contract.

    These values are the deployment's own capacity policy. They do not change platform
    model-configuration ownership, the configuration publication contract or upstream model
    mapping, and they are read once at start-up from the deployment document.

    ``interactive_reserve == 0`` keeps the previous behaviour exactly: no waiting at all and
    an immediate ``queue_full`` when capacity is busy. Any positive reserve is what starts
    the bounded pool. ``max_in_flight`` is the hard global limit; ``interactive_reserve``
    slots of it are reserved for interactive work and may only be claimed by background work
    while fewer than ``max_in_flight - interactive_reserve`` slots are in flight.
    """

    max_in_flight: int
    max_provider_in_flight: int
    interactive_reserve: int
    max_queue_length: int
    wait_timeout_ms: int

    def validate(self):
        for name in (
            "max_in_flight",
            "max_provider_in_flight",
            "max_queue_length",
            "wait_timeout_ms",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError("positive scheduling limit required")
        if (
            type(self.interactive_reserve) is not int
            or self.interactive_reserve < 0
            or self.interactive_reserve >= self.max_in_flight
        ):
            raise ValueError("interactive reserve must be below the global limit")
        return self


def default_policy(global_limit=16, provider_limit=4):
    """The policy of a deployment that did not configure scheduling: immediate refusal."""
    return SchedulingPolicy(global_limit, provider_limit, 0, 1, 1)


@dataclass(frozen=True)
class Classification:
    """Which authenticated service belongs to which workload class.

    The mapping is deployment-owned and explicit. It is never derived from a request body,
    a request header, a model name or the protocol, and an external client with no binding
    uses ``default_class`` instead of choosing a class for itself.
    """

    bindings: dict = field(default_factory=dict)
    default_class: str = "interactive"

    def validate(self):
        if self.default_class not in WORKLOAD_CLASSES:
            raise ValueError("invalid default workload class")
        for service, workload_class in self.bindings.items():
            if not isinstance(service, str) or not service:
                raise ValueError("invalid bound service")
            if workload_class not in WORKLOAD_CLASSES:
                raise ValueError("invalid workload class")
        return self

    def for_service(self, service, declared_workload=None):
        """The bound class of one authenticated service; the declared value is inert."""
        return self.bindings.get(service, self.default_class)


class SourceUnavailable(Rejected):
    """Only transport/service outage can use an unexpired, already verified cache."""

    def __init__(self):
        super().__init__("dependency_unavailable", 503)


class HttpConfigSource:
    def __init__(
        self,
        session,
        contracts,
        targets,
        secrets,
        base_url,
        credential_ref,
        origin_env,
        on_success=None,
    ):
        targets.check(base_url)
        self.session, self.contracts, self.secrets = session, contracts, secrets
        self.base_url, self.credential_ref, self.origin_env = base_url, credential_ref, origin_env
        # Observation seam: called once per snapshot this process really fetched and verified.
        # It is never called for a cache hit, it cannot change the returned document, and a
        # failure inside it is confined to the observation side channel.
        self.on_success = on_success

    async def fetch(self, version):
        request_id = str(uuid.uuid4())
        body = {
            "query": {
                "schema_version": 1,
                "request_id": request_id,
                "origin": {"assertion_ref": os.environ.get(self.origin_env, "")},
            },
            "config_version": version,
        }
        self.contracts.validate("model#config_request", body)
        headers = {"Authorization": "Bearer " + self.secrets.resolve(self.credential_ref)}
        # TS-103: propagate the correlation of the request that triggered this already
        # registered internal peer call, so one attempt can be followed across the gateway and
        # the platform. The value is the same validated or freshly generated one the request
        # carries; it is never taken from the payload, never replaces the credential, never
        # changes the body and never reaches a model upstream. A fetch that no request
        # triggered (start-up validation, a background refresh) carries no header at all.
        correlation = current_correlation()
        if correlation is not None:
            headers[CORRELATION_HEADER] = correlation
        try:
            async with self.session.post(
                self.base_url + "/internal/v1/model-config/snapshot",
                json=body,
                headers=headers,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=3),
            ) as response:
                if response.status in {403, 410}:
                    raise Rejected("forbidden", 403)
                if response.status >= 500:
                    raise SourceUnavailable()
                if response.status == 401:
                    raise Rejected("dependency_unavailable", 503)
                if response.status == 404:
                    raise Rejected("unsupported_version", 400)
                if response.status != 200:
                    raise Rejected("invalid_input", 400)
                raw = await read_limited(response.content, 1_048_576)
                document = loads(raw)
                self.contracts.validate("model#config_response", document)
                if document["request_id"] != request_id:
                    raise Rejected("invalid_input", 400)
                if self.on_success is not None:
                    try:
                        self.on_success()
                    except Exception:
                        LOG.warning("platform_observation_failed")
                return document
        except (aiohttp.ClientError, TimeoutError):
            raise SourceUnavailable() from None


async def read_limited(content, limit):
    result = bytearray()
    async for chunk in content.iter_chunked(16384):
        result.extend(chunk)
        if len(result) > limit:
            raise Rejected("budget_exceeded", 413)
    return bytes(result)


class ConfigCache:
    def __init__(self, source, contracts, targets, diagnostics, refresh_seconds=30, max_entries=32):
        self.source, self.contracts, self.targets, self.diagnostics = (
            source,
            contracts,
            targets,
            diagnostics,
        )
        self.refresh_seconds, self.max_entries = refresh_seconds, max_entries
        self.entries = {}
        self.lock = asyncio.Lock()

    def revoke(self, version):
        self.diagnostics.revoke(version)
        self.entries.pop(version, None)

    async def get(self, version):
        async with self.lock:
            if self.diagnostics.is_revoked(version):
                raise Rejected("forbidden", 403)
            cached = self.entries.get(version)
            if cached and time.monotonic() < cached[1] + self.refresh_seconds:
                validate_snapshot(self.contracts, cached[0], version, self.targets)
                return copy.deepcopy(cached[0])
            try:
                document = await self.source.fetch(version)
            except Rejected as exc:
                if exc.code == "forbidden":
                    self.revoke(version)
                    raise
                # Only transport/service unavailability permits the verified, same-version cache.
                if not cached or not isinstance(exc, SourceUnavailable):
                    raise
                validate_snapshot(self.contracts, cached[0], version, self.targets)
                return copy.deepcopy(cached[0])
            if self.diagnostics.is_revoked(version):
                raise Rejected("forbidden", 403)
            validate_snapshot(self.contracts, document, version, self.targets)
            # An immutable version may vary only in the query correlation ID.
            stable = {k: v for k, v in document.items() if k != "request_id"}
            self.diagnostics.remember_config(version, stable)
            if len(self.entries) >= self.max_entries:
                self.entries.pop(next(iter(self.entries)))
            self.entries[version] = (copy.deepcopy(document), time.monotonic())
            return copy.deepcopy(document)
