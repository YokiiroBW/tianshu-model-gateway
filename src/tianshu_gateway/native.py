"""Published model-protocol/v1 consumption: trusted identity, native snapshots, receipts.

Native configuration has its own owner-issued version space, its own ledger key space and
its own wire names. Nothing here reads or inherits the Chat ``config_version`` chain, and
identity is only ever taken from the deployment registration that authenticated the call.
"""

import asyncio
import copy
import os
import time
import uuid
from dataclasses import dataclass

import aiohttp

from .config import SourceUnavailable, read_limited, timestamp, utcnow
from .contracts import (
    NATIVE_CONTRACT,
    NATIVE_PROTOCOL,
    NATIVE_SNAPSHOT_PATH,
    NATIVE_WORKLOAD,
    Rejected,
    loads,
)

REQUIRED_PERMISSION = "config.snapshot"
MAX_SNAPSHOT_BYTES = 1_048_576


@dataclass(frozen=True)
class NativeGrant:
    """One authenticated native caller, registered by the deployment.

    ``principal_id``, ``caller_service`` and ``credential_namespace`` are trusted inputs
    defined here; a request body, ``metadata``/``user`` field or arbitrary client header
    never supplies them. ``config_versions`` mirrors the published legacy field so a
    deployment can declare Chat versions without granting anything native.
    """

    service: str
    credential_ref: str
    principal_id: str
    credential_namespace: str
    provider_ids: tuple[str, ...] = ()
    native_config_versions: tuple[int, ...] = ()
    permissions: tuple[str, ...] = (REQUIRED_PERMISSION,)
    config_versions: tuple[int, ...] = ()
    native_config_version: int | None = None
    expires_at: str | None = None
    revoked: bool = False
    internal: bool = False

    def identity(self):
        """Native ledger key space: contract plus trusted subject, service and namespace."""
        return (NATIVE_CONTRACT, self.principal_id, self.service, self.credential_namespace)

    def validate(self):
        for name in ("service", "principal_id", "credential_namespace"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError("native registration identity required")
        if not isinstance(self.credential_ref, str) or not self.credential_ref.startswith(
            "secret-ref:"
        ):
            raise ValueError("native credential reference required")
        if type(self.internal) is not bool or type(self.revoked) is not bool:
            raise ValueError("invalid native registration flags")
        for field_name in ("provider_ids", "permissions"):
            value = getattr(self, field_name)
            if not isinstance(value, tuple) or any(not isinstance(v, str) for v in value):
                raise ValueError("invalid native registration list")
        for field_name in ("native_config_versions", "config_versions"):
            value = getattr(self, field_name)
            if not isinstance(value, tuple) or any(type(v) is not int or v < 1 for v in value):
                raise ValueError("invalid native registration version")
        if len(set(self.native_config_versions)) != len(self.native_config_versions):
            raise ValueError("duplicate native registration version")
        if self.native_config_version is not None and (
            type(self.native_config_version) is not int or self.native_config_version < 1
        ):
            raise ValueError("invalid pinned native version")
        if self.expires_at is not None:
            if not isinstance(self.expires_at, str) or not self.expires_at.endswith("Z"):
                raise ValueError("native authorization expiry must be a UTC timestamp")
            timestamp(self.expires_at)


def authorize(grant, now=None):
    """Published trusted-access relations, evaluated before any native work is done."""
    now = now or utcnow()
    if grant.revoked:
        raise Rejected("forbidden", 403)
    if REQUIRED_PERMISSION not in grant.permissions:
        raise Rejected("forbidden", 403)
    if not grant.native_config_versions:
        # A missing native allowlist denies; it is never inherited from Chat versions.
        raise Rejected("forbidden", 403)
    if grant.expires_at is not None and not now < timestamp(grant.expires_at):
        raise Rejected("forbidden", 403)


def requested_version(grant, raw):
    """Explicit validated-service version header, otherwise the deployed native selection."""
    if raw is None:
        return grant.native_config_version
    if not isinstance(raw, str) or not raw.isascii() or not raw.isdecimal():
        raise Rejected()
    if str(int(raw)) != raw or int(raw) < 1:
        raise Rejected()
    version = int(raw)
    if version not in grant.native_config_versions:
        raise Rejected("forbidden", 403)
    return version


def validate_native_snapshot(contracts, document, requested, grant, targets, now=None):
    """Consumption checks for one owner-published native snapshot.

    Publication, immutability and revocation records belong to the platform; this only
    verifies what the gateway is about to route on. ``requested`` is None when the
    platform must select the maximum authorized native version itself.
    """
    contracts.validate("native#config_response", document)
    now = now or utcnow()
    version = document["native_config_version"]
    if requested is not None and version != requested:
        raise Rejected("version_conflict", 409)
    if version not in grant.native_config_versions:
        # The platform must never answer with a native version this caller may not read.
        raise Rejected("forbidden", 403)
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
    return providers


def select_native_route(document, grant, body):
    """Resolve the exact provider for this caller; no defaults, substitution or fallback."""
    binding = next((b for b in document["bindings"] if b["workload"] == NATIVE_WORKLOAD), None)
    if binding is None:
        raise Rejected()
    provider = next(
        (p for p in document["providers"] if p["provider_id"] == binding["provider_id"]), None
    )
    if provider is None:
        raise Rejected()
    if binding["fallback"] != "disabled" or provider["state_references"] != "reject":
        raise Rejected()
    for policy in ("model_policy", "reasoning_policy"):
        if provider[policy]["mode"] != "preserve_client" or provider[policy]["fields"]:
            raise Rejected()
    if provider["protocol"] != NATIVE_PROTOCOL:
        raise Rejected("version_conflict", 409)
    if binding["provider_id"] not in grant.provider_ids:
        raise Rejected("forbidden", 403)
    if provider["credential_namespace"] != grant.credential_namespace:
        raise Rejected("forbidden", 403)
    if body["model"] != provider["model_id"] or binding["model_id"] != provider["model_id"]:
        raise Rejected()
    return binding, provider


def route_context(grant, provider, request_id, turn_id, version):
    """Internal trusted projection; never accepted from a client."""
    return {
        "request_id": request_id,
        "turn_id": turn_id,
        "native_config_version": version,
        "workload": NATIVE_WORKLOAD,
        "protocol": NATIVE_PROTOCOL,
        "contract": NATIVE_CONTRACT,
        "caller_service": grant.service,
        "principal_id": grant.principal_id,
        "credential_namespace": provider["credential_namespace"],
    }


def reasoning_projection(body):
    """Only the native reasoning value is projected; no Chat reasoning policy applies."""
    return {"reasoning": copy.deepcopy(body["reasoning"])} if "reasoning" in body else {}


def route_receipt(grant, provider, body, request_id, version, observed_at):
    """Native receipt: native version and protocol only, never a Chat version or label."""
    reasoning = reasoning_projection(body)
    return {
        "schema_version": 1,
        "request_id": request_id,
        "native_config_version": version,
        "provider_id": provider["provider_id"],
        "credential_namespace": provider["credential_namespace"],
        "caller_service": grant.service,
        "requested_model": body["model"],
        "resolved_model": body["model"],
        "requested_reasoning": reasoning,
        "effective_reasoning": copy.deepcopy(reasoning),
        "applied_policies": [],
        "protocol": NATIVE_PROTOCOL,
        "outcome": "unknown",
        "upstream_request_id": None,
        "usage": None,
        "usage_complete": False,
        "fallback_used": False,
        "observed_at": observed_at,
        "native_usage": None,
        "contract": NATIVE_CONTRACT,
        "principal_id": grant.principal_id,
        "response_id": None,
    }


class NativeConfigSource:
    """Read-only client for the published native snapshot port.

    The platform Models owner remains the only configuration publisher; this class never
    writes, caches into Chat tables or guesses a protocol from the request body.
    """

    def __init__(self, session, contracts, targets, secrets, base_url, credential_ref, origin_env):
        targets.check(base_url)
        self.session, self.contracts, self.secrets = session, contracts, secrets
        self.base_url, self.credential_ref, self.origin_env = base_url, credential_ref, origin_env

    async def fetch(self, version):
        request_id = str(uuid.uuid4())
        body = {
            "query": {
                "schema_version": 1,
                "request_id": request_id,
                "origin": {"assertion_ref": os.environ.get(self.origin_env, "")},
            },
            "native_config_version": version,
            "contract": NATIVE_CONTRACT,
        }
        self.contracts.validate("native#config_request", body)
        headers = {"Authorization": "Bearer " + self.secrets.resolve(self.credential_ref)}
        try:
            async with self.session.post(
                self.base_url + NATIVE_SNAPSHOT_PATH,
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
                    raise Rejected("not_found", 404)
                if response.status != 200:
                    raise Rejected("invalid_input", 400)
                try:
                    raw = await read_limited(response.content, MAX_SNAPSHOT_BYTES)
                except Rejected:
                    raise Rejected("dependency_unavailable", 503) from None
                document = loads(raw)
                self.contracts.validate("native#config_response", document)
                if document["request_id"] != request_id:
                    raise Rejected("version_conflict", 409)
                return document
        except (aiohttp.ClientError, TimeoutError):
            raise SourceUnavailable() from None


class NativeConfigCache:
    """Verified native snapshots with an independent ledger key space.

    Chat versions and their revocation chain are never consulted, and a native version is
    never written into the shared Chat tables. Only transport/service unavailability may
    reuse an already verified, unexpired same-version snapshot.
    """

    def __init__(self, source, contracts, targets, ledger, refresh_seconds=30, max_entries=32):
        self.source, self.contracts, self.targets, self.ledger = (
            source,
            contracts,
            targets,
            ledger,
        )
        self.refresh_seconds, self.max_entries = refresh_seconds, max_entries
        self.entries = {}
        self.lock = asyncio.Lock()

    def revoke(self, grant, version):
        identity = grant.identity()
        self.ledger.native_revoke(identity, version)
        self.entries.pop((identity, version), None)

    def reject_revoked(self, identity, version):
        if self.ledger.native_is_revoked(identity, version):
            raise Rejected("forbidden", 403)

    async def get(self, grant, requested):
        identity = grant.identity()
        async with self.lock:
            if requested is not None:
                # A revoked native version is refused without consulting the platform again.
                self.reject_revoked(identity, requested)
            key = None if requested is None else (identity, requested)
            cached = self.entries.get(key) if key is not None else None
            if cached and time.monotonic() < cached[1] + self.refresh_seconds:
                self.reject_revoked(identity, cached[0]["native_config_version"])
                validate_native_snapshot(self.contracts, cached[0], requested, grant, self.targets)
                return copy.deepcopy(cached[0])
            try:
                document = await self.source.fetch(requested)
            except Rejected as exc:
                if exc.code == "forbidden" and requested is not None:
                    # The platform refused this caller/version pair: remember the refusal.
                    self.revoke(grant, requested)
                    raise
                if cached is None or not isinstance(exc, SourceUnavailable):
                    raise
                self.reject_revoked(identity, cached[0]["native_config_version"])
                validate_native_snapshot(self.contracts, cached[0], requested, grant, self.targets)
                return copy.deepcopy(cached[0])
            version = document["native_config_version"]
            self.reject_revoked(identity, version)
            validate_native_snapshot(self.contracts, document, requested, grant, self.targets)
            # An immutable version may vary only in the query correlation ID.
            stable = {k: v for k, v in document.items() if k != "request_id"}
            self.ledger.native_remember_config(identity, version, stable)
            if key is not None:
                if len(self.entries) >= self.max_entries:
                    self.entries.pop(next(iter(self.entries)))
                self.entries[key] = (copy.deepcopy(document), time.monotonic())
            return copy.deepcopy(document)
