"""Opt-in, same-ref model origin renewal. No administrator credential or bootstrap path."""

import asyncio
import os
import re
import time
import uuid
from datetime import datetime
from urllib.parse import urlsplit

import aiohttp

from .config import SourceUnavailable, read_limited
from .contracts import Rejected, loads
from .observability import CORRELATION_HEADER, current_correlation

PATH = "/internal/v1/model-config/origin/renew"
FIELDS = {"schema_version", "request_id", "assertion_ref"}
REF = re.compile(r"origin:[0-9a-f]{32}")
DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z")


def validate_request(body):
    if not isinstance(body, dict) or set(body) != FIELDS:
        raise Rejected()
    if type(body["schema_version"]) is not int or body["schema_version"] != 1:
        raise Rejected()
    request_id, ref = body["request_id"], body["assertion_ref"]
    try:
        valid = isinstance(request_id, str) and str(uuid.UUID(request_id)) == request_id
    except ValueError:
        valid = False
    if not valid or not isinstance(ref, str) or REF.fullmatch(ref) is None:
        raise Rejected()


def validate_response(body, request=None):
    if not isinstance(body, dict) or set(body) != FIELDS | {"expires_at"}:
        raise Rejected()
    echoed = {key: body[key] for key in FIELDS}
    validate_request(echoed)
    if request is not None and echoed != request:
        raise Rejected()
    value = body["expires_at"]
    if not isinstance(value, str) or DATE.fullmatch(value) is None:
        raise Rejected()
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (ValueError, OverflowError, OSError):
        raise Rejected() from None


def validate_settings(enabled, base_url):
    if type(enabled) is not bool or (enabled and urlsplit(base_url).scheme != "https"):
        raise ValueError("invalid model origin renewal configuration")


class OriginRenewal:
    def __init__(self, session, targets, secrets, base_url, credential_ref, origin_env):
        validate_settings(True, base_url)
        targets.check(base_url)
        self.session, self.secrets = session, secrets
        self.base_url, self.credential_ref, self.origin_env = base_url, credential_ref, origin_env
        self.ref = os.environ.get(origin_env, "")
        if REF.fullmatch(self.ref) is None:
            raise Rejected("dependency_unavailable", 503)
        self.expires_at = None
        self.deadline = self.renew_at = 0.0
        self.closed = False
        self.failure = None
        self._flight = self._worker = None

    def deny(self, code="forbidden", status=403):
        if self.failure is None:
            self.failure = (code, status)

    def _available(self):
        if self.closed:
            raise Rejected("dependency_unavailable", 503)
        # Environment is bootstrap input, never a live channel for silently replacing authority.
        if os.environ.get(self.origin_env, "") != self.ref:
            self.deny()
        if self.expires_at is not None and (
            time.time() >= self.expires_at or time.monotonic() >= self.deadline
        ):
            self.deny()
            raise Rejected("forbidden", 403)
        if self.failure is not None:
            raise Rejected(*self.failure)

    def check(self):
        """Read-only gate, including after any wait and on every cache access."""
        self._available()
        if self.expires_at is None:
            raise Rejected("dependency_unavailable", 503)

    async def ensure(self):
        self._available()
        if self.expires_at is not None and time.monotonic() < self.renew_at:
            return
        if self._flight is None or self._flight.done():
            self._flight = asyncio.create_task(self._renew(), name="model-origin-renewal-flight")
            # A cancelled waiter cannot leave an unobserved owner exception.
            self._flight.add_done_callback(
                lambda task: task.exception() if not task.cancelled() else None
            )
        await asyncio.shield(self._flight)
        self.check()

    async def start(self):
        await self.ensure()
        if self._worker is None:
            self._worker = asyncio.create_task(self._run(), name="model-origin-renewal-worker")

    async def _run(self):
        try:
            while not self.closed:
                await asyncio.sleep(max(0, self.renew_at - time.monotonic()))
                await self.ensure()
        except Rejected:
            # Failed closed until explicit re-bootstrap/restart. No background issuance loop.
            return

    async def _renew(self):
        # Even all retries share a three-second round and the current authority's deadline.
        stop = time.monotonic() + 3.0
        if self.expires_at is not None:
            stop = min(stop, self.deadline, time.monotonic() + self.expires_at - time.time())
        body = {"schema_version": 1, "request_id": str(uuid.uuid4()), "assertion_ref": self.ref}
        validate_request(body)
        try:
            async with asyncio.timeout_at(stop):
                for attempt in range(3):
                    self._available()
                    if time.monotonic() >= stop:
                        raise TimeoutError()
                    try:
                        await self._attempt(body, stop)
                        return
                    except SourceUnavailable:
                        self._available()
                        if attempt == 2:
                            raise
                        remaining = stop - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError() from None
                        await asyncio.sleep(min((0.25, 0.5)[attempt], remaining))
        except (TimeoutError, Rejected):
            # If the deadline crossed, a late response must not revive local authority.
            self._available()
            self.deny("dependency_unavailable", 503)
            raise Rejected("dependency_unavailable", 503) from None

    async def _attempt(self, body, stop):
        headers = {"Authorization": "Bearer " + self.secrets.resolve(self.credential_ref)}
        correlation = current_correlation()
        if correlation is not None:
            headers[CORRELATION_HEADER] = correlation
        started_mono = time.monotonic()
        try:
            async with self.session.post(
                self.base_url + PATH,
                json=body,
                headers=headers,
                allow_redirects=False,
                # The enclosing round is also this request's total deadline. A second
                # aiohttp timer at the same instant can consume the outer cancellation.
                timeout=aiohttp.ClientTimeout(total=None),
            ) as response:
                if response.status in {401, 403, 410}:
                    self.deny()
                    raise Rejected("forbidden", 403)
                if response.status >= 500 or response.status == 429:
                    raise SourceUnavailable()
                if response.status != 200 or response.content_type != "application/json":
                    raise Rejected("dependency_unavailable", 503)
                if response.headers.get("Content-Encoding", "identity") != "identity":
                    raise Rejected("dependency_unavailable", 503)
                raw = await read_limited(response.content, 4096)
                expires = validate_response(loads(raw), body)
        except (aiohttp.ClientError, TimeoutError):
            raise SourceUnavailable() from None
        self._available()
        if time.monotonic() >= stop:
            raise TimeoutError()
        # Never grant more than the producer's maximum 3600s. Subtract the whole round trip
        # conservatively; wall-clock rollback cannot extend the monotonic authority deadline.
        remaining = expires - time.time()
        if not 0 < remaining <= 3600:
            raise Rejected("dependency_unavailable", 503)
        deadline = started_mono + remaining
        if deadline <= time.monotonic():
            raise Rejected("dependency_unavailable", 503)
        previous = self.expires_at
        self.expires_at, self.deadline = expires, deadline
        # Half of the remaining lifetime, at most 30s between checks, with no minimum that
        # could put a short TTL over its ceiling. No child references are ever accepted.
        self.renew_at = time.monotonic() + min(30.0, (deadline - time.monotonic()) / 2)
        if (previous is not None and expires <= previous) or deadline - time.monotonic() <= 0.1:
            self.renew_at = deadline

    async def close(self):
        self.closed = True
        tasks = [task for task in (self._worker, self._flight) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


class CurrentSource:
    """Opt-in cache source: an unavailable live authority never becomes a cache fallback."""

    def __init__(self, source, origin):
        self.source, self.origin = source, origin

    async def fetch(self, version):
        await self.origin.ensure()
        try:
            result = await self.source.fetch(version)
        except Rejected as exc:
            if exc.code == "forbidden" or (
                exc.code == "dependency_unavailable" and not isinstance(exc, SourceUnavailable)
            ):
                self.origin.deny()
            # Strip SourceUnavailable's fallback permission in this stricter mode.
            raise Rejected(exc.code, exc.status) from None
        self.origin.check()
        return result


class CurrentCache:
    def __init__(self, cache, origin):
        self.cache, self.origin = cache, origin
        cache.refresh_seconds = 0
        cache.source = CurrentSource(cache.source, origin)

    async def get(self, *args):
        await self.origin.ensure()
        result = await self.cache.get(*args)
        self.origin.check()
        return result

    def __getattr__(self, name):
        return getattr(self.cache, name)
