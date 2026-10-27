"""Independent configuration loads retain validation, revocation and cancellation."""

import asyncio
import copy
import tempfile
import unittest
import uuid
from dataclasses import replace
from pathlib import Path

from gateway_fixtures import CONTRACT, NATIVE_CONTRACT, RecordingServices, registration
from tianshu_gateway.config import ConfigCache, RegisteredTargets, SourceUnavailable
from tianshu_gateway.contracts import Contracts, Rejected
from tianshu_gateway.diagnostics import Diagnostics
from tianshu_gateway.native import NativeConfigCache, NativeGrant


class SnapshotSource:
    def __init__(self, document, version_field):
        self.document, self.version_field = document, version_field
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = []
        self.error = None

    async def fetch(self, version):
        self.calls.append(version)
        if version == 7 and self.calls.count(7) == 1:
            self.started.set()
            await self.release.wait()
        if self.error is not None:
            raise self.error
        document = copy.deepcopy(self.document)
        document[self.version_field] = version
        document["request_id"] = str(uuid.uuid4())
        return document


class ConfigConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.ledger = Diagnostics(Path(self.temporary.name) / "requests.sqlite")
        self.addCleanup(self.ledger.close)
        self.contracts = Contracts(CONTRACT, NATIVE_CONTRACT)
        self.targets = RegisteredTargets([registration("http://127.0.0.1:1/v1")])
        self.services = RecordingServices()
        self.services.configure("http://127.0.0.1:1")
        self.grant = NativeGrant(
            service="caller-fixture",
            credential_ref="secret-ref:fixture/native",
            principal_id="principal-fixture",
            credential_namespace="credential-namespace-fixture",
            native_config_versions=(7, 8),
            native_config_version=7,
        )

    def cache(self, native, *, refresh_seconds=30):
        source = SnapshotSource(
            self.services.native_config if native else self.services.config,
            "native_config_version" if native else "config_version",
        )
        if native:
            cache = NativeConfigCache(
                source, self.contracts, self.targets, self.ledger, refresh_seconds
            )

            def get(version):
                return cache.get(self.grant, version)

            def revoke(version):
                return cache.revoke(self.grant, version)
        else:
            cache = ConfigCache(source, self.contracts, self.targets, self.ledger, refresh_seconds)
            get, revoke = cache.get, cache.revoke
        return source, get, revoke

    async def test_blocked_version_does_not_delay_other_version(self):
        for native in (False, True):
            with self.subTest(native=native):
                source, get, _ = self.cache(native)
                slow = asyncio.create_task(get(7))
                try:
                    await asyncio.wait_for(source.started.wait(), 1)
                    fast = await asyncio.wait_for(get(8), 1)
                    self.assertEqual(fast[source.version_field], 8)
                    self.assertFalse(slow.done())
                finally:
                    source.release.set()
                    await slow

    async def test_same_key_shares_load_and_cancelled_waiter_does_not_release_owner(self):
        for native in (False, True):
            with self.subTest(native=native):
                source, get, _ = self.cache(native)
                owner = asyncio.create_task(get(7))
                await asyncio.wait_for(source.started.wait(), 1)
                cancelled = asyncio.create_task(get(7))
                follower = asyncio.create_task(get(7))
                try:
                    await asyncio.sleep(0)
                    cancelled.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await cancelled
                    self.assertFalse(owner.done())
                    self.assertFalse(follower.done())
                    source.release.set()
                    first, second = await asyncio.gather(owner, follower)
                    self.assertEqual(source.calls, [7])
                    self.assertEqual(first, second)
                    first["providers"].clear()
                    self.assertTrue(second["providers"])
                finally:
                    source.release.set()
                    await asyncio.gather(owner, follower, return_exceptions=True)

    async def test_native_identity_does_not_wait_for_another_identity(self):
        source = SnapshotSource(self.services.native_config, "native_config_version")
        cache = NativeConfigCache(source, self.contracts, self.targets, self.ledger)
        slow = asyncio.create_task(cache.get(self.grant, 7))
        try:
            await asyncio.wait_for(source.started.wait(), 1)
            other = replace(self.grant, principal_id="other-principal-fixture")
            fast = await asyncio.wait_for(cache.get(other, 7), 1)
            self.assertEqual(fast["native_config_version"], 7)
            self.assertFalse(slow.done())
            cache.revoke(self.grant, 7)
            self.assertEqual(await cache.get(other, 7), fast)
        finally:
            source.release.set()
            await asyncio.gather(slow, return_exceptions=True)

    async def test_revocation_during_fetch_is_still_rechecked(self):
        for native in (False, True):
            with self.subTest(native=native):
                source, get, revoke = self.cache(native)
                task = asyncio.create_task(get(7))
                await asyncio.wait_for(source.started.wait(), 1)
                revoke(7)
                source.release.set()
                with self.assertRaises(Rejected) as caught:
                    await task
                self.assertEqual(caught.exception.code, "forbidden")

    async def test_only_outage_reuses_verified_same_version_cache(self):
        for native in (False, True):
            with self.subTest(native=native):
                # Separate versions keep this test independent of previous revocations.
                source, get, _ = self.cache(native, refresh_seconds=0)
                document = await get(8)
                source.error = SourceUnavailable()
                self.assertEqual(await get(8), document)
                source.error = Rejected("version_conflict", 409)
                with self.assertRaises(Rejected) as caught:
                    await get(8)
                self.assertEqual(caught.exception.code, "version_conflict")


if __name__ == "__main__":
    unittest.main()
