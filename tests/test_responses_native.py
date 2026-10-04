"""Native Responses route tests: published contract, trusted identity, no Chat inheritance.

Everything runs against isolated loopback fixtures (platform substitute plus recording
upstream). No real platform, model provider, account or paid call is involved.
"""

import asyncio
import copy
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web

from gateway_fixtures import (
    CONTRACT,
    DOCUMENTS,
    NATIVE_CONTRACT,
    NATIVE_DOCUMENTS,
    SECRETS,
    RecordingServices,
    native_event,
    registration,
    start_http,
)
from tianshu_gateway.config import ClientGrant, RegisteredTargets, utcnow
from tianshu_gateway.contracts import Contracts, Rejected
from tianshu_gateway.diagnostics import Diagnostics
from tianshu_gateway.native import (
    NativeGrant,
    authorize,
    reasoning_projection,
    requested_version,
    route_context,
    route_receipt,
    select_native_route,
    validate_native_snapshot,
)
from tianshu_gateway.server import GATEWAY, Settings, create_app

NATIVE_PATH = "/v1/responses"
RECEIPT_PATH = "/internal/v1/native-model-requests/"
MODEL = NATIVE_DOCUMENTS["native_request"]["model"]
NAMESPACE = "credential-namespace-fixture"


def identity(principal="principal-fixture", service="caller-fixture", namespace=NAMESPACE):
    return ("model-protocol/v1", principal, service, namespace)


def grant(**overrides):
    fields = {
        "service": "caller-fixture",
        "credential_ref": "secret-ref:fixture/native",
        "principal_id": "principal-fixture",
        "credential_namespace": NAMESPACE,
        "provider_ids": ("provider-fixture",),
        "native_config_versions": (7,),
        "native_config_version": 7,
        "internal": True,
    }
    fields.update(overrides)
    return NativeGrant(**fields)


def native_body(**overrides):
    body = copy.deepcopy(NATIVE_DOCUMENTS["native_request"])
    body.update(overrides)
    return body


class NativeBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.contracts = Contracts(CONTRACT, NATIVE_CONTRACT)
        self.targets = RegisteredTargets(
            [
                registration("http://127.0.0.1:1/v1"),
                {
                    "base_url": NATIVE_DOCUMENTS["config_response"]["providers"][0]["base_url"],
                    "addresses": ["192.0.2.1"],
                },
            ]
        )

    def test_published_native_contract_is_loaded_separately_from_chat(self):
        for kind in ("config_request", "config_response", "route_context", "route_receipt"):
            self.contracts.validate(
                "native#" + kind,
                NATIVE_DOCUMENTS[
                    {
                        "config_request": "config_request",
                        "config_response": "config_response",
                        "route_context": "route_context",
                        "route_receipt": "route_receipt",
                    }[kind]
                ],
            )
        with self.assertRaises(ValueError):
            Contracts(CONTRACT).validate(
                "native#config_response", NATIVE_DOCUMENTS["config_response"]
            )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "native"
            root.mkdir()
            for name in ("manifest.json",):
                (root / name).write_bytes((NATIVE_CONTRACT / name).read_bytes())
            (root / "schemas").mkdir()
            schema = (NATIVE_CONTRACT / "schemas/model.json").read_bytes().replace(b"\r\n", b"\n")
            (root / "schemas/model.json").write_bytes(schema + b" ")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                Contracts(CONTRACT, root)
        tampered = copy.deepcopy(NATIVE_DOCUMENTS["config_response"])
        tampered["native_config_version"] = 0
        with self.assertRaises(Rejected):
            self.contracts.validate("native#config_response", tampered)

    def test_registration_validation_and_authorization_relations(self):
        grant().validate()
        for overrides in (
            {"native_config_versions": (0,)},
            {"native_config_versions": (7, 7)},
            {"config_versions": ("7",)},
            {"provider_ids": (7,)},
            {"permissions": "config.snapshot"},
            {"expires_at": "2026-09-14T00:00:00"},
            {"internal": "yes"},
            {"credential_ref": "native-token-raw"},
            {"principal_id": ""},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                grant(**overrides).validate()
        authorize(grant())
        for overrides in (
            {"revoked": True},
            {"permissions": ()},
            {"native_config_versions": ()},
            {"expires_at": (utcnow() - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(Rejected):
                authorize(grant(**overrides))
        authorize(
            grant(expires_at=(utcnow() + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"))
        )

    def test_header_version_selection_never_inherits_legacy_versions(self):
        pinned = grant()
        self.assertEqual(requested_version(pinned, "7"), 7)
        self.assertEqual(requested_version(pinned, None), 7)
        self.assertIsNone(requested_version(grant(native_config_version=None), None))
        for raw in ("", "07", "+7", "7 ", "7.0", "seven", "0", "-1"):
            with self.subTest(raw=raw), self.assertRaises(Rejected):
                requested_version(pinned, raw)
        with self.assertRaises(Rejected):
            requested_version(pinned, "8")
        # The legacy Chat allowlist never authorizes a native version.
        legacy = grant(native_config_versions=(7,), config_versions=(999,))
        with self.assertRaises(Rejected):
            requested_version(legacy, "999")
        self.assertEqual(legacy.identity(), identity())

    def test_snapshot_and_route_relations_use_published_codes(self):
        document = copy.deepcopy(NATIVE_DOCUMENTS["config_response"])
        now = utcnow()
        document["published_at"] = (now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        document["usable_until"] = (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        authorized = grant()
        validate_native_snapshot(self.contracts, document, 7, authorized, self.targets, now)
        for requested, update, code in (
            (8, {}, "version_conflict"),
            (
                7,
                {"usable_until": (now - timedelta(minutes=4)).isoformat().replace("+00:00", "Z")},
                "dependency_unavailable",
            ),
        ):
            with self.subTest(code=code), self.assertRaises(Rejected) as raised:
                changed = copy.deepcopy(document)
                changed.update(update)
                validate_native_snapshot(
                    self.contracts, changed, requested, authorized, self.targets, now
                )
            self.assertEqual(raised.exception.code, code)
        with self.assertRaises(Rejected) as raised:
            validate_native_snapshot(
                self.contracts, document, 7, grant(native_config_versions=(8,)), self.targets, now
            )
        self.assertEqual(raised.exception.code, "forbidden")
        body = native_body()
        binding, provider = select_native_route(document, authorized, body)
        self.assertEqual(
            (binding["workload"], provider["provider_id"]), ("native.responses", "provider-fixture")
        )
        for changed_grant, changed_body, code in (
            (grant(), native_body(model="other-model"), "invalid_input"),
            (grant(provider_ids=("provider-other",)), body, "forbidden"),
            (grant(credential_namespace="credential-namespace-other"), body, "forbidden"),
        ):
            with self.subTest(code=code), self.assertRaises(Rejected) as raised:
                select_native_route(document, changed_grant, changed_body)
            self.assertEqual(raised.exception.code, code)
        unusable = copy.deepcopy(document)
        unusable["bindings"][0]["workload"] = "companion.text"
        with self.assertRaises(Rejected):
            select_native_route(unusable, authorized, body)

    def test_context_and_receipt_shapes_stay_native(self):
        document = NATIVE_DOCUMENTS["config_response"]
        body = native_body()
        provider = document["providers"][0]
        context = route_context(grant(), provider, "request-fixture", "turn-fixture", 7)
        self.contracts.validate("native#route_context", context)
        observed_at = utcnow().isoformat().replace("+00:00", "Z")
        receipt = route_receipt(grant(), provider, body, "request-fixture", 7, observed_at)
        self.contracts.validate("native#route_receipt", receipt)
        self.assertNotIn("config_version", receipt)
        self.assertEqual(receipt["protocol"], "openai-responses")
        self.assertEqual(receipt["requested_reasoning"], {"reasoning": body["reasoning"]})
        self.assertEqual(receipt["effective_reasoning"], receipt["requested_reasoning"])
        self.assertEqual(receipt["applied_policies"], [])
        self.assertFalse(receipt["fallback_used"])
        self.assertIsNone(receipt["response_id"])
        self.assertEqual(receipt["principal_id"], "principal-fixture")
        self.assertEqual(receipt["credential_namespace"], NAMESPACE)
        self.assertEqual(reasoning_projection({"model": MODEL}), {})
        receipt["usage"] = None
        receipt["native_usage"] = None
        receipt["usage_complete"] = False
        self.contracts.validate("native#route_receipt", receipt)
        receipt["usage_complete"] = True
        with self.assertRaises(Rejected):
            self.contracts.validate("native#route_receipt", receipt)

    def test_native_ledger_key_space_is_independent_from_chat(self):
        with tempfile.TemporaryDirectory() as temporary:
            # Close inside the block: Windows keeps the SQLite file locked otherwise.
            ledger = Diagnostics(str(Path(temporary) / "ledger.sqlite"))
            try:
                ledger.revoke(7)
                self.assertTrue(ledger.is_revoked(7))
                self.assertFalse(ledger.native_is_revoked(identity(), 7))
                ledger.native_revoke(identity(), 7)
                self.assertTrue(ledger.native_is_revoked(identity(), 7))
                self.assertTrue(ledger.is_revoked(7))
                self.assertFalse(ledger.native_is_revoked(identity(principal="other"), 7))
                chat = {
                    "caller_service": "caller-fixture",
                    "request_id": "shared-id",
                    "config_version": 7,
                }
                ledger.begin(chat, None)
                native = {
                    "contract": "model-protocol/v1",
                    "principal_id": "principal-fixture",
                    "caller_service": "caller-fixture",
                    "credential_namespace": NAMESPACE,
                    "request_id": "shared-id",
                    "native_config_version": 7,
                }
                ledger.native_begin(native, "shared-turn")
                self.assertEqual(ledger.get("caller-fixture", "shared-id")["config_version"], 7)
                self.assertEqual(
                    ledger.native_get(identity(), "shared-id")["native_config_version"], 7
                )
                self.assertIsNone(ledger.native_get(identity(principal="other"), "shared-id"))
                with self.assertRaises(Rejected) as raised:
                    ledger.native_begin(native, "shared-turn")
                self.assertEqual(raised.exception.code, "idempotency_conflict")
                with self.assertRaises(Rejected) as raised:
                    ledger.native_begin(
                        {**native, "request_id": "next", "native_config_version": 8}, "shared-turn"
                    )
                self.assertEqual(raised.exception.code, "version_conflict")
                self.assertFalse(ledger.is_revoked(8))
            finally:
                ledger.close()


class NativeHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.environment = patch.dict(os.environ, SECRETS)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.services = RecordingServices()
        upstream_app = web.Application()
        upstream_app.router.add_post("/v1/chat/completions", self.services.upstream)
        upstream_app.router.add_post(NATIVE_PATH, self.services.native_upstream)
        runner, self.upstream_url = await start_http(upstream_app)
        self.addAsyncCleanup(runner.cleanup)
        self.services.configure(self.upstream_url)
        # The native provider has its own credential reference: no Chat upstream key is
        # reused, and a reflected native credential is detectable in this deployment.
        self.native_credential_ref = "secret-ref:fixture/native-provider"
        self.services.native_config["providers"][0]["credential_ref"] = self.native_credential_ref
        platform_app = web.Application()
        platform_app.router.add_post("/internal/v1/model-config/snapshot", self.services.snapshot)
        platform_app.router.add_post(
            "/internal/v1/model-config/native/snapshot", self.services.native_snapshot
        )
        runner, self.platform_url = await start_http(platform_app)
        self.addAsyncCleanup(runner.cleanup)
        references = {
            "secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM",
            "secret-ref:fixture/native-provider": "TS042_TEST_NATIVE_UPSTREAM",
            "secret-ref:fixture/native": "TS042_TEST_NATIVE",
            "secret-ref:fixture/native-external": "TS042_TEST_NATIVE_EXTERNAL",
            "secret-ref:fixture/native-other": "TS042_TEST_NATIVE_OTHER",
            "secret-ref:fixture/client": "TS041_TEST_CLIENT",
            "secret-ref:fixture/external": "TS041_TEST_EXTERNAL",
            "secret-ref:fixture/other": "TS041_TEST_OTHER",
            "secret-ref:fixture/platform": "TS041_TEST_PLATFORM",
        }
        self.grants = [
            grant(expires_at=(utcnow() + timedelta(minutes=10)).isoformat().replace("+00:00", "Z")),
            grant(
                service="caller-external",
                principal_id="principal-external",
                credential_ref="secret-ref:fixture/native-external",
                native_config_version=None,
                internal=False,
            ),
            grant(
                service="caller-other",
                principal_id="principal-other",
                credential_ref="secret-ref:fixture/native-other",
                native_config_versions=(),
            ),
        ]
        self.settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "native.sqlite"),
            self.platform_url,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            references,
            [registration(self.platform_url), registration(self.upstream_url + "/v1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                ),
            ],
            native_enabled=True,
            native_contract_directory=str(NATIVE_CONTRACT),
            native_clients=self.grants,
        )
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.gateway = self.app[GATEWAY]
        self.client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10), trust_env=False
        )
        self.addAsyncCleanup(self.client.close)
        self.sequence = 0

    def native_headers(self, *, version=7, request_id=None, turn=None, service="caller-fixture"):
        self.sequence += 1
        if service != "caller-fixture":
            return {"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE_EXTERNAL"]}
        headers = {
            "Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"],
            "X-Request-ID": request_id or f"native-{self.sequence}",
            "X-Tianshu-Turn-ID": turn or f"native-turn-{self.sequence}",
        }
        if version is not None:
            headers["X-Tianshu-Native-Config-Version"] = str(version)
        return headers

    def chat_headers(self, *, request_id="chat-1", turn="chat-turn-1", version=7):
        return {
            "Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"],
            "X-Request-ID": request_id,
            "X-Tianshu-Turn-ID": turn,
            "X-Tianshu-Config-Version": str(version),
            "X-Tianshu-Workload": "companion.text",
        }

    async def post_native(self, body=None, headers=None):
        return await self.client.post(
            self.url + NATIVE_PATH,
            json=native_body() if body is None else body,
            headers=self.native_headers() if headers is None else headers,
        )

    async def read_receipt(self, request_id, token="TS042_TEST_NATIVE", expected=200):
        async with self.client.get(
            self.url + RECEIPT_PATH + request_id,
            headers={"Authorization": "Bearer " + SECRETS[token]},
        ) as response:
            body = await response.text()
            self.assertEqual(response.status, expected, body)
            result = json.loads(body)
        if expected == 200:
            self.gateway.contracts.validate("native#route_receipt", result)
        return result

    def assert_native_error(self, document, code, state="not_started"):
        self.assertEqual(document["contract"], "model-protocol/v1")
        self.assertEqual(document["code"], code)
        self.assertEqual(document["execution_state"], state)
        self.assertFalse(document["retryable"])
        self.assertIsInstance(document["request_id"], str)
        self.gateway.contracts.validate("native#error", document)

    def published_contract(self):
        spec = importlib.util.spec_from_file_location(
            "native_release_validator", NATIVE_CONTRACT / "validate.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.Contract(NATIVE_CONTRACT, CONTRACT / "schemas/common.json")

    async def exchange_case(self, route, request_id, turn_id, version, expected_version=7):
        """Feed the real recorded exchange into the published normative relation logic."""
        config_request, headers = self.services.native_config_calls[-1]
        self.assertEqual(headers["Authorization"], "Bearer " + SECRETS["TS041_TEST_PLATFORM"])
        snapshot = self.services.native_snapshot_documents[-1]
        raw, sent = self.services.native_calls[-1]
        now = utcnow()
        auth = {
            **NATIVE_DOCUMENTS["trusted_access"],
            "native_config_versions": [7, 8],
            "config_versions": [999],
            "provider_ids": ["provider-fixture"],
            "credential_namespace": NAMESPACE,
            "assertion_ref": "origin-fixture",
            "expires_at": (now + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
            "revoked": False,
        }
        context = route_context(
            self.grants[0], snapshot["providers"][0], request_id, turn_id, version
        )
        case = {
            "auth": auth,
            "request": config_request,
            "snapshots": [snapshot],
            "now": now.isoformat().replace("+00:00", "Z"),
            "revoked_native_versions": [],
            "context": context,
            "receipt": route,
            "native_request": json.loads(raw),
            "upstream_request": json.loads(raw),
            "native_response": self.services.native_response,
            "observation": {
                "transport_complete": True,
                "terminal": True,
                "status": self.services.native_response["status"],
            },
        }
        self.assertEqual(sent["Authorization"], "Bearer " + SECRETS["TS042_TEST_NATIVE_UPSTREAM"])
        self.assertNotIn(SECRETS["TS041_TEST_UPSTREAM"], json.dumps(sent))
        self.assertEqual(self.published_contract().exchange(case), expected_version)
        return raw, sent

    async def test_native_json_request_and_response_bytes_are_preserved(self):
        body = native_body(
            stream=False, store=False, vendor_extension={"value": None, "list": [0, "中文"]}
        )
        raw = json.dumps(body, ensure_ascii=False, indent=2).encode()
        raw = raw[:-1] + b', "precise": 0.12345678901234567890123456789}'
        headers = self.native_headers(request_id="native-proof", turn="native-turn-proof")
        async with self.client.post(
            self.url + NATIVE_PATH,
            data=raw,
            headers={**headers, "Content-Type": "application/json"},
        ) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["X-Request-ID"], "native-proof")
            self.assertEqual(await response.read(), self.services.native_response_bytes)
            self.assertNotIn("Set-Cookie", response.headers)
        recorded, sent = self.services.native_calls[0]
        self.assertEqual(recorded, raw)
        self.assertEqual(sent["Accept-Encoding"], "identity")
        self.assertEqual(sent["Accept"], "application/json")
        for name in ("X-Request-ID", "X-Tianshu-Native-Config-Version", "X-Tianshu-Turn-ID"):
            self.assertNotIn(name, sent)
        route = await self.read_receipt("native-proof")
        self.assertEqual(route["outcome"], "succeeded")
        self.assertEqual(route["native_config_version"], 7)
        self.assertEqual(route["provider_id"], "provider-fixture")
        self.assertEqual(route["principal_id"], "principal-fixture")
        self.assertEqual(route["caller_service"], "caller-fixture")
        self.assertEqual(route["requested_model"], MODEL)
        self.assertEqual(route["resolved_model"], MODEL)
        self.assertEqual(route["response_id"], self.services.native_response["id"])
        self.assertEqual(route["native_usage"], self.services.native_response["usage"])
        self.assertEqual(route["usage"], {"input_tokens": 0, "output_tokens": 8})
        self.assertTrue(route["usage_complete"])
        self.assertEqual(route["upstream_request_id"], "native-upstream-fixture")
        await self.exchange_case(route, "native-proof", "native-turn-proof", 7)

    async def test_native_sse_bytes_single_byte_and_terminal_states(self):
        headers = self.native_headers(request_id="native-stream", turn="native-turn-stream")
        self.services.native_mode = "one_byte"
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=True),
            headers=headers,
        ) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["Content-Type"], "text/event-stream")
            self.assertEqual(await response.read(), self.services.native_stream)
        route = await self.read_receipt("native-stream")
        self.assertEqual(route["outcome"], "succeeded")
        self.assertTrue(route["usage_complete"])
        self.assertEqual(route["response_id"], self.services.native_response["id"])
        await self.exchange_case(route, "native-stream", "native-turn-stream", 7)

    async def test_native_stream_failure_states_and_missing_terminal_are_honest(self):
        self.services.native_mode = "stream"
        for status, outcome in (
            ("failed", "failed"),
            ("incomplete", "unknown"),
        ):
            self.services.native_stream = native_event(
                "response." + status,
                response={**self.services.native_response, "status": status, "usage": None},
            )
            headers = self.native_headers(request_id=f"native-{status}", turn=f"turn-{status}")
            async with self.client.post(
                self.url + NATIVE_PATH, json=native_body(stream=True), headers=headers
            ) as response:
                self.assertEqual(response.status, 200)
                await response.read()
            route = await self.read_receipt(f"native-{status}")
            self.assertEqual(route["outcome"], outcome)
            self.assertIsNone(route["usage"])
            self.assertFalse(route["usage_complete"])
        self.services.native_stream = self.services.native_stream[:-2]
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=True),
            headers=self.native_headers(request_id="native-cut", turn="turn-cut"),
        ) as response:
            self.assertEqual(response.status, 200)
            # The upstream closed by itself: the incomplete observation may not truncate it.
            self.assertEqual(await response.read(), self.services.native_stream)
        route = await self.read_receipt("native-cut")
        self.assertEqual(route["outcome"], "unknown")
        self.assertFalse(route["usage_complete"])
        self.assertEqual(len(self.services.native_calls), 3)

    async def test_observer_budget_and_unparseable_events_never_truncate_native_sse(self):
        self.services.native_mode = "stream"
        created = native_event(
            "response.created",
            response={**self.services.native_response, "status": "in_progress", "usage": None},
        )
        completed = native_event("response.completed", response=self.services.native_response)
        cases = (
            # An over-budget event exhausts the observer: bytes stay, result is unknown.
            (
                "over_budget",
                native_event("response.output_text.delta", delta="x" * 270000) + completed,
                "unknown",
                False,
            ),
            # An unknown event type is preserved and does not break a complete stream.
            (
                "unknown_event",
                created + native_event("vendor.future", opaque={"keep": True}) + completed,
                "succeeded",
                True,
            ),
            # An unparseable event invalidates observation only.
            (
                "unparseable_event",
                created
                + b"event: response.output_text.delta\r\ndata: not-json\r\n\r\n"
                + completed,
                "unknown",
                False,
            ),
        )
        for label, stream, outcome, usage_complete in cases:
            with self.subTest(label=label):
                self.services.native_stream = stream
                request_id = f"budget-{label}"
                async with self.client.post(
                    self.url + NATIVE_PATH,
                    json=native_body(stream=True),
                    headers=self.native_headers(request_id=request_id, turn=f"turn-{label}"),
                ) as response:
                    self.assertEqual(response.status, 200)
                    received = await response.read()
                self.assertEqual(received, stream)
                route = await self.read_receipt(request_id)
                self.assertEqual(route["outcome"], outcome)
                self.assertEqual(route["usage_complete"], usage_complete)
                # No local event may be appended: the last bytes are the upstream's own.
                self.assertTrue(received.endswith(stream[-32:]))

    async def test_native_upstream_errors_are_raw_and_never_retried(self):
        self.services.native_mode = "error_json"
        for status, outcome in ((400, "failed"), (429, "failed"), (503, "unknown")):
            self.services.native_http_status = status
            headers = self.native_headers(
                request_id=f"native-err-{status}", turn=f"turn-e-{status}"
            )
            async with self.client.post(
                self.url + NATIVE_PATH, json=native_body(), headers=headers
            ) as response:
                self.assertEqual(response.status, status)
                self.assertEqual(response.headers["Content-Type"], "application/json")
                self.assertNotIn("Location", response.headers)
                self.assertNotIn("Set-Cookie", response.headers)
                document = await response.json()
                self.assertEqual(document["error"]["code"], "rate_limit_exceeded")
            route = await self.read_receipt(f"native-err-{status}")
            self.assertEqual(route["outcome"], outcome)
            self.assertIsNone(route["usage"])
        self.services.native_mode = "error"
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers=self.native_headers(request_id="native-text-err", turn="turn-text-err"),
        ) as response:
            self.assertEqual(response.status, 502)
            self.assert_native_error(await response.json(), "result_unknown", "unknown")
        self.assertEqual(len(self.services.native_calls), 4)

    async def test_identity_comes_from_registration_and_routes_never_cross(self):
        headers = self.native_headers(request_id="native-id", turn="turn-id")
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(
                stream=False,
                metadata={"principal_id": "principal-forged", "caller_service": "caller-forged"},
                user="principal-forged",
            ),
            headers=headers,
        ) as response:
            self.assertEqual(response.status, 200)
        route = await self.read_receipt("native-id")
        self.assertEqual(route["principal_id"], "principal-fixture")
        self.assertEqual(route["caller_service"], "caller-fixture")
        # A client-supplied identity header is refused outright, never silently ignored.
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers={**self.native_headers(), "X-Tianshu-Principal-Id": "principal-forged"},
        ) as response:
            self.assertEqual(response.status, 400)
            self.assert_native_error(await response.json(), "invalid_input")
        # Chat credentials and Chat headers never open the native route.
        async with self.client.post(
            self.url + NATIVE_PATH, json=native_body(), headers=self.chat_headers()
        ) as response:
            self.assertEqual(response.status, 401)
            self.assert_native_error(await response.json(), "unauthorized")
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            # The native credential authenticates; the Chat protocol headers are refused.
            headers={**self.chat_headers(), **self.native_headers()},
        ) as response:
            self.assertEqual(response.status, 400)
            self.assert_native_error(await response.json(), "invalid_input")
        # Native credentials and the native version header never open the Chat route.
        async with self.client.post(
            self.url + "/v1/chat/completions",
            json={"model": "fixture-model", "messages": []},
            headers={"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"]},
        ) as response:
            self.assertEqual(response.status, 401)
        chat = self.chat_headers()
        chat["X-Tianshu-Native-Config-Version"] = "7"
        async with self.client.post(
            self.url + "/v1/chat/completions",
            json={"model": "fixture-model", "messages": []},
            headers=chat,
        ) as response:
            self.assertEqual(response.status, 400)
        self.assertEqual(len(self.services.native_calls), 1)
        self.assertEqual(self.services.calls, [])

    async def test_native_versions_are_authorized_not_inherited(self):
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers=self.native_headers(version=9, request_id="v9", turn="t9"),
        ) as response:
            self.assertEqual(response.status, 403)
            self.assert_native_error(await response.json(), "forbidden")
        # A caller whose native allowlist is empty is denied without any platform call.
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers={
                "Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE_OTHER"],
                "X-Request-ID": "v-other",
                "X-Tianshu-Turn-ID": "t-other",
                "X-Tianshu-Native-Config-Version": "7",
            },
        ) as response:
            self.assertEqual(response.status, 403)
        self.assertEqual(self.services.native_config_calls, [])
        self.assertEqual(self.services.native_calls, [])
        # Platform-selected latest is verified against the deployment allowlist.
        self.services.native_versions = {7, 9}
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers=self.native_headers(service="caller-external", request_id=None),
        ) as response:
            self.assertEqual(response.status, 403)
            self.assert_native_error(await response.json(), "forbidden")
        self.services.native_versions = {7}
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=False),
            headers=self.native_headers(service="caller-external"),
        ) as response:
            self.assertEqual(response.status, 200)
        self.assertIsNone(self.services.native_config_calls[-1][0]["native_config_version"])
        self.assertEqual(self.services.native_config_calls[-1][0]["contract"], "model-protocol/v1")
        self.assertEqual(
            self.services.native_config_calls[-1][0]["query"]["origin"]["assertion_ref"],
            "origin-fixture",
        )
        # An explicit version outside the caller's allowlist never reaches the platform.
        calls = len(self.services.native_config_calls)
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers=self.native_headers(version=9, request_id="v9b", turn="t9b"),
        ) as response:
            self.assertEqual(response.status, 403)
        self.assertEqual(len(self.services.native_config_calls), calls)

    async def test_native_config_failures_do_not_fall_back_to_chat(self):
        self.services.native_source_status = 503
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers=self.native_headers(request_id="cfg-503", turn="t-503"),
        ) as response:
            self.assertEqual(response.status, 503)
            self.assert_native_error(await response.json(), "dependency_unavailable")
        self.services.native_source_status = 200
        self.services.native_versions = {8}
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers=self.native_headers(version=7, request_id="cfg-404", turn="t-404"),
        ) as response:
            self.assertEqual(response.status, 404)
            self.assert_native_error(await response.json(), "not_found")
        self.services.native_versions = {7}
        self.services.native_source_wrong_request = True
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers=self.native_headers(request_id="cfg-wrong", turn="t-wrong"),
        ) as response:
            self.assertEqual(response.status, 409)
            self.assert_native_error(await response.json(), "version_conflict")
        self.services.native_source_wrong_request = False
        self.services.native_config["usable_until"] = "2020-01-01T00:00:00Z"
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(),
            headers=self.native_headers(request_id="cfg-expired", turn="t-expired"),
        ) as response:
            self.assertEqual(response.status, 503)
        self.assertEqual(self.services.native_calls, [])

    async def test_native_revocation_is_separate_from_chat_revocation(self):
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=False),
            headers=self.native_headers(request_id="rev-native", turn="t-rev-native"),
        ) as response:
            self.assertEqual(response.status, 200)
        # Platform 403 for this caller/version is remembered without touching Chat rows.
        self.services.native_revoked_versions = {7}
        self.gateway.native_cache.entries.clear()
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=False),
            headers=self.native_headers(request_id="rev-platform", turn="t-rev-platform"),
        ) as response:
            self.assertEqual(response.status, 403)
            self.assert_native_error(await response.json(), "forbidden")
        calls = len(self.services.native_config_calls)
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=False),
            headers=self.native_headers(request_id="rev-cached", turn="t-rev-cached"),
        ) as response:
            self.assertEqual(response.status, 403)
        self.assertEqual(len(self.services.native_config_calls), calls)
        self.assertFalse(self.gateway.diagnostics.is_revoked(7))
        # Chat version 7 keeps working with the same integer while native 7 is revoked.
        async with self.client.post(
            self.url + "/v1/chat/completions",
            json=copy.deepcopy(DOCUMENTS["native_request"]),
            headers=self.chat_headers(request_id="chat-after-native-revoke", turn="t-chat"),
        ) as response:
            self.assertEqual(response.status, 200)
        # The reverse direction: a Chat revocation never closes the native route. Another
        # trusted native subject keeps using the same integer 7.
        self.services.native_revoked_versions = set()
        self.services.native_versions = {7}
        self.gateway.native_cache.entries.clear()
        self.gateway.cache.revoke(7)
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=False),
            headers=self.native_headers(service="caller-external"),
        ) as response:
            self.assertEqual(response.status, 200)
        self.assertTrue(self.gateway.diagnostics.is_revoked(7))
        self.assertTrue(self.gateway.diagnostics.native_is_revoked(identity(), 7))
        self.assertFalse(
            self.gateway.diagnostics.native_is_revoked(
                identity(principal="principal-external", service="caller-external"), 7
            )
        )

    async def test_deployment_revoked_native_version_blocks_before_send(self):
        settings = copy.copy(self.settings)
        settings.revoked_native_versions = [7]
        settings.diagnostics_path = str(Path(self.temp.name) / "revoked.sqlite")
        app = create_app(settings)
        runner, url = await start_http(app)
        self.addAsyncCleanup(runner.cleanup)
        async with self.client.post(
            url + NATIVE_PATH, json=native_body(), headers=self.native_headers()
        ) as response:
            self.assertEqual(response.status, 403)
        self.assertEqual(self.services.native_calls, [])

    async def test_expired_or_unpermitted_registration_is_denied(self):
        settings = copy.copy(self.settings)
        settings.diagnostics_path = str(Path(self.temp.name) / "expired.sqlite")
        settings.native_clients = [
            grant(expires_at=(utcnow() - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")),
            grant(
                service="caller-external",
                principal_id="principal-external",
                credential_ref="secret-ref:fixture/native-external",
                permissions=(),
                internal=False,
            ),
        ]
        app = create_app(settings)
        runner, url = await start_http(app)
        self.addAsyncCleanup(runner.cleanup)
        async with self.client.post(
            url + NATIVE_PATH, json=native_body(), headers=self.native_headers()
        ) as response:
            self.assertEqual(response.status, 403)
        async with self.client.post(
            url + NATIVE_PATH,
            json=native_body(),
            headers={"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE_EXTERNAL"]},
        ) as response:
            self.assertEqual(response.status, 403)
        self.assertEqual(self.services.native_config_calls, [])

    async def test_invalid_registration_cannot_read_receipts_and_version_revocation_keeps_audit(
        self,
    ):
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=False),
            headers=self.native_headers(request_id="audit-own", turn="audit-turn"),
        ) as response:
            self.assertEqual(response.status, 200)
        # Revoking the native *version* stops new routing but never erases the audit row:
        # the two concepts are deliberately independent.
        self.gateway.native_cache.revoke(self.grants[0], 7)
        route = await self.read_receipt("audit-own")
        self.assertEqual(route["outcome"], "succeeded")
        for label, replacement in (
            ("revoked", replace(self.grants[0], revoked=True)),
            ("expired", replace(self.grants[0], expires_at="2000-01-01T00:00:00Z")),
        ):
            with self.subTest(label=label):
                settings = copy.copy(self.settings)
                settings.native_clients = [replacement, *self.grants[1:]]
                app = create_app(settings)
                runner, url = await start_http(app)
                self.addAsyncCleanup(runner.cleanup)
                # The caller's own historical receipt is refused: an unusable registration
                # is not an identity, even for rows it wrote itself.
                async with self.client.get(
                    url + RECEIPT_PATH + "audit-own",
                    headers={"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"]},
                ) as response:
                    self.assertEqual(response.status, 403)
                    self.assert_native_error(await response.json(), "forbidden")
                # Cross-subject isolation for a still-valid caller is unchanged.
                async with self.client.get(
                    url + RECEIPT_PATH + "audit-own",
                    headers={"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE_EXTERNAL"]},
                ) as response:
                    self.assertEqual(response.status, 404)
                # New native work is refused as well.
                async with self.client.post(
                    url + NATIVE_PATH,
                    json=native_body(stream=False),
                    headers=self.native_headers(),
                ) as response:
                    self.assertEqual(response.status, 403)

    async def test_native_cli_subprocess_serves_route_from_json_settings(self):
        """A real CLI start from a deployment JSON document whose arrays are lists."""
        # Exactly what the CLI parses from disk: dataclasses.asdict keeps tuples, so the
        # round trip through JSON text is the boundary this test must exercise.
        document = json.loads(json.dumps(asdict(self.settings)))
        document["diagnostics_path"] = str(Path(self.temp.name) / "cli-native.sqlite")
        self.assertIsInstance(document["native_clients"][0]["native_config_versions"], list)
        self.assertIsInstance(document["native_clients"][0]["provider_ids"], list)
        path = Path(self.temp.name) / "native-settings.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        command = [
            sys.executable,
            "-B",
            "-m",
            "tianshu_gateway",
            "--settings",
            str(path),
            "--port",
            str(port),
            "--local-test",
        ]
        flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=flags,
        )
        try:
            async with asyncio.timeout(5):
                while True:
                    try:
                        async with self.client.post(
                            f"http://127.0.0.1:{port}" + NATIVE_PATH,
                            json=native_body(stream=False),
                            headers=self.native_headers(
                                request_id="cli-native", turn="cli-native-turn"
                            ),
                        ) as response:
                            self.assertEqual(response.status, 200)
                            break
                    except aiohttp.ClientConnectorError:
                        await asyncio.sleep(0.03)
            async with self.client.get(
                f"http://127.0.0.1:{port}" + RECEIPT_PATH + "cli-native",
                headers={"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"]},
            ) as response:
                self.assertEqual(response.status, 200)
                receipt = await response.json()
            self.assertEqual(receipt["resolved_model"], MODEL)
            self.assertEqual(receipt["native_config_version"], 7)
            self.assertIsNone(receipt.get("config_version"))
        finally:
            if process.returncode is None:
                process.terminate()
            out, error = await asyncio.wait_for(process.communicate(), 5)
        for secret in SECRETS.values():
            self.assertNotIn(secret.encode(), out + error)

    async def test_state_references_size_and_unknown_targets_fail_before_send(self):
        for update, code, status in (
            ({"previous_response_id": "resp_fixture"}, "state_reference_unsupported", 409),
            ({"conversation": "conv_fixture"}, "state_reference_unsupported", 409),
            ({"prompt": {"id": "pmpt_fixture"}}, "state_reference_unsupported", 409),
            ({"background": True}, "unsupported_operation", 501),
            (
                {"input": [{"type": "item_reference", "id": "item_fixture"}]},
                "state_reference_unsupported",
                409,
            ),
            (
                {"input": [{"role": "user", "content": [{"type": "input_file", "file_id": "f"}]}]},
                # A file_id is a server-side reference, so it is refused as a state
                # reference before the input_file operation itself is classified.
                "state_reference_unsupported",
                409,
            ),
            (
                {"input": [{"role": "user", "content": [{"type": "input_audio"}]}]},
                "unsupported_operation",
                501,
            ),
            ({"model": "other-model"}, "invalid_input", 400),
        ):
            with self.subTest(update=update):
                async with self.client.post(
                    self.url + NATIVE_PATH,
                    json=native_body(**update),
                    headers=self.native_headers(),
                ) as response:
                    self.assertEqual(response.status, status)
                    self.assert_native_error(await response.json(), code)
        self.settings.max_request_bytes = 32
        async with self.client.post(
            self.url + NATIVE_PATH, json=native_body(), headers=self.native_headers()
        ) as response:
            self.assertEqual(response.status, 413)
            self.assert_native_error(await response.json(), "payload_too_large")
        self.settings.max_request_bytes = 1_048_576
        async with self.client.post(
            self.url + NATIVE_PATH,
            json={"model": MODEL, "input": "hi"},
            headers=self.native_headers(request_id="dup", turn="t-dup"),
        ) as response:
            self.assertEqual(response.status, 200)
        async with self.client.post(
            self.url + NATIVE_PATH,
            json={"model": MODEL, "input": "hi"},
            headers=self.native_headers(request_id="dup", turn="t-dup"),
        ) as response:
            self.assertEqual(response.status, 409)
            self.assert_native_error(await response.json(), "idempotency_conflict")
        self.assertEqual(len(self.services.native_calls), 1)

    async def test_receipt_reads_are_scoped_and_unknown_ids_do_not_leak(self):
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=False),
            headers=self.native_headers(
                request_id="scope-1", turn="t-scope-1", service="caller-external"
            ),
        ) as response:
            self.assertEqual(response.status, 200)
            # An external caller supplies no internal headers, so the gateway assigns the
            # correlation ID; a client-chosen ID is never accepted as an address.
            request_id = response.headers["X-Request-ID"]
            self.assertNotEqual(request_id, "scope-1")
        async with self.client.get(
            self.url + RECEIPT_PATH + request_id,
            headers={"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"]},
        ) as response:
            # Another subject of the same deployment never learns that this ID exists.
            self.assertEqual(response.status, 404)
        route = await self.read_receipt(request_id, token="TS042_TEST_NATIVE_EXTERNAL")
        self.assertEqual(route["request_id"], request_id)
        self.assertEqual(route["caller_service"], "caller-external")
        self.assertEqual(route["principal_id"], "principal-external")
        async with self.client.get(
            self.url + RECEIPT_PATH + "missing-id",
            headers={"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE"]},
        ) as response:
            self.assertEqual(response.status, 404)
            self.assert_native_error(await response.json(), "not_found")
        async with self.client.get(
            self.url + RECEIPT_PATH + request_id,
            headers={"Authorization": "Bearer " + SECRETS["TS042_TEST_NATIVE_OTHER"]},
        ) as response:
            # This registration carries no native version allowlist, so it is not a usable
            # identity at all: it is refused before any row lookup (never a 200, and not a
            # 404 that would imply the identity itself were valid).
            self.assertEqual(response.status, 403)
            self.assert_native_error(await response.json(), "forbidden")
        async with self.client.get(
            self.url + RECEIPT_PATH + request_id,
            headers={"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]},
        ) as response:
            self.assertEqual(response.status, 401)
        async with self.client.get(self.url + RECEIPT_PATH + request_id) as response:
            self.assertEqual(response.status, 401)

    async def test_native_timeout_and_cancel_report_unknown(self):
        self.services.native_config["bindings"][0]["timeout_ms"] = 150
        self.services.native_mode = "sse_hold"
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=True),
            headers=self.native_headers(request_id="native-slow", turn="t-native-slow"),
        ) as response:
            self.assertEqual(response.status, 200)
            with self.assertRaises(aiohttp.ClientPayloadError):
                await response.read()
        route = await self.read_receipt("native-slow")
        self.assertEqual(route["outcome"], "unknown")
        reason = self.gateway.diagnostics.connection.execute(
            "SELECT reason FROM native_requests WHERE request_id=?", ("native-slow",)
        ).fetchone()[0]
        self.assertEqual(reason, "timeout_unknown")
        self.assertEqual(len(self.services.native_calls), 1)

    async def test_native_secrets_are_never_echoed(self):
        self.services.native_mode = "secret_stream"
        async with self.client.post(
            self.url + NATIVE_PATH,
            json=native_body(stream=True),
            headers=self.native_headers(request_id="native-secret", turn="t-native-secret"),
        ) as response:
            self.assertEqual(response.status, 200)
            with self.assertRaises(aiohttp.ClientPayloadError):
                await response.read()
        route = await self.read_receipt("native-secret")
        self.assertNotIn(SECRETS["TS042_TEST_NATIVE_UPSTREAM"], json.dumps(route))
        self.assertEqual(route["outcome"], "unknown")

    async def test_native_diagnostics_write_failure_stays_unknown(self):
        def disk_failure(*args):
            raise OSError("fixture failure " + SECRETS["TS042_TEST_NATIVE_UPSTREAM"])

        with patch.object(self.gateway.native_ledger, "native_finish", side_effect=disk_failure):
            with self.assertLogs("tianshu_gateway", level="ERROR") as logs:
                async with self.client.post(
                    self.url + NATIVE_PATH,
                    json=native_body(stream=False),
                    headers=self.native_headers(request_id="native-disk", turn="t-native-disk"),
                ) as response:
                    self.assertEqual(response.status, 502)
                    self.assert_native_error(await response.json(), "result_unknown", "unknown")
        self.assertNotIn(SECRETS["TS042_TEST_NATIVE_UPSTREAM"], " ".join(logs.output))
        route = await self.read_receipt("native-disk")
        self.assertEqual(route["outcome"], "unknown")
        self.assertEqual(
            self.gateway.diagnostics.connection.execute(
                "SELECT reason FROM native_requests WHERE request_id=?", ("native-disk",)
            ).fetchone()[0],
            "in_flight",
        )


class NativeDisabledTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.environment = patch.dict(os.environ, SECRETS)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "disabled.sqlite"),
            "http://127.0.0.1:1",
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            {"secret-ref:fixture/client": "TS041_TEST_CLIENT"},
            [registration("http://127.0.0.1:1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                )
            ],
        )
        self.app = create_app(self.settings)
        self.runner, self.url = await start_http(self.app)
        self.addAsyncCleanup(self.runner.cleanup)
        self.client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=False)
        self.addAsyncCleanup(self.client.close)

    async def test_native_is_off_by_default_without_native_material(self):
        self.assertFalse(self.app[GATEWAY].contracts.native)
        self.assertIsNone(self.app[GATEWAY].native_cache)
        headers = {"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]}
        for method in ("post", "get"):
            async with getattr(self.client, method)(
                self.url + NATIVE_PATH, headers=headers
            ) as response:
                self.assertEqual(response.status, 501)
                document = await response.json()
                self.assertEqual(document["contract"], "model-protocol/v1")
                self.assertEqual(document["code"], "unsupported_operation")
        async with self.client.get(self.url + RECEIPT_PATH + "any-id", headers=headers) as response:
            self.assertEqual(response.status, 404)

    async def test_native_material_without_enable_is_rejected(self):
        settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "half.sqlite"),
            "http://127.0.0.1:1",
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            {"secret-ref:fixture/client": "TS041_TEST_CLIENT"},
            [registration("http://127.0.0.1:1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                )
            ],
            native_contract_directory=str(NATIVE_CONTRACT),
        )
        with self.assertRaises(ValueError):
            create_app(settings)
        settings = Settings(
            str(CONTRACT),
            str(Path(self.temp.name) / "enabled.sqlite"),
            "http://127.0.0.1:1",
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            {"secret-ref:fixture/client": "TS041_TEST_CLIENT"},
            [registration("http://127.0.0.1:1")],
            [],
            native_enabled=True,
            native_contract_directory=str(NATIVE_CONTRACT),
        )
        with self.assertRaises(ValueError):
            create_app(settings)


if __name__ == "__main__":
    unittest.main()
