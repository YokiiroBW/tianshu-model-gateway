"""Runs actual read-only legacy Python, never a copy or live transport."""
import json
import hashlib
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
legacy = os.environ.get("LEGACY_ROUTING_ROOT")
if not legacy:
    context = json.loads((ROOT / ".runtime/workspace-context.json").read_text(encoding="utf-8"))
    legacy = str(Path(context["workspace"]) / "references/legacy-routing")
LEGACY = Path(legacy).resolve()
if not (LEGACY / "src/aigateway/dataplane.py").is_file():
    raise RuntimeError("Set LEGACY_ROUTING_ROOT to the read-only legacy-routing checkout")
sys.path.insert(0, str(LEGACY / "src"))

from aigateway.dataplane import NativeClientRequest, NativeRequestPlanner
from aigateway.domain import NativeProtocol, ReasoningEffort
from aigateway.errors import ProtocolRequestInvalid

FIXTURES = json.loads((ROOT / "tests/fixtures/native.json").read_text(encoding="utf-8"))


class LegacyCharacterizationTests(unittest.TestCase):
    def test_reviewed_source_hashes_match(self):
        evidence = json.loads((ROOT / "tests/fixtures/legacy-source.json").read_text(encoding="utf-8-sig"))
        for relative, expected in evidence["files"].items():
            with self.subTest(source=relative):
                self.assertEqual(hashlib.sha256((LEGACY / relative).read_bytes()).hexdigest(), expected,
                                 "Legacy source changed: review evidence before updating hashes")

    def plan(self, fixture, body=None, headers=None, path=None):
        route = SimpleNamespace(protocol=NativeProtocol(fixture["protocol"]),
                                api_base_url="https://fixture.invalid/v1",
                                model="legacy-route-model", reasoning_effort=ReasoningEffort.HIGH)
        request = NativeClientRequest("POST", path or fixture["path"],
                                      fixture["headers"] if headers is None else headers,
                                      fixture["body"] if body is None else body)
        return NativeRequestPlanner().prepare(request, route)

    def test_actual_legacy_silently_overrides_model_and_effort(self):
        for fixture, field in zip(FIXTURES["requests"],
                                  ("reasoning", "reasoning_effort", "output_config")):
            with self.subTest(protocol=fixture["protocol"]):
                result = dict(self.plan(fixture).body)
                self.assertEqual(result["model"], "legacy-route-model")
                self.assertNotEqual(result, fixture["body"])
                if field == "reasoning_effort":
                    self.assertEqual(result[field], "high")
                    result[field] = "low"
                else:
                    self.assertEqual(result[field]["effort"], "high")
                    result[field]["effort"] = "low"
                result["model"] = "fixture-client-model"
                self.assertEqual(result, fixture["body"])

    def test_actual_legacy_headers_keep_protocol_drop_client_auth(self):
        for fixture in FIXTURES["requests"]:
            result = self.plan(fixture)
            self.assertNotIn("authorization", result.headers)
            self.assertNotIn("x-api-key", result.headers)
            for key, value in fixture["headers"].items():
                if key.lower() not in {"authorization", "x-api-key"}:
                    self.assertEqual(result.headers[key.lower()], value)
            self.assertEqual(result.url, "https://fixture.invalid" + fixture["path"])

    def test_actual_legacy_rejects_invalid_stream_nan_and_header_controls(self):
        fixture = FIXTURES["requests"][0]
        for change in ({"stream": "true"}, {"x": float("nan")}, {"reasoning": None}):
            with self.subTest(change=change), self.assertRaises(ProtocolRequestInvalid):
                self.plan(fixture, body={**fixture["body"], **change})
        with self.assertRaises(ProtocolRequestInvalid):
            self.plan(fixture, headers={"accept": "a\r\nb"})

    def test_actual_legacy_has_no_embeddings_or_state_retrieval_path(self):
        fixture = FIXTURES["requests"][0]
        for path in ("/v1/embeddings", "/v1/responses/resp_fixture", "/v1/responses/compact"):
            with self.subTest(path=path), self.assertRaises(ProtocolRequestInvalid):
                self.plan(fixture, path=path)

    def test_actual_legacy_requires_global_reasoning_enum(self):
        for value in ("none", "minimal", "provider-future"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ReasoningEffort(value)


if __name__ == "__main__":
    unittest.main()
