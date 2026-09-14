"""Candidate-only schemas; never registered as gateway runtime contracts."""

import copy
import json
import os
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE = ROOT / "docs/candidates/model-protocol-v1"
WORKSPACE = (
    Path(os.environ["TIANSHU_WORKSPACE"])
    if os.environ.get("TIANSHU_WORKSPACE")
    else Path(json.loads((ROOT / ".runtime/workspace-context.json").read_text())["workspace"])
)


def read(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


class ResponsesCandidateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = read(CANDIDATE / "schema.json")
        cls.old = read(WORKSPACE / "contracts/text-dialogue/v1/schemas/model.json")
        common = read(WORKSPACE / "contracts/text-dialogue/v1/schemas/common.json")
        cls.registry = Registry().with_resources(
            (s["$id"], Resource.from_contents(s)) for s in (cls.schema, cls.old, common)
        )
        cls.examples = read(CANDIDATE / "examples.json")

    def validator(self, kind, old=False):
        schema = self.old if old else self.schema
        return Draft202012Validator(
            {"$ref": schema["$id"] + "#/$defs/" + kind},
            registry=self.registry,
            format_checker=FormatChecker(),
        )

    def test_schema_and_positive_examples(self):
        Draft202012Validator.check_schema(self.schema)
        for example in self.examples:
            with self.subTest(example=example["id"]):
                self.validator(example["schema"]).validate(example["document"])

    def test_negative_examples(self):
        for example in read(CANDIDATE / "negative-examples.json"):
            with self.subTest(example=example["id"]):
                self.assertTrue(
                    list(self.validator(example["schema"]).iter_errors(example["document"]))
                )

    def test_chat_contract_stays_separate(self):
        for example in self.examples:
            if example["schema"] not in {"config_request", "native_request", "native_response"}:
                with self.subTest(example=example["id"]):
                    self.assertTrue(
                        list(
                            self.validator(example["schema"], old=True).iter_errors(
                                example["document"]
                            )
                        )
                    )
        for name in ("provider", "route_context", "route_receipt"):
            self.assertEqual(
                self.old["$defs"][name]["properties"]["protocol"],
                {"const": "openai-chat-completions"},
            )

    def test_opaque_native_values_and_store_are_not_policy_defaults(self):
        base = next(e["document"] for e in self.examples if e["id"] == "native_request")
        for value in (None, False, True):
            document = copy.deepcopy(base)
            if value is not None:
                document["store"] = value
            document["reasoning"] = {"effort": "fixture-future-value", "extra": 0}
            document["tools"][0]["parameters"]["properties"]["file_id"] = {"type": "string"}
            self.validator("native_request").validate(document)

    def test_manifest_does_not_claim_publication(self):
        manifest = read(CANDIDATE / "manifest.json")
        self.assertEqual(manifest["status"], "candidate_unpublished")
        self.assertEqual(manifest["enabled_runtime_routes"], [])
        self.assertEqual(manifest["producer_confirmation"], "pending_platform_increment")

    def test_joint_fixture_agrees_on_route_and_unchanged_parameters(self):
        docs = {e["id"]: e["document"] for e in self.examples}
        config, context, receipt = (
            docs["config_response"],
            docs["route_context"],
            docs["route_receipt"],
        )
        provider, binding = config["providers"][0], config["bindings"][0]
        for name in ("contract", "config_version"):
            self.assertEqual(config[name], context[name])
            self.assertEqual(config[name], receipt[name])
        for name in ("credential_namespace", "protocol"):
            self.assertEqual(provider[name], context[name])
            self.assertEqual(provider[name], receipt[name])
        for name in ("principal_id", "caller_service"):
            self.assertEqual(context[name], receipt[name])
        self.assertEqual(provider["provider_id"], binding["provider_id"])
        self.assertEqual(provider["provider_id"], receipt["provider_id"])
        self.assertEqual(provider["model_id"], binding["model_id"])
        self.assertEqual(provider["model_id"], docs["native_request"]["model"])
        self.assertEqual(receipt["requested_model"], docs["native_request"]["model"])
        self.assertEqual(receipt["requested_model"], receipt["resolved_model"])
        self.assertEqual(receipt["requested_reasoning"], receipt["effective_reasoning"])
        self.assertEqual(
            receipt["effective_reasoning"], {"reasoning": docs["native_request"]["reasoning"]}
        )
        self.assertEqual(receipt["response_id"], docs["native_response"]["id"])
