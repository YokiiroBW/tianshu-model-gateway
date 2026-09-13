import json
import unittest
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

from protocol_lab import (Rejected, Route, can_fallback, check_embedding, prepare,
                          relay, select_route, usage_record)

FIXTURES = json.loads((Path(__file__).parent / "fixtures/native.json").read_text(encoding="utf-8"))


class NativeFidelityTests(unittest.TestCase):
    def test_external_payloads_preserve_every_field_and_do_not_alias(self):
        for fixture in FIXTURES["requests"]:
            with self.subTest(protocol=fixture["protocol"]):
                original = deepcopy(fixture["body"])
                result, changes = prepare(original)
                self.assertEqual(result, fixture["body"])
                self.assertEqual(changes, [])
                result["x_fixture_extension"]["list"].append("changed")
                self.assertEqual(original, fixture["body"])

    def test_defaults_only_fill_absent_fields(self):
        for value in (None, False, 0, "", "low", "future-effort"):
            with self.subTest(value=value):
                result, changes = prepare({"reasoning": {"effort": value, "summary": "auto"}},
                                          mode="defaults", parameters={"reasoning.effort": "high"})
                self.assertEqual(result["reasoning"]["effort"], value)
                self.assertEqual(changes, [])
        result, changes = prepare({"reasoning": {"summary": "auto"}}, mode="defaults",
                                  parameters={"reasoning.effort": "high"})
        self.assertEqual(result["reasoning"], {"summary": "auto", "effort": "high"})
        self.assertFalse(changes[0]["was_present"])

    def test_explicit_mapping_is_limited_and_reported(self):
        for fixture, field in zip(FIXTURES["requests"],
                                  ("reasoning.effort", "reasoning_effort", "output_config.effort")):
            with self.subTest(protocol=fixture["protocol"]):
                result, changes = prepare(fixture["body"], mode="explicit_mapping",
                                          parameters={"model": "fixture-mapped", field: "high"})
                self.assertEqual([c["path"] for c in changes], ["model", field])
                self.assertEqual([c["before"] for c in changes], ["fixture-client-model", "low"])
                self.assertEqual([c["after"] for c in changes], ["fixture-mapped", "high"])
                result["model"] = "fixture-client-model"
                if "." in field:
                    parent, key = field.split(".")
                    result[parent][key] = "low"
                else:
                    result[field] = "low"
                self.assertEqual(result, fixture["body"])

    def test_ambiguous_policy_and_null_parent_fail_closed(self):
        for kwargs in ({"mode": "implicit"}, {"parameters": {"model": "other"}},
                       {"mode": "explicit_mapping", "parameters": {"tools": []}},
                       {"mode": "defaults", "parameters": {"reasoning.effort": "high"}}):
            with self.subTest(kwargs=kwargs), self.assertRaises(Rejected):
                prepare({"reasoning": None}, **kwargs)

    def test_sse_bytes_survive_every_two_chunk_boundary(self):
        for fixture in FIXTURES["streams"]:
            wire = fixture["wire"].encode("utf-8")
            for split in range(len(wire) + 1):
                with self.subTest(protocol=fixture["protocol"], split=split):
                    chunks = [wire[:split], wire[split:]]
                    self.assertEqual(list(relay(iter(chunks))), chunks)

    def test_relay_is_lazy_and_does_not_swallow_transport_failure(self):
        pulled = []
        def upstream():
            pulled.append(1)
            yield b"event: partial\n"
            raise TimeoutError("fixture interrupted after output")
        stream = relay(upstream())
        self.assertEqual(pulled, [])
        self.assertEqual(next(stream), b"event: partial\n")
        self.assertEqual(pulled, [1])
        with self.assertRaises(TimeoutError):
            next(stream)
        self.assertEqual(pulled, [1])

    def test_fidelity_oracle_detects_field_and_wire_loss(self):
        # Negative controls: these corruptions must not compare equal to the golden payload.
        for fixture in FIXTURES["requests"]:
            for key in fixture["body"]:
                damaged = deepcopy(fixture["body"])
                del damaged[key]
                with self.subTest(protocol=fixture["protocol"], deleted=key):
                    self.assertNotEqual(damaged, fixture["body"])
        for fixture in FIXTURES["streams"]:
            wire = fixture["wire"].encode("utf-8")
            self.assertNotEqual(wire[:-1], wire)


class RoutingBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.primary = Route("provider-a", "credential-scope-a", "config-1",
                             "openai_chat_completions", "fixture-model",
                             frozenset({"text", "tools", "streaming"}), "effort=low")
        self.backup = replace(self.primary, provider="provider-b", credential_namespace="scope-b")

    def test_new_turn_uses_current_but_references_pin_entire_snapshot(self):
        new = replace(self.primary, config_version="config-2", model="new-model")
        pins = {("caller-a", "resp_fixture"): self.primary,
                ("caller-a", "file_fixture"): self.primary}
        self.assertEqual(select_route(new, (), pins, principal="caller-a"), new)
        self.assertEqual(select_route(new, ("resp_fixture", "file_fixture"), pins,
                                      principal="caller-a"), self.primary)

    def test_unknown_cross_principal_and_conflicting_references_rejected(self):
        pins = {("caller-a", "resp_fixture"): self.primary,
                ("caller-a", "conversation_fixture"): self.backup}
        for principal, references in (("caller-b", ("resp_fixture",)),
                                      ("caller-a", ("missing",)),
                                      ("caller-a", ("resp_fixture", "conversation_fixture"))):
            with self.subTest(principal=principal, references=references), self.assertRaises(Rejected):
                select_route(self.backup, references, pins, principal=principal)

    def test_fallback_positive_and_negative_matrix(self):
        base = dict(configured=True, required=frozenset({"text", "tools"}),
                    outcome="not_sent", attempts=1, limit=2)
        self.assertTrue(can_fallback(self.primary, self.backup, **base))
        self.assertTrue(can_fallback(self.primary, self.backup,
                                    **{**base, "outcome": "rejected_without_execution"}))
        for change in ({"configured": False}, {"attempts": 2}, {"attempts": -1}, {"limit": 0},
                       {"stream_started": True}, {"tool_may_have_run": True},
                       {"references": ("resp_fixture",)}, {"outcome": "unknown"},
                       {"outcome": "timeout_after_send"}, {"outcome": "success"}):
            with self.subTest(change=change):
                self.assertFalse(can_fallback(self.primary, self.backup, **{**base, **change}))
        for backup in (replace(self.backup, protocol="anthropic_messages"),
                       replace(self.backup, model="different-model"),
                       replace(self.backup, capabilities=frozenset({"text"})),
                       replace(self.backup, parameter_signature="effort=high")):
            with self.subTest(backup=backup):
                self.assertFalse(can_fallback(self.primary, backup, **base))

    def test_unknown_usage_is_distinct_from_zero_and_partial(self):
        self.assertEqual(usage_record(None, completed=True)["usage_status"], "unknown")
        reported = {"input_tokens": 0, "output_tokens": 0, "future_detail": {"cached": 0}}
        record = usage_record(reported, completed=False)
        self.assertEqual(record["native_usage"], reported)
        self.assertEqual(record["usage_status"], "reported")
        self.assertFalse(record["complete"])
        self.assertIsNone(record["cost"])
        self.assertEqual(record["price_status"], "unknown")
        self.assertNotIn("total_tokens", record["native_usage"])
        reported["future_detail"]["cached"] = 99
        self.assertEqual(record["native_usage"]["future_detail"]["cached"], 0)

    def test_embedding_same_dimensions_do_not_allow_space_switch(self):
        collection = dict(provider="provider-a", model="fixture-embed", revision="r1",
                          dimensions=3, preprocessing="normalize-v1", space="space-a")
        check_embedding(collection, deepcopy(collection))
        for field in collection:
            with self.subTest(field=field), self.assertRaises(Rejected):
                check_embedding(collection, {**collection, field: "changed"})
            missing = deepcopy(collection)
            del missing[field]
            with self.subTest(missing=field), self.assertRaises(Rejected):
                check_embedding(collection, missing)


if __name__ == "__main__":
    unittest.main()
