import copy
import shutil
import tempfile
import unittest
from pathlib import Path

from gateway_fixtures import CONTRACT, DOCUMENTS, STREAM, registration
from tianshu_gateway.config import RegisteredTargets, timestamp, validate_snapshot
from tianshu_gateway.contracts import Contracts, Rejected, loads
from tianshu_gateway.routing import StreamObserver, apply_fields, record_usage


class GatewayBoundaryTests(unittest.TestCase):
    def test_contract_hash_pin_normalizes_crlf_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "contract"
            shutil.copytree(CONTRACT, root)
            path = root / "schemas/model.json"
            source = path.read_bytes().replace(b"\r\n", b"\n")
            path.write_bytes(source.replace(b"\n", b"\r\n"))
            Contracts(root).validate("model#native_request", DOCUMENTS["native_request"])
            path.write_bytes(source + b" ")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                Contracts(root)

    def test_sse_observer_all_split_boundaries_and_bounded_malformed_events(self):
        for index in range(len(STREAM) + 1):
            observer = StreamObserver()
            observer.feed(STREAM[:index])
            observer.feed(STREAM[index:])
            self.assertTrue(observer.complete, index)
        for data in (b"data: " + b"x" * 64, b"data: not-json\n\n", STREAM + b"data: [DONE]\n\n"):
            observer = StreamObserver(limit=32)
            observer.feed(data)
            self.assertFalse(observer.complete)
            self.assertTrue(observer.invalid)
            self.assertLessEqual(len(observer.buffer), 32)
        observer = StreamObserver()
        observer.feed(STREAM.replace(b"\r\n", b"\r"))
        observer.end()
        self.assertTrue(observer.complete)

    def test_json_rejects_duplicate_nonfinite_and_out_of_range_numbers(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}', b'{"a":1e999}', b"\xff"):
            with self.assertRaises(Rejected):
                loads(raw)

    def test_sse_event_error_field_variants_never_complete(self):
        prefix = STREAM.removesuffix(b"data: [DONE]\r\n\r\n")
        for newline in (b"\n", b"\r\n", b"\r"):
            for field in (b"event:error", b"event: error"):
                stream = prefix.replace(b"\r\n", newline) + newline.join(
                    (
                        field,
                        b'data:{"message":',
                        b'data: "provider failed"}',
                        b"",
                        b"data:[DONE]",
                        b"",
                        b"",
                    )
                )
                for index in range(len(stream) + 1):
                    with self.subTest(newline=newline, field=field, split=index):
                        observer = StreamObserver()
                        observer.feed(stream[:index])
                        observer.feed(stream[index:])
                        observer.end()
                        self.assertTrue(observer.error)
                        self.assertTrue(observer.done)
                        self.assertFalse(observer.invalid)
                        self.assertFalse(observer.complete)

    def test_sse_event_field_literal_values_last_value_and_reset(self):
        prefix = STREAM.removesuffix(b"data: [DONE]\r\n\r\n")
        for fields in (
            b"event:  error",
            b"event:\terror",
            b"Event:error",
            b":event:error",
            b"event:error\nevent:message",
            b"event:error\nevent",
            b"event:error\n\n",
        ):
            with self.subTest(fields=fields):
                observer = StreamObserver()
                observer.feed(prefix + fields + b'\ndata:{"message":"fixture"}\n\ndata:[DONE]\n\n')
                self.assertFalse(observer.error)
                self.assertTrue(observer.complete)
        for fields, data in (
            (b"event:message\nevent:error", b'data:{"message":"failed"}'),
            (b"event:error", b"data:[DONE]"),
            (b"event:error", b"data"),
        ):
            with self.subTest(fields=fields, data=data):
                observer = StreamObserver()
                observer.feed(prefix + fields + b"\n" + data + b"\n\n")
                self.assertTrue(observer.error)
                self.assertFalse(observer.complete)

    def test_field_patch_adds_missing_and_replaces_escaped_keys(self):
        raw = b' { "\\u006dodel":"old", "messages": [], "x": 0.123456789012345678901 } '
        effective = {"model": "new", "reasoning_effort": None}
        result = apply_fields(raw, effective, [{"field": "model"}, {"field": "reasoning_effort"}])
        self.assertIn(b'"\\u006dodel":"new"', result)
        self.assertIn(b'"x": 0.123456789012345678901', result)
        self.assertIsNone(loads(result)["reasoning_effort"])

    def test_usage_never_invents_counts_or_total_cost(self):
        for native in (
            None,
            {},
            {"prompt_tokens": True},
            {"prompt_tokens": -1},
            {"prompt_tokens": 0},
            {"completion_tokens": 2, "vendor_price": None},
        ):
            receipt = {"usage": None, "native_usage": None, "usage_complete": False}
            record_usage(receipt, native, True)
            self.assertFalse(receipt["usage_complete"])
            self.assertNotIn("cost", receipt)
        receipt = {}
        record_usage(receipt, {"prompt_tokens": 0, "completion_tokens": 0}, True)
        self.assertEqual(receipt["usage"], {"input_tokens": 0, "output_tokens": 0})
        self.assertTrue(receipt["usage_complete"])

    def test_publication_validator_rejects_ambiguous_workload_at_exact_expiry(self):
        contracts = Contracts(CONTRACT)
        document = copy.deepcopy(DOCUMENTS["config"])
        targets = RegisteredTargets(
            [{"base_url": document["providers"][0]["base_url"], "addresses": ["192.0.2.1"]}]
        )
        validate_snapshot(contracts, document, 7, targets, timestamp(document["published_at"]))
        with self.assertRaises(Rejected):
            validate_snapshot(contracts, document, 7, targets, timestamp(document["usable_until"]))
        document["bindings"].append(copy.deepcopy(document["bindings"][0]))
        with self.assertRaises(Rejected):
            validate_snapshot(contracts, document, 7, targets, timestamp(document["published_at"]))

    def test_target_registration_rejects_unsafe_or_unreviewed_boundaries(self):
        for base in (
            "http://user:password@127.0.0.1:123/v1",
            "http://127.0.0.1:123/v1?url=evil",
            "http://127.0.0.1:123/../v1",
            "file:///tmp/key",
            "http://127.0.0.1:123/v1/",
        ):
            with self.assertRaises(ValueError):
                RegisteredTargets([registration(base)])
        with self.assertRaises(ValueError):
            RegisteredTargets([{"base_url": "http://127.0.0.1:123/v1", "addresses": ["127.0.0.1"]}])
        with self.assertRaises(ValueError):
            RegisteredTargets(
                [{"base_url": "https://127.0.0.1:123/v1", "addresses": ["127.0.0.2"]}]
            )


if __name__ == "__main__":
    unittest.main()
