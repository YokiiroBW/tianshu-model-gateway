"""Portable validator acceptance; copies only release files and explicit common.json."""

import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("release_validator", ROOT / "validate.py")
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)
COMMON = None


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.root = self.path / "release"
        shutil.copytree(ROOT, self.root, ignore=shutil.ignore_patterns("__pycache__"))
        self.common = self.path / "common.json"
        shutil.copyfile(COMMON, self.common)

    def run_validator(self):
        environment = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("TIANSHU_", "PYTHON", "LEGACY_"))
        }
        return subprocess.run(
            [
                sys.executable,
                "-I",
                "-B",
                str(self.root / "validate.py"),
                "--common",
                str(self.common),
            ],
            cwd=self.path,
            env=environment,
            capture_output=True,
            timeout=20,
        )

    def refresh(self, relative):
        manifest = validator.load(self.root / "manifest.json")
        manifest["sha256"][relative] = validator.digest(self.root / relative)
        (self.root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    def test_isolated_copied_package_has_no_product_or_workspace_dependency(self):
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        summary = json.loads(result.stdout)
        self.assertGreaterEqual(summary["relations"], 40)
        self.assertEqual(summary["result"], "valid")

    def test_crlf_conversion_keeps_dependency_and_package_identity(self):
        for path in [self.common, *self.root.rglob("*")]:
            if path.is_file():
                path.write_bytes(validator.normalized(path).replace(b"\n", b"\r\n"))
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_wrong_or_missing_common_fails_without_fallback(self):
        self.common.write_bytes(self.common.read_bytes() + b" ")
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"dependency_unavailable", result.stderr)
        self.common.unlink()
        self.assertNotEqual(self.run_validator().returncode, 0)

    def test_modified_or_unlisted_file_fails_integrity(self):
        schema = self.root / "schemas/model.json"
        schema.write_bytes(schema.read_bytes() + b" ")
        self.assertNotEqual(self.run_validator().returncode, 0)
        self.refresh("schemas/model.json")
        (self.root / "unlisted.txt").write_text("fixture")
        self.assertNotEqual(self.run_validator().returncode, 0)

    def test_schema_cannot_add_remote_dependency_even_with_new_local_hash(self):
        path = self.root / "schemas/model.json"
        schema = validator.load(path)
        schema["$defs"]["native_request"] = {"$ref": "https://untrusted.invalid/schema.json"}
        path.write_text(json.dumps(schema), encoding="utf-8")
        self.refresh("schemas/model.json")
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"unsupported_version", result.stderr)

    def test_manifest_cannot_escape_package(self):
        path = self.root / "manifest.json"
        manifest = validator.load(path)
        manifest["sha256"]["../common.json"] = validator.COMMON_SHA256
        path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertNotEqual(self.run_validator().returncode, 0)

    def test_latest_never_uses_legacy_grants_or_untrusted_identity(self):
        contract = validator.Contract(self.root, self.common)
        case = copy.deepcopy(validator.load(self.root / "examples/relations.json")["base"])
        case["auth"]["native_config_versions"] = []
        case["auth"]["config_versions"] = [7]
        with self.assertRaises(validator.Invalid) as raised:
            contract.exchange(case)
        self.assertEqual(raised.exception.code, "forbidden")
        case["auth"]["native_config_versions"] = [7]
        case["native_request"]["user"] = "other-principal"
        case["upstream_request"] = copy.deepcopy(case["native_request"])
        case["context"]["principal_id"] = "other-principal"
        with self.assertRaises(validator.Invalid) as raised:
            contract.exchange(case)
        self.assertEqual(raised.exception.code, "forbidden")

    def test_every_declared_relation_is_checked_for_exact_failure(self):
        contract = validator.Contract(self.root, self.common)
        results = contract.verify_package()
        self.assertGreater(results["documents"], 15)
        self.assertGreater(results["negative_documents"], 8)
        self.assertGreater(results["relations"], 40)

    def test_json_fidelity_distinguishes_booleans_and_precise_decimals(self):
        left, right = self.path / "left.json", self.path / "right.json"
        left.write_text('{"number":0.123456789012345678901,"flag":false}', encoding="utf-8")
        right.write_text('{"number":0.123456789012345678902,"flag":false}', encoding="utf-8")
        self.assertFalse(validator.same_json(validator.load(left), validator.load(right)))
        self.assertFalse(validator.same_json({"flag": False}, {"flag": 0}))
        self.assertFalse(validator.same_json({"flag": True}, {"flag": 1}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--common", type=Path, required=True)
    args = parser.parse_args()
    COMMON = args.common.resolve()
    unittest.main(argv=[sys.argv[0]], verbosity=2)
