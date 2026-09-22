"""Build input and explicit-scope boundaries, not Linux container execution."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/build/check_install.py"


class BuildInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        (ROOT / ".runtime").mkdir(exist_ok=True)

    def test_default_plan_does_not_create_environment(self):
        target = ROOT / ".runtime" / ("plan-test-" + uuid.uuid4().hex)
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--scope", str(target)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(json.loads(result.stdout)["linux_image_executed"])
        self.assertFalse(target.exists())

    def test_existing_scope_is_never_reused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / ".runtime") as directory:
            sentinel = Path(directory) / "preserve"
            sentinel.write_bytes(b"original")
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--scope", directory, "--execute"],
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 2)
            self.assertEqual(sentinel.read_bytes(), b"original")
            self.assertEqual(len(list(Path(directory).iterdir())), 1)

    def test_outside_scope_is_rejected_before_install(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "new"
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--scope", str(target), "--execute"],
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 2)
            self.assertFalse(target.exists())

    def test_froms_are_evidenced_linux_amd64_manifests(self):
        evidence = json.loads((ROOT / "scripts/build/base-images.json").read_text())
        record = evidence["images"][0]
        self.assertEqual(record["platform"], "linux/amd64")
        ref = "python:" + record["tag"] + "@" + record["manifest_digest"]
        bases = [
            x.split()
            for x in (ROOT / "Dockerfile").read_text().splitlines()
            if x.startswith("FROM ")
        ]
        self.assertEqual(len(bases), 2)
        for parts in bases:
            self.assertEqual(parts[1:3], ["--platform=linux/amd64", ref])


if __name__ == "__main__":
    unittest.main()
