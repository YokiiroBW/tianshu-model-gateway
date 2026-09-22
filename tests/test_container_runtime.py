"""TS-103: the container base, its liveness check and the container example document.

The container image has never been built or run in this environment, because no container
runtime is available. Nothing in this module claims otherwise: it verifies the *text* of the
container definition, the behaviour of the liveness check against a real local TLS server, and
the shape of the example deployment document. `docs/deployment.md` records exactly which items
remain unverified for that reason.
"""

import asyncio
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from aiohttp import web

from observability_fixtures import WORKSPACE, start_tls, write_tls
from tianshu_gateway.server import load_settings

ROOT = Path(__file__).resolve().parent.parent
DOCKERFILE = ROOT / "Dockerfile"
DOCKERIGNORE = ROOT / ".dockerignore"
HEALTHCHECK = ROOT / "scripts" / "healthcheck.py"
EXAMPLE = ROOT / "config" / "container.example.json"
DIAGNOSTICS_PACKAGE = WORKSPACE / "contracts" / "diagnostics" / "v1"


def dockerfile_lines():
    """The Dockerfile's logical instructions: comments dropped, backslash continuations joined.

    Reading physical lines would mis-count a multi-line ``HEALTHCHECK`` as a second ``CMD`` and
    would miss the flags that live on its continuation line.
    """
    logical, buffer = [], ""
    for raw in DOCKERFILE.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.endswith("\\"):
            buffer += stripped[:-1].rstrip() + " "
            continue
        buffer += stripped
        logical.append(buffer)
        buffer = ""
    if buffer:
        logical.append(buffer)
    return logical


class ContainerDefinitionTests(unittest.TestCase):
    """Static properties of the image: pinned, unprivileged, credential-free, probe-safe."""

    def setUp(self):
        self.lines = dockerfile_lines()
        self.text = DOCKERFILE.read_text(encoding="utf-8")

    def instructions(self, keyword):
        return [line for line in self.lines if line.upper().startswith(keyword.upper() + " ")]

    def test_the_image_uses_a_pinned_python_base_and_no_floating_tag(self):
        bases = self.instructions("FROM")
        self.assertEqual(len(bases), 2, bases)
        for line in bases:
            self.assertRegex(
                line,
                r"^FROM --platform=linux/amd64 python:3\.12\.\d+-slim-bookworm@sha256:[a-f0-9]{64}( AS \w+)?$",
            )
            self.assertNotIn("latest", line)

    def test_the_locked_dependency_set_is_installed_with_locked_resolution(self):
        self.assertIn("--locked", self.text)
        self.assertIn("uv.lock", self.text)
        self.assertIn("--no-dev", self.text)

    def test_the_image_never_contains_a_credential_or_a_default_endpoint(self):
        for line in self.lines:
            upper = line.upper()
            if upper.startswith(("ENV ", "ARG ", "LABEL ")):
                for forbidden in ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "CREDENTIAL"):
                    self.assertNotIn(forbidden, upper, line)
        # The only environment variables are interpreter hygiene and the venv path.
        names = set()
        for line in self.instructions("ENV"):
            for token in line[4:].split():
                if "=" in token:
                    names.add(token.split("=", 1)[0])
        self.assertEqual(
            names,
            {"PYTHONDONTWRITEBYTECODE", "PYTHONUNBUFFERED", "PATH", "UV_PROJECT_ENVIRONMENT"}
            | {"UV_LINK_MODE", "UV_PYTHON_DOWNLOADS", "UV_NO_CACHE"},
        )

    def test_the_process_runs_as_an_unprivileged_user(self):
        users = self.instructions("USER")
        self.assertEqual(len(users), 1, users)
        self.assertNotIn("root", users[0].lower())
        self.assertIn("useradd", self.text)
        self.assertIn("--uid 10001", self.text)

    def test_the_healthcheck_is_liveness_only_and_over_tls(self):
        healthchecks = self.instructions("HEALTHCHECK")
        self.assertEqual(len(healthchecks), 1, healthchecks)
        command = healthchecks[0]
        self.assertIn("/health/live", command)
        self.assertNotIn("/health/ready", command)
        self.assertIn("https://", command)
        self.assertIn("--cacert", command)
        self.assertIn("healthcheck.py", command)

    def test_the_entry_point_needs_explicit_deployment_input_and_real_tls(self):
        entrypoints = self.instructions("ENTRYPOINT")
        commands = self.instructions("CMD")
        self.assertEqual(len(entrypoints), 1, entrypoints)
        self.assertEqual(len(commands), 1, commands)
        self.assertIn("tianshu_gateway", entrypoints[0])
        self.assertIn("--settings", commands[0])
        self.assertIn("--tls-cert", commands[0])
        self.assertIn("--tls-key", commands[0])
        # The loopback-only HTTP fixture mode must never be the container default.
        self.assertNotIn("--local-test", self.text)

    def test_a_graceful_stop_signal_is_declared(self):
        stopsignals = self.instructions("STOPSIGNAL")
        self.assertEqual(stopsignals, ["STOPSIGNAL SIGTERM"])

    def test_the_writable_paths_are_declared_as_mount_points_not_baked_in(self):
        self.assertIn("/var/log/tianshu", self.text)
        self.assertIn("/var/lib/tianshu", self.text)
        self.assertIn("/etc/tianshu/settings.json", self.text)
        self.assertNotIn("VOLUME", self.text)

    def test_the_build_context_excludes_local_state_and_credentials(self):
        ignored = {
            line.strip()
            for line in DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
        for required in (
            ".git",
            ".venv",
            ".runtime",
            ".env",
            ".env.*",
            "tests",
            "*.sqlite",
            "*.sqlite-wal",
            "*.sqlite-shm",
        ):
            self.assertIn(required, ignored, required)

    def test_the_liveness_check_uses_only_the_standard_library(self):
        source = HEALTHCHECK.read_text(encoding="utf-8")
        for forbidden in ("import aiohttp", "import jsonschema", "import referencing"):
            self.assertNotIn(forbidden, source, forbidden)
        self.assertIn("import ssl", source)
        self.assertIn("create_default_context", source)


class HealthcheckBehaviourTests(unittest.IsolatedAsyncioTestCase):
    """The check itself, run as a real subprocess against a real local TLS server."""

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.seen = []
        self.status = 200
        self.body = b'{"status":"alive"}'

        async def live(request):
            self.seen.append(dict(request.headers))
            return web.Response(body=self.body, status=self.status, content_type="application/json")

        app = web.Application()
        app.router.add_get("/health/live", live)
        self.runner, self.url = await start_tls(app, self.temp.name)
        self.addAsyncCleanup(self.runner.cleanup)
        self.cert, _key = write_tls(self.temp.name)

    async def run_check(self, *extra):
        command = [
            sys.executable,
            "-B",
            str(HEALTHCHECK),
            "--url",
            self.url + "/health/live",
            "--cacert",
            str(self.cert),
            "--timeout",
            "5",
            *extra,
        ]
        result = await asyncio.to_thread(
            subprocess.run, command, capture_output=True, timeout=60, cwd=str(ROOT)
        )
        report = {}
        if result.stdout.strip():
            report = json.loads(result.stdout.decode("utf-8").strip().splitlines()[-1])
        return result.returncode, report

    async def test_a_live_process_reports_alive_and_exits_zero(self):
        code, report = await self.run_check()
        self.assertEqual(code, 0, report)
        self.assertEqual(report, {"check": "liveness", "result": "alive"})

    async def test_the_check_sends_no_credential_and_no_business_header(self):
        await self.run_check()
        self.assertTrue(self.seen)
        headers = {name.lower() for name in self.seen[0]}
        self.assertNotIn("authorization", headers)
        self.assertNotIn("x-tianshu-correlation-id", headers)
        self.assertNotIn("cookie", headers)
        self.assertEqual(headers & {"accept"}, {"accept"})

    async def test_a_non_200_answer_fails_the_check(self):
        self.status = 503
        self.body = b'{"status":"not_ready","service":"gateway","checks":{}}'
        code, report = await self.run_check()
        self.assertEqual(code, 1)
        self.assertEqual(report["result"], "unexpected_status")

    async def test_a_wrong_body_fails_the_check(self):
        self.body = b'{"status":"ready"}'
        code, report = await self.run_check()
        self.assertEqual(code, 1)
        self.assertEqual(report["result"], "unexpected_body")

    async def test_a_non_json_body_fails_the_check(self):
        self.body = b"OK"
        code, report = await self.run_check()
        self.assertEqual(code, 1)
        self.assertEqual(report["result"], "invalid_body")

    async def test_an_unreachable_server_fails_the_check(self):
        runner, url = await start_tls(web.Application(), self.temp.name)
        await runner.cleanup()
        command = [
            sys.executable,
            "-B",
            str(HEALTHCHECK),
            "--url",
            url + "/health/live",
            "--cacert",
            str(self.cert),
            "--timeout",
            "2",
        ]
        result = await asyncio.to_thread(
            subprocess.run, command, capture_output=True, timeout=60, cwd=str(ROOT)
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout.decode("utf-8").strip())["result"], "unreachable")

    async def test_an_unverifiable_certificate_fails_the_check(self):
        # No --cacert: the fixture certificate is not in any system trust store, so the check
        # must fail rather than silently accepting an unverified peer.
        command = [
            sys.executable,
            "-B",
            str(HEALTHCHECK),
            "--url",
            self.url + "/health/live",
            "--timeout",
            "5",
        ]
        result = await asyncio.to_thread(
            subprocess.run, command, capture_output=True, timeout=60, cwd=str(ROOT)
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout.decode("utf-8").strip())["result"], "unreachable")

    async def test_a_plaintext_url_is_refused_unless_explicitly_allowed(self):
        command = [
            sys.executable,
            "-B",
            str(HEALTHCHECK),
            "--url",
            "http://127.0.0.1:1/health/live",
            "--timeout",
            "2",
        ]
        result = await asyncio.to_thread(
            subprocess.run, command, capture_output=True, timeout=60, cwd=str(ROOT)
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(
            json.loads(result.stdout.decode("utf-8").strip())["result"], "plaintext_refused"
        )

    async def test_an_invalid_url_fails_the_check(self):
        command = [
            sys.executable,
            "-B",
            str(HEALTHCHECK),
            "--url",
            "not-a-url",
            "--timeout",
            "2",
        ]
        result = await asyncio.to_thread(
            subprocess.run, command, capture_output=True, timeout=60, cwd=str(ROOT)
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout.decode("utf-8").strip())["result"], "invalid_url")


class ContainerExampleTests(unittest.TestCase):
    """The example deployment document is a real, loadable document, not prose in JSON."""

    def setUp(self):
        self.raw = EXAMPLE.read_bytes()
        self.document = json.loads(self.raw)

    def test_the_example_is_strict_json_without_comment_keys(self):
        for key in self.document:
            self.assertFalse(key.startswith("_"), key)

    def test_the_example_loads_through_the_real_settings_loader(self):
        settings = load_settings(self.raw)
        self.assertEqual(settings.clients[0].service, "companion")
        self.assertIsNotNone(settings.observability)
        self.assertEqual(
            settings.diagnostics_contract_directory, "/etc/tianshu/contracts/diagnostics/v1"
        )
        self.assertFalse(settings.native_enabled)
        self.assertEqual(settings.observability.log_directory, "/var/log/tianshu")

    def test_the_example_is_valid_on_the_platform_it_targets(self):
        settings = load_settings(self.raw)
        if sys.platform == "win32":
            # The absolute-path rule is a property of the host filesystem, so it is asserted
            # where the paths are real. On Windows the loader above is the whole check.
            self.skipTest("absolute-path validation is host filesystem dependent")
        settings.validate()

    def test_the_example_carries_no_literal_credential(self):
        text = self.raw.decode("utf-8")
        for forbidden in ("Bearer ", "token-", "sk-", "password", "BEGIN PRIVATE KEY"):
            self.assertNotIn(forbidden, text, forbidden)
        for name, reference in self.document["secret_references"].items():
            self.assertRegex(reference, r"^[A-Z][A-Z0-9_]+$", name)

    def test_the_example_points_at_the_published_diagnostics_package(self):
        self.assertTrue((DIAGNOSTICS_PACKAGE / "manifest.json").is_file())
        self.assertEqual(
            Path(self.document["diagnostics_contract_directory"]).name, DIAGNOSTICS_PACKAGE.name
        )

    def test_the_example_requests_a_durable_log_directory(self):
        block = self.document["observability"]
        self.assertEqual(
            set(block),
            {
                "log_directory",
                "max_directory_bytes",
                "probe_token_env",
                "probe_budget_ms",
                "observation_validity_seconds",
            },
        )
        self.assertGreaterEqual(block["max_directory_bytes"], 32 * 1024 * 1024)
        self.assertLessEqual(block["max_directory_bytes"], 64 * 1024 * 1024 * 1024)
        self.assertGreaterEqual(block["probe_budget_ms"], 1)
        self.assertGreater(block["observation_validity_seconds"], 0)


if __name__ == "__main__":
    unittest.main()
