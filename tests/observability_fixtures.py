"""Isolation fixtures for the TS-103 observation, health and container tests.

Everything here is synthetic and local: a temporary log directory, a temporary private ledger,
an in-process platform double, an in-process upstream double and a real local HTTP server. No
NAS, no production container, no real account, no paid model call and no network egress.

The TLS material is a long-lived self-signed fixture inlined as PEM text. The dependency lock
is frozen by this task, so no certificate library may be added to the test environment; the
fixture is generated once, is valid until 2126 and is used only against loopback servers
started by these tests.
"""

import copy
import io
import logging
import os
import socket
import ssl
import tempfile
from pathlib import Path
from unittest.mock import patch

import aiohttp
from aiohttp import web

from gateway_fixtures import (
    CONTRACT,
    DOCUMENTS,
    SECRETS,
    WORKSPACE,
    RecordingServices,
    registration,
    start_http,
)
from tianshu_gateway.config import ClientGrant
from tianshu_gateway.observability import Observability, ObservabilitySettings, events, health, sink
from tianshu_gateway.server import GATEWAY, Settings, create_app

__all__ = [
    "CONTRACT",
    "DOCUMENTS",
    "FIXTURE_INSTANCE_ID",
    "ObservedGateway",
    "ObservedTestCase",
    "PROBE_TOKEN",
    "SECRETS",
    "TLS_CERT_PEM",
    "TLS_KEY_PEM",
    "WORKSPACE",
    "check_providers",
    "observation_settings",
    "only",
    "platform_calls_document",
    "platform_headers",
    "read_log",
    "registration",
    "server_ssl_context",
    "start_http",
    "start_tls",
    "write_tls",
]

FIXTURE_INSTANCE_ID = "ts103fixtureinstance0000000000000001"
PROBE_TOKEN = "ts103-diagnostics-token-fixture-0123456789"

TLS_KEY_PEM = b"""-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC13wqKZnwRARNv
2Jyo9L5l6jzR0Vb2L1lxpOG+eYKD0SdwKzHSf3SC9azivJ08HUD46WIAPyOHptCX
88KC2vs3XzZXjJ4JvwD4tHzX7I8rgOkc59y9x2TtYUW3w7wNQbs6ODs4zwbgb2i/
0MApbtWgG5yjz4a6j7UMgxGOA4tbG9biJ5+USSBtK10tpV3QSQHu2yZ9rwrdRpVO
wch62K7H1WzI9JQbGUfC0iNsBdDyQXEqZYnlLAq9ksQFLnn/ZIe8MrVO2VossnXu
OVdXEEUyP4j/UHE47Fv9j1i5hZmcCMftctff5HU2hZyYuYQLHxLgRfLa0BYMfLzn
KffBCBUXAgMBAAECggEAGpzYaC0b7XRjWY6a9JbaP4PZl3g9nzOJhTlzy6abJFpt
c2/r+pnU547h1/cdPkfSPeS6Burg8j9vhDSn8J7w0LGZfDvNf66X3/dafKbBwm4Q
GRSNNqX1N6PFYGD/ZPnf1uKV4+JYvBcBVG7MQk5/avjEtdcEUiCnhP0ENhbVrvnc
2nJLMWO3skGJA6G2t+um4Kr0FtQ/SZ9+yonVWKSrMmMddSKxuSXiirQDa/gZozR5
tXQlcYUlQgQ3Sn35eKQl/SfCiYxqYffW6XjRPJZs1eLx2JiIT2BkH3kl9bFPpZTh
1rRoW3fjByrgMBB47LW23j2F5PKFN1pk1BBEN8kEEQKBgQDhaXBjvSBqtZXcr1s+
qC1lahXIl4v4O8mGc5+0y9mJHJAwKks8lk3AGvhUS5oXS+fkrE0SE3728YSit47X
jjZCJn5vTQXRKCkhAgZEZ4buKeKFp2OyKl85ao4RyaM4uC7k+Sk2gGTQ6ypndSVv
zpm3TX4+Z5ADcl7FREBJdgoYsQKBgQDOjQyGWZxVC+ujp62rzwfCnNhssHd86mzE
e6V9gMBx6+9gbcUl/rQXVHV0fSiO0YssK5OVWR1rhW83P/LchvcPCxoVM1ecOlNE
zvL3B1bwAjK+i4nKav/ZAI3D9CTQKEvRdlkBRyVz+Z5zl+f/0+UzCKr3+KyB8OrK
8PiBvf38RwKBgAWIuiqotQgJp0FAyOOz61FnFlvTZKtWhG8ZnZ0puBCGs/+Kukgl
hIkn4FrpdEIIKgxSMp4z/lT/vvrjuM0P/8MGAOqooHDvJHtb+l1pkUV9n8MaRfdU
1Puq4wwKwEgfNX+HonxlEJSUgXkCxkWFc/6tF2Fe14lOIIeFUnK7RCoxAoGAPwiM
afOu4cVhg/AH8AaeN6Xl5kV0MYrY5p1VQ5enIxz9UFAvegjgrL35mjMXzX3lGvWx
dEJd2BJAfnvlgacufkjFDPM+KU8jWjxNqVV0EoqZMc0jn8JHkdG5cbNwCJZDjQiw
4NL1ew8Sa/RPuKLRr2FVy5b4Di+Xd5dSP0Xb3MECgYEAmJ08GuyQ/XWCYSfBHy6F
JcfGSSBYXlRTwnak6YAil8DTGHxAhcs/ul9D2PzckVwSnIGVCcNeGO+K6D58+8f3
5XN5Mk6fIjIyshj8iYMcVgpSVjJWXBKnyTTtdYLGRA5d3Xm51duN9qZJ0jUt7VyX
MkR65vWvlLgi6/AhpKfT4/w=
-----END PRIVATE KEY-----
"""
TLS_CERT_PEM = b"""-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIUGBSj1Bm0kWV1A0UTLq0y0LOkgL0wDQYJKoZIhvcNAQEL
BQAwIDEeMBwGA1UEAwwVdGlhbnNodS10czEwMy1maXh0dXJlMCAXDTI2MDEwMTAw
MDAwMFoYDzIxMjYwMTAxMDAwMDAwWjAgMR4wHAYDVQQDDBV0aWFuc2h1LXRzMTAz
LWZpeHR1cmUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC13wqKZnwR
ARNv2Jyo9L5l6jzR0Vb2L1lxpOG+eYKD0SdwKzHSf3SC9azivJ08HUD46WIAPyOH
ptCX88KC2vs3XzZXjJ4JvwD4tHzX7I8rgOkc59y9x2TtYUW3w7wNQbs6ODs4zwbg
b2i/0MApbtWgG5yjz4a6j7UMgxGOA4tbG9biJ5+USSBtK10tpV3QSQHu2yZ9rwrd
RpVOwch62K7H1WzI9JQbGUfC0iNsBdDyQXEqZYnlLAq9ksQFLnn/ZIe8MrVO2Vos
snXuOVdXEEUyP4j/UHE47Fv9j1i5hZmcCMftctff5HU2hZyYuYQLHxLgRfLa0BYM
fLznKffBCBUXAgMBAAGjLzAtMBoGA1UdEQQTMBGCCWxvY2FsaG9zdIcEfwAAATAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQCWmBTl8FCX+B6RXIbQ
0/II7c//pgT8WffxGffD8eDoLEvagFkRns5uJI4HVPN/I1yFyueAWr8p13xanz98
5JhEV1E6XmzMpSALPC5UWnAOU+S0+CA1r5/GCdNMyMpXmsBybkepC0YWGf7nLFWp
VXt9o7dMoczpDzzTpA5m7iI4v5or4Isp/koNWcpJGIupy+EYp2D4+3FzQ3S8/22E
BROToWxQcxiNxvjfKFoPxlgiBYPGgY8OZks1UOVWsa5Op+1VL42uqfF9slG5ioLI
YerEaLRQFQbSVHN8URH4+2CwXNTtLd9bt6h/Y7GGCuq4KdkTSpYKRu744haoSgS6
DH1R
-----END CERTIFICATE-----
"""


def write_tls(directory):
    """Materialise the fixture key/certificate; returns ``(cert_path, key_path)``."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    cert, key = directory / "server.crt", directory / "server.key"
    cert.write_bytes(TLS_CERT_PEM)
    key.write_bytes(TLS_KEY_PEM)
    return cert, key


def server_ssl_context(directory):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    cert, key = write_tls(directory)
    context.load_cert_chain(cert, key)
    return context


async def start_tls(app, directory):
    """Start one loopback HTTPS server for the fixture certificate."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.setblocking(False)
    runner = web.AppRunner(app, handler_cancellation=True, access_log=None, shutdown_timeout=0.5)
    await runner.setup()
    await web.SockSite(runner, sock, ssl_context=server_ssl_context(directory)).start()
    return runner, f"https://127.0.0.1:{sock.getsockname()[1]}"


def observation_settings(log_directory, **overrides):
    fields = {
        "log_directory": None if log_directory is None else str(log_directory),
        "probe_token_env": "TS103_DIAGNOSTICS_TOKEN",
        "probe_budget_ms": 1000,
        "observation_validity_seconds": 60,
    }
    fields.update(overrides)
    return ObservabilitySettings(**fields)


def read_log(log_directory):
    """Every stored record, decoded through the frozen closed record."""
    return sink.read_records(log_directory)


def only(log_directory, event_name):
    return [record for record in read_log(log_directory) if record["event"] == event_name]


class ObservedGateway:
    """One fully assembled gateway with the observation adapter wired in.

    The platform and upstream doubles are the existing synthetic fixtures; the only additions
    are the temporary log directory, the independent readiness token and the address the
    in-process server listens on.
    """

    def __init__(self, temp, stderr=None, **observation_overrides):
        self.temp = Path(temp)
        self.stderr = stderr
        self.log_directory = self.temp / "runtime-log"
        self.contract = events.load_contract(str(WORKSPACE / "contracts/diagnostics/v1"))
        self.observability = Observability(
            observation_settings(self.log_directory, **observation_overrides),
            contract=self.contract,
            instance_id=FIXTURE_INSTANCE_ID,
            stderr=stderr,
        )
        self.services = RecordingServices()
        self.sequence = 0
        self.platform_runner = None
        self.upstream_runner = None
        self.runner = None
        self.url = None
        self.settings = None
        self.app = None

    def fill_log_budget(self):
        """Occupy the whole directory budget with a real file, before the sink opens.

        This is the deployment condition the contract calls "capacity": the directory is at its
        configured budget, so the sink must refuse a *new* record, keep every existing byte and
        report itself as unusable. A sparse file makes the condition instant to set up.
        """
        self.log_directory.mkdir(parents=True, exist_ok=True)
        # Deliberately not a segment name: the budget counts every file in the directory, and
        # this fixture must not be mistaken for a stored record by the readers below.
        filler = self.log_directory / "gateway-operator-filler.bin"
        with open(filler, "wb") as handle:
            handle.truncate(self.observability.settings.max_directory_bytes)
        return filler

    async def start(
        self, *, policy=None, native_enabled=False, upstream_wrapper=None, fill_log_budget=False
    ):
        if fill_log_budget:
            self.fill_log_budget()
        upstream_app = web.Application()
        upstream_app.router.add_post(
            "/v1/chat/completions",
            self.services.upstream if upstream_wrapper is None else upstream_wrapper(self.services),
        )
        self.upstream_runner, self.upstream_url = await start_http(upstream_app)
        self.services.configure(self.upstream_url)
        platform_app = web.Application()
        platform_app.router.add_post("/internal/v1/model-config/snapshot", self.services.snapshot)
        self.platform_runner, self.platform_url = await start_http(platform_app)
        references = {"secret-ref:fixture/provider-a": "TS041_TEST_UPSTREAM"}
        for name in ("CLIENT", "OTHER", "EXTERNAL", "PLATFORM"):
            references["secret-ref:fixture/" + name.lower()] = "TS041_TEST_" + name
        contract_directory = str(CONTRACT)
        self.settings = Settings(
            contract_directory,
            str(self.temp / "diagnostics.sqlite"),
            self.platform_url,
            "secret-ref:fixture/platform",
            "TS041_TEST_ORIGIN",
            references,
            [registration(self.platform_url), registration(self.upstream_url + "/v1")],
            [
                ClientGrant(
                    "companion", "secret-ref:fixture/client", "provider-fixture", 7, True, (8,)
                ),
                ClientGrant("external", "secret-ref:fixture/external", "provider-fixture", 7),
                ClientGrant("other", "secret-ref:fixture/other", "provider-fixture", 7),
            ],
            native_enabled=native_enabled,
            scheduling=policy,
            diagnostics_contract_directory=str(WORKSPACE / "contracts/diagnostics/v1"),
            observability=self.observability.settings,
        )
        self.app = create_app(self.settings, self.observability)
        self.runner, self.url = await start_http(self.app)
        self.gateway = self.app[GATEWAY]
        return self

    async def stop(self):
        for runner in (self.runner, self.upstream_runner, self.platform_runner):
            if runner is not None:
                await runner.cleanup()
        self.runner = self.upstream_runner = self.platform_runner = None

    def records(self):
        return read_log(self.log_directory)

    def named(self, event_name):
        return only(self.log_directory, event_name)

    def corpus(self):
        """Every raw byte this process stored, as one string, for leak assertions."""
        return b"".join(path.read_bytes() for path in sink.segment_paths(self.log_directory))

    def headers(self, *, internal=True, request_id=None, turn=None, version=7, correlation=None):
        self.sequence += 1
        headers = {"Authorization": "Bearer " + SECRETS["TS041_TEST_CLIENT"]}
        if internal:
            headers.update(
                {
                    "X-Request-ID": request_id or f"request-{self.sequence}",
                    "X-Tianshu-Turn-ID": turn or f"turn-{self.sequence}",
                    "X-Tianshu-Config-Version": str(version),
                    "X-Tianshu-Workload": "companion.text",
                }
            )
        if correlation is not None:
            headers["X-Tianshu-Correlation-Id"] = correlation
        return headers

    def body(self, **overrides):
        document = copy.deepcopy(DOCUMENTS["native_request"])
        document.update(overrides)
        return document


class ObservedTestCase:
    """Mixin adding the standard isolated environment to one async test case."""

    async def observe(self, *, observation=None, **settings):
        environment = patch.dict(os.environ, {**SECRETS, "TS103_DIAGNOSTICS_TOKEN": PROBE_TOKEN})
        environment.start()
        self.addCleanup(environment.stop)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        # Every side channel this process can write to is captured, so a leak test can scan the
        # stored segments, the emergency channel and the application log in one place.
        self.stderr = io.StringIO()
        self.logged = []
        handler = _CollectingHandler(self.logged)
        logging.getLogger().addHandler(handler)
        self.addCleanup(logging.getLogger().removeHandler, handler)
        self.harness = ObservedGateway(temp.name, stderr=self.stderr, **(observation or {}))
        await self.harness.start(**settings)
        self.addAsyncCleanup(self.harness.stop)
        self.client = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10), trust_env=False
        )
        self.addAsyncCleanup(self.client.close)
        return self.harness

    def all_output(self):
        """Everything the gateway wrote anywhere: stored bytes, emergency channel, app log."""
        return (
            self.harness.corpus()
            + self.stderr.getvalue().encode("utf-8", "replace")
            + "\n".join(self.logged).encode("utf-8", "replace")
        )


class _CollectingHandler(logging.Handler):
    """Captures formatted application log lines so a leak test can scan them too."""

    def __init__(self, sink_list):
        super().__init__()
        self.sink_list = sink_list

    def emit(self, record):
        try:
            self.sink_list.append(record.getMessage())
        except Exception:
            self.sink_list.append("<unformattable>")


def check_providers(**overrides):
    """A ``CheckProviders`` whose eight checks are explicitly supplied per test.

    A value that is already callable is used as the provider itself, so a test can supply a
    provider that sleeps, re-enters the probe or raises; anything else becomes a provider that
    returns that fixed value.
    """
    values = dict.fromkeys(health.CHECK_NAMES, "ok")
    values.update(overrides)
    return health.CheckProviders(
        **{
            name: value if callable(value) else (lambda value=value: value)
            for name, value in values.items()
        }
    )


def platform_calls_document(services):
    """Every snapshot request body the platform double received, decoded."""
    return [body for body, _ in services.config_calls]


def platform_headers(services):
    """Every header set the platform double received, in call order."""
    return [headers for _, headers in services.config_calls]
