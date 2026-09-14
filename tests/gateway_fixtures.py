"""Only tests host the configuration publisher and recording upstream substitute."""

import asyncio
import copy
import json
import socket
import os
from datetime import timedelta
from pathlib import Path

from aiohttp import web

from tianshu_gateway.config import utcnow

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = (
    Path(os.environ["TIANSHU_WORKSPACE"])
    if os.environ.get("TIANSHU_WORKSPACE")
    else Path(json.loads((ROOT / ".runtime/workspace-context.json").read_text())["workspace"])
)
CONTRACT = WORKSPACE / "contracts/text-dialogue/v1"
NATIVE_CONTRACT = WORKSPACE / "contracts/model-protocol/v1"
DOCUMENTS = {
    item["id"]: item["document"]
    for item in json.loads((CONTRACT / "examples/documents.json").read_text(encoding="utf-8"))
}
# Official published model-protocol/v1 fixtures; the gateway must not invent its own shapes.
NATIVE_DOCUMENTS = {
    item["id"]: item["document"]
    for item in json.loads(
        (NATIVE_CONTRACT / "examples/documents.json").read_text(encoding="utf-8")
    )
}

# Deliberately fake, task-specific environment values; no actual account is contacted.
SECRETS = {
    "TS041_TEST_CLIENT": "fixture-client-token-041",
    "TS041_TEST_EXTERNAL": "fixture-external-token-041",
    "TS041_TEST_OTHER": "fixture-other-token-041",
    "TS041_TEST_PLATFORM": "fixture-platform-token-041",
    "TS041_TEST_UPSTREAM": "fixture-upstream-token-041",
    "TS041_TEST_ORIGIN": "origin-fixture",
    "TS042_TEST_NATIVE": "fixture-native-token-042",
    "TS042_TEST_NATIVE_EXTERNAL": "fixture-native-external-token-042",
    "TS042_TEST_NATIVE_OTHER": "fixture-native-other-token-042",
    "TS042_TEST_NATIVE_UPSTREAM": "fixture-native-upstream-token-042",
}
NATIVE_PLATFORM_TOKEN = SECRETS["TS041_TEST_PLATFORM"]


def registration(base):
    return {"base_url": base, "addresses": ["127.0.0.1"], "allow_private_http": True}


async def start_http(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.setblocking(False)
    runner = web.AppRunner(app, handler_cancellation=True, access_log=None, shutdown_timeout=0.5)
    await runner.setup()
    await web.SockSite(runner, sock).start()
    return runner, f"http://127.0.0.1:{sock.getsockname()[1]}"


def event(document):
    return (
        b"data: "
        + json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode()
        + b"\r\n\r\n"
    )


def native_event(kind, **fields):
    return (
        b"event: "
        + kind.encode()
        + b"\r\ndata: "
        + json.dumps({"type": kind, **fields}, ensure_ascii=False, separators=(",", ":")).encode()
        + b"\r\n\r\n"
    )


STREAM = (
    b": fixture keepalive\r\n\r\n"
    + event(
        {
            "id": "chatcmpl-fixture",
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": "你好",
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-fixture",
                                "type": "function",
                                "function": {"name": "fixture_readonly", "arguments": '{"q":'},
                            }
                        ],
                    },
                    "finish_reason": None,
                }
            ],
        }
    )
    + event(
        {
            "choices": [
                {
                    "index": 0,
                    "delta": {"tool_calls": [{"index": 0, "function": {"arguments": '"测试"}'}}]},
                    "finish_reason": "tool_calls",
                }
            ],
        }
    )
    + event(
        {
            "choices": [],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 9,
                "unknown_vendor_detail": {"cached": None},
            },
        }
    )
    + b"data: [DONE]\r\n\r\n"
)

NATIVE_RESPONSE = copy.deepcopy(NATIVE_DOCUMENTS["native_response"])
NATIVE_RESPONSE["usage"] = {
    "input_tokens": 0,
    "output_tokens": 8,
    "output_tokens_details": {"reasoning_tokens": 3},
    "vendor": {"x": None},
}

NATIVE_STREAM = (
    b": fixture keepalive\r\n\r\n"
    + native_event(
        "response.created",
        response={**NATIVE_RESPONSE, "status": "in_progress", "usage": None},
    )
    + native_event("response.output_text.delta", delta="你好", sequence_number=1)
    + native_event("response.function_call_arguments.delta", delta='{"q":', sequence_number=2)
    + native_event("response.vendor_future", unknown={"unchanged": True})
    + native_event("response.completed", response=NATIVE_RESPONSE)
)


class RecordingServices:
    def __init__(self):
        self.calls = []
        self.config_calls = []
        self.source_status = 200
        self.source_wrong_request = False
        self.mode = "json"
        self.http_status = 429
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.disconnected = asyncio.Event()
        self.source_release = None
        self.stream = STREAM
        self.response = copy.deepcopy(DOCUMENTS["native_response"])
        self.response["usage"] = {"prompt_tokens": 0, "vendor_unknown": {"partial": True}}
        # Native (model-protocol/v1) fixtures: separate records, modes and version space.
        self.native_calls = []
        self.native_config_calls = []
        self.native_snapshot_documents = []
        self.native_source_status = 200
        self.native_source_wrong_request = False
        self.native_source_release = None
        self.native_versions = {7}
        self.native_revoked_versions = set()
        self.native_mode = "json"
        self.native_http_status = 429
        self.native_started = asyncio.Event()
        self.native_release = asyncio.Event()
        self.native_disconnected = asyncio.Event()
        self.native_stream = NATIVE_STREAM
        self.native_response = copy.deepcopy(NATIVE_RESPONSE)
        self.native_response_bytes = b""
        self.cookie_value = "upstream-cookie-fixture"

    def configure(self, upstream):
        self.config = copy.deepcopy(DOCUMENTS["config"])
        self.config["published_at"] = (
            (utcnow() - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        )
        self.config["usable_until"] = (
            (utcnow() + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        )
        self.config["providers"][0]["base_url"] = upstream + "/v1"
        self.config["bindings"][0]["timeout_ms"] = 1500
        self.native_config = copy.deepcopy(NATIVE_DOCUMENTS["config_response"])
        self.native_config["published_at"] = (
            (utcnow() - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        )
        self.native_config["usable_until"] = (
            (utcnow() + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        )
        self.native_config["providers"][0]["base_url"] = upstream + "/v1"
        self.native_config["bindings"][0]["timeout_ms"] = 1500

    async def snapshot(self, request):
        body = await request.json()
        self.config_calls.append((body, dict(request.headers)))
        if request.headers.get("Authorization") != "Bearer " + SECRETS["TS041_TEST_PLATFORM"]:
            return web.Response(status=401)
        if self.source_release is not None:
            await self.source_release.wait()
        if self.source_status != 200:
            return web.Response(status=self.source_status)
        document = copy.deepcopy(self.config)
        document["request_id"] = (
            "wrong-id" if self.source_wrong_request else body["query"]["request_id"]
        )
        return web.json_response(document)

    async def native_snapshot(self, request):
        body = await request.json()
        self.native_config_calls.append((body, dict(request.headers)))
        if request.headers.get("Authorization") != "Bearer " + NATIVE_PLATFORM_TOKEN:
            return web.Response(status=401)
        if self.native_source_release is not None:
            await self.native_source_release.wait()
        if self.native_source_status != 200:
            return web.Response(status=self.native_source_status)
        requested = body["native_config_version"]
        if requested is None:
            # The platform owns authorized-latest selection; the gateway must still verify it.
            version = max(self.native_versions)
        else:
            if requested not in self.native_versions:
                return web.Response(status=404)
            version = requested
        if version in self.native_revoked_versions:
            return web.Response(status=403)
        document = copy.deepcopy(self.native_config)
        document["native_config_version"] = version
        document["request_id"] = (
            "wrong-id" if self.native_source_wrong_request else body["query"]["request_id"]
        )
        self.native_snapshot_documents.append(copy.deepcopy(document))
        return web.json_response(document)

    async def upstream(self, request):
        raw = await request.read()
        self.calls.append((raw, dict(request.headers)))
        self.started.set()
        try:
            if self.mode == "hold":
                await self.release.wait()
            if self.mode == "error":
                return web.Response(
                    status=self.http_status,
                    text="reflected " + SECRETS["TS041_TEST_UPSTREAM"],
                    headers={
                        "X-Request-ID": SECRETS["TS041_TEST_UPSTREAM"],
                        "Location": request.scheme + "://127.0.0.1:1/stolen",
                        "Set-Cookie": "secret=" + SECRETS["TS041_TEST_UPSTREAM"],
                    },
                )
            if self.mode in {"stream", "drop", "sse_hold", "secret_stream", "one_byte"}:
                response = web.StreamResponse(
                    headers={
                        "Content-Type": "text/event-stream",
                        "X-Request-ID": "upstream-fixture",
                    }
                )
                await response.prepare(request)
                data = self.stream
                if self.mode == "sse_hold":
                    await response.write(data[:300])
                    await self.release.wait()
                    await response.write(data[300:])
                elif self.mode == "secret_stream":
                    data = b"data: " + SECRETS["TS041_TEST_UPSTREAM"].encode() + b"\n\n"
                    for byte in data:
                        await response.write(bytes([byte]))
                        await asyncio.sleep(0)
                elif self.mode == "one_byte":
                    for byte in data:
                        await response.write(bytes([byte]))
                        await asyncio.sleep(0)
                else:
                    await response.write(data)
                if self.mode == "drop":
                    request.transport.abort()
                else:
                    await response.write_eof()
                return response
            if self.mode == "secret_json":
                return web.json_response({**self.response, "error": SECRETS["TS041_TEST_UPSTREAM"]})
            if self.mode == "invalid_json":
                return web.Response(body=b'{"id":', content_type="application/json")
            if self.mode == "encoded":
                return web.Response(
                    body=b"compressed",
                    headers={"Content-Type": "application/json", "Content-Encoding": "gzip"},
                )
            return web.json_response(self.response, headers={"X-Request-ID": "upstream-fixture"})
        except (asyncio.CancelledError, ConnectionResetError):
            self.disconnected.set()
            raise

    async def native_upstream(self, request):
        raw = await request.read()
        self.native_calls.append((raw, dict(request.headers)))
        self.native_started.set()
        try:
            if self.native_mode == "hold":
                await self.native_release.wait()
            if self.native_mode == "error":
                return web.Response(
                    status=self.native_http_status,
                    text="reflected " + SECRETS["TS042_TEST_NATIVE_UPSTREAM"],
                    headers={
                        "X-Request-ID": SECRETS["TS042_TEST_NATIVE_UPSTREAM"],
                        "Location": request.scheme + "://127.0.0.1:1/stolen",
                        "Set-Cookie": "secret=" + SECRETS["TS042_TEST_NATIVE_UPSTREAM"],
                    },
                )
            if self.native_mode == "error_json":
                return web.json_response(
                    {"error": {"code": "rate_limit_exceeded", "message": "fixture"}},
                    status=self.native_http_status,
                    headers={
                        "Location": request.scheme + "://127.0.0.1:1/stolen",
                        "Set-Cookie": "secret=" + SECRETS["TS042_TEST_NATIVE_UPSTREAM"],
                    },
                )
            if self.native_mode in {"stream", "drop", "sse_hold", "one_byte", "secret_stream"}:
                response = web.StreamResponse(
                    headers={
                        "Content-Type": "text/event-stream",
                        "X-Request-ID": "native-upstream-fixture",
                    }
                )
                await response.prepare(request)
                data = self.native_stream
                if self.native_mode == "sse_hold":
                    await response.write(data[:350])
                    await self.native_release.wait()
                    await response.write(data[350:])
                elif self.native_mode == "one_byte":
                    for byte in data:
                        await response.write(bytes([byte]))
                        await asyncio.sleep(0)
                elif self.native_mode == "secret_stream":
                    data = native_event(
                        "response.output_text.delta", delta=SECRETS["TS042_TEST_NATIVE_UPSTREAM"]
                    ) + native_event("response.completed", response=self.native_response)
                    for byte in data:
                        await response.write(bytes([byte]))
                        await asyncio.sleep(0)
                else:
                    await response.write(data)
                if self.native_mode == "drop":
                    request.transport.abort()
                else:
                    await response.write_eof()
                return response
            if self.native_mode == "secret_json":
                return web.json_response(
                    {**self.native_response, "error": SECRETS["TS042_TEST_NATIVE_UPSTREAM"]}
                )
            if self.native_mode == "invalid_json":
                return web.Response(body=b'{"id":', content_type="application/json")
            if self.native_mode == "encoded":
                return web.Response(
                    body=b"compressed",
                    headers={"Content-Type": "application/json", "Content-Encoding": "gzip"},
                )
            if self.native_mode == "wrong_shape":
                return web.json_response(
                    {"object": "response", "id": "resp-fixture", "status": "unmodelled"}
                )
            self.native_response_bytes = json.dumps(self.native_response).encode()
            return web.Response(
                body=self.native_response_bytes,
                content_type="application/json",
                headers={"X-Request-ID": "native-upstream-fixture"},
            )
        except (asyncio.CancelledError, ConnectionResetError):
            self.native_disconnected.set()
            raise
