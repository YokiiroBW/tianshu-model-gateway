"""Native Responses transport used by the registered gateway route.

The caller owns trusted identity, configuration and ledger start; this module owns the
single upstream HTTP attempt, bounded observation and the ledger finish. It never reads
Chat configuration, never takes identity from the payload and never replays a request.
"""

import asyncio
import time

import aiohttp

from .config import read_limited, utcnow
from .contracts import NATIVE_CONTRACT, Rejected, loads
from .diagnostics import attempt_metrics, native_identity, redact
from .routing import SecretGuard, StreamObserver

PROTOCOL = "openai-responses"
TERMINALS = {"completed": "succeeded", "failed": "failed", "incomplete": "unknown"}
# Recognized native lifecycle events that carry model output for the latency projection.
# This is a local diagnostic set, not a protocol addition: an unknown event still counts
# as the first observed event and simply leaves the first-output latency unobserved.
OUTPUT_EVENT_KINDS = frozenset(
    {
        "response.output_item.added",
        "response.output_item.done",
        "response.content_part.added",
        "response.content_part.done",
        "response.output_text.delta",
        "response.output_text.done",
        "response.refusal.delta",
        "response.function_call_arguments.delta",
        "response.function_call_arguments.done",
        "response.reasoning_summary_part.added",
        "response.reasoning_summary_text.delta",
        "response.reasoning_text.delta",
    }
)


def validate_request(raw):
    """Bound the supported operation without rebuilding native JSON or tool schemas."""
    body = loads(raw)
    if not isinstance(body, dict):
        raise Rejected()
    model = body.get("model")
    if (
        not isinstance(model, str)
        or not model.strip()
        or any(ord(c) < 32 or ord(c) == 127 for c in model)
    ):
        raise Rejected()
    if not isinstance(body.get("input"), (str, list)):
        raise Rejected()
    for key in ("stream", "background", "store"):
        if key in body and type(body[key]) is not bool:
            raise Rejected()
    if body.get("background"):
        raise Rejected("unsupported_operation", 501)
    for key in (
        "previous_response_id",
        "conversation",
        "conversation_id",
        "response_id",
        "prompt",
        "file_id",
        "container_id",
    ):
        if body.get(key) is not None:
            raise Rejected("state_reference_unsupported", 409)
    cache = body.get("prompt_cache_options")
    if isinstance(cache, dict) and cache.get("comparison_response_id") is not None:
        raise Rejected("state_reference_unsupported", 409)
    tools = body.get("tools", [])
    if not isinstance(tools, list):
        raise Rejected()
    for tool in tools:
        if not isinstance(tool, dict) or tool.get("type") not in {"function", "custom"}:
            raise Rejected("unsupported_operation", 501)

    # Inspect native item positions, not arbitrary user data or JSON Schema properties.
    def items(values):
        if not isinstance(values, list):
            return
        for item in values:
            if not isinstance(item, dict):
                raise Rejected()
            if item.get("type") == "item_reference" or (
                "id" in item and set(item) <= {"id", "type"}
            ):
                raise Rejected("state_reference_unsupported", 409)
            if any(item.get(k) is not None for k in ("file_id", "container_id", "response_id")):
                raise Rejected("state_reference_unsupported", 409)
            if item.get("type") in {"input_file", "input_image", "input_audio", "computer_call"}:
                raise Rejected("unsupported_operation", 501)
            items(item.get("content"))
            if item.get("type") in {"function_call_output", "custom_tool_call_output"}:
                items(item.get("output"))

    items(body["input"])
    items(body.get("instructions"))
    return body


def usage_projection(native):
    if not isinstance(native, dict):
        return None
    return {
        key: native[key]
        for key in ("input_tokens", "output_tokens")
        if type(native.get(key)) is int and native[key] >= 0
    } or None


class ResponsesObserver(StreamObserver):
    """Reuse the bounded SSE line scanner, observing native typed lifecycle events."""

    def __init__(self, limit=262144, started=None):
        super().__init__(limit=limit, started=started)
        self.status = None
        self.response_id = None
        self.terminal_usage = False

    def observe_response(self, value, terminal=None):
        if not isinstance(value, dict) or value.get("object") != "response":
            raise Rejected()
        response_id = value.get("id")
        if not isinstance(response_id, str) or not response_id:
            raise Rejected()
        if self.response_id is not None and self.response_id != response_id:
            raise Rejected()
        self.response_id = response_id
        if isinstance(value.get("usage"), dict):
            self.native_usage = value["usage"]
        status = value.get("status")
        if terminal is not None:
            if status != terminal or terminal not in TERMINALS:
                raise Rejected()
            if terminal == "completed" and value.get("error") is not None:
                raise Rejected()
            self.status = terminal
            self.done = True
            self.terminal_usage = isinstance(value.get("usage"), dict)

    def dispatch(self):
        if not self.data:
            return
        if self.first_event_ms is None:
            self.first_event_ms = self.elapsed_ms()
        try:
            if self.done:
                raise Rejected()
            value = loads(b"\n".join(self.data))
            if not isinstance(value, dict) or not isinstance(value.get("type"), str):
                raise Rejected()
            kind = value["type"]
            if self.event_type and self.event_type != kind.encode():
                raise Rejected()
            if kind == "error":
                self.done = self.error = True
                self.status = "failed"
            elif kind.startswith("response.") and kind[9:] in TERMINALS:
                self.observe_response(value.get("response"), kind[9:])
            elif "response" in value:
                self.observe_response(value["response"])
            if self.first_output_ms is None and kind in OUTPUT_EVENT_KINDS:
                self.first_output_ms = self.elapsed_ms()
        except (Rejected, UnicodeError):
            self.invalid = True

    @property
    def complete(self):
        return self.done and not self.invalid and not self.buffer.strip() and not self.data


def record_observation(receipt, observer, complete):
    receipt["response_id"] = observer.response_id
    receipt["native_usage"] = observer.native_usage
    receipt["usage"] = usage_projection(observer.native_usage)
    receipt["usage_complete"] = bool(
        complete and observer.terminal_usage and receipt["usage"] and len(receipt["usage"]) == 2
    )


async def send_responses(
    session,
    targets,
    ledger,
    *,
    payload,
    base_url,
    credential,
    receipt,
    start_response,
    write,
    timeout=180.0,
    max_response_bytes=8_388_608,
    max_request_bytes=1_048_576,
    secrets=(),
    validate_response=None,
    validate_receipt=None,
    on_first_output=None,
):
    """One HTTP attempt with asynchronous sink/backpressure and native-ledger observation.

    start_response(status, content_type) and write(bytes) are awaited. The caller must
    already have authenticated the request, selected the native configuration and recorded
    the receipt in the native ledger (``native_begin``); this function always finishes that
    row. ``ledger`` is the independent native key space: it exposes
    ``native_is_revoked(identity, version)`` and
    ``native_finish(receipt, reason, elapsed_ms, upstream_status)``.

    The caller must abort its downstream on exceptions; this function never adds SSE
    markers, retries, redirects, publishes config, or claims a local receipt is a wire
    contract.

    Wire-level failures (unreadable, compressed, oversized or unparseable bodies, wrong
    content type, timeouts, cancellation, connection loss, reflected credentials) fail the
    attempt. An observation shortfall is different: once the upstream response has been
    received in full, the observer may only downgrade the recorded outcome to unknown, so
    the raw bytes are still delivered unchanged.

    ``on_first_output`` is a fixed, optional observation seam: it is called at most once, at
    the instant this attempt first produced model output. It receives no chunk, no count and no
    content, and it cannot influence a byte of the transfer -- a failure inside it is confined
    to the side channel.
    """
    if timeout <= 0 or min(max_request_bytes, max_response_bytes) <= 0:
        raise ValueError("positive transport limits required")
    if len(payload) > max_request_bytes:
        raise Rejected("payload_too_large", 413)
    body = validate_request(payload)
    first_output = [on_first_output is not None]

    def note_output():
        """Report the first observed output once; never participates in the transfer."""
        if not first_output[0]:
            return
        first_output[0] = False
        try:
            on_first_output()
        except Exception:
            return

    if (
        receipt.get("protocol") != PROTOCOL
        or receipt.get("contract") != NATIVE_CONTRACT
        or type(receipt.get("native_config_version")) is not int
        or receipt.get("resolved_model") != body["model"]
    ):
        # A Chat-shaped or incomplete receipt must never reach the native transport.
        raise Rejected("invalid_input", 400)
    targets.check(base_url)
    identity = native_identity(receipt)
    if ledger.native_is_revoked(identity, receipt["native_config_version"]):
        raise Rejected("forbidden", 403)
    secrets = (*secrets, credential)
    receipt.update(outcome="unknown", usage=None, native_usage=None, usage_complete=False)
    receipt = redact(receipt, secrets)
    upstream_status = None
    reason = "transport_unknown"
    started = time.monotonic()
    observer = ResponsesObserver(started=started)
    complete = False
    # True once a complete upstream response has been read and examined, which is what
    # separates "the provider reported no usage" from "no usage could be observed".
    inspected = False
    try:
        async with asyncio.timeout(timeout):
            async with session.post(
                base_url + "/responses",
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + credential,
                    "Accept": "text/event-stream" if body.get("stream") else "application/json",
                    "Accept-Encoding": "identity",
                },
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as upstream:
                upstream_status = upstream.status
                request_id = upstream.headers.get("x-request-id")
                if request_id and len(request_id) <= 256:
                    receipt["upstream_request_id"] = redact(request_id, secrets)
                if upstream.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise Rejected("result_unknown", 502, "unknown")
                guard = SecretGuard(secrets)
                if not 200 <= upstream.status < 300:
                    raw = await read_limited(upstream.content, max_response_bytes)
                    guard.feed(raw, final=True)
                    # Native JSON errors are preserved; no upstream cookies/location/credentials.
                    if upstream.content_type != "application/json":
                        raise Rejected("result_unknown", 502, "unknown")
                    loads(raw)
                    await start_response(upstream.status, "application/json")
                    await write(raw)
                    receipt["outcome"] = "failed" if 400 <= upstream.status < 500 else "unknown"
                    reason = "upstream_http_error"
                    return
                if body.get("stream"):
                    if upstream.content_type != "text/event-stream":
                        raise Rejected("result_unknown", 502, "unknown")
                    await start_response(upstream.status, "text/event-stream")
                    async for chunk in upstream.content.iter_any():
                        observer.feed(chunk)
                        if observer.first_output_ms is not None:
                            note_output()
                        safe = guard.feed(chunk)
                        if safe:
                            await write(safe)
                    observer.end()
                    inspected = True
                    # The upstream stream ended by itself, so every byte belongs downstream.
                    # Observation is a side channel: an observation shortfall (per-event
                    # budget, unparseable or unknown event, missing terminal) may only mark
                    # the result unknown. It never truncates a delivered stream and never
                    # fabricates a terminal event.
                    tail = guard.feed(b"", final=True)
                    if tail:
                        await write(tail)
                else:
                    if upstream.content_type != "application/json":
                        raise Rejected("result_unknown", 502, "unknown")
                    raw = await read_limited(upstream.content, max_response_bytes)
                    guard.feed(raw, final=True)
                    native = loads(raw)
                    if isinstance(native, dict):
                        inspected = True
                    try:
                        if validate_response is not None:
                            validate_response(native)
                        observer.observe_response(
                            native, native.get("status") if isinstance(native, dict) else None
                        )
                    except Rejected:
                        # Received in full but not confirmable as a published native
                        # response: the bytes stay the upstream's and the result is unknown.
                        pass
                    await start_response(upstream.status, "application/json")
                    await write(raw)
                    # A single JSON body is its own first output.
                    note_output()
                if observer.complete:
                    complete = True
                    receipt["outcome"] = TERMINALS[observer.status]
                    reason = "response_" + observer.status
                else:
                    # Neither success nor a transport failure: the wire attempt completed
                    # but the result was not observed, so it stays unknown.
                    reason = "observation_incomplete"
    except asyncio.CancelledError:
        reason = "cancelled_unknown"
        raise
    except TimeoutError:
        reason = "timeout_unknown"
        raise Rejected("result_unknown", 502, "unknown") from None
    except (aiohttp.ClientError, ConnectionError, Rejected):
        raise Rejected("result_unknown", 502, "unknown") from None
    finally:
        record_observation(receipt, observer, complete)
        receipt["observed_at"] = utcnow().isoformat().replace("+00:00", "Z")
        safe_receipt = redact(receipt, secrets)
        if validate_receipt is not None:
            validate_receipt(safe_receipt)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        ledger.native_finish(
            safe_receipt,
            reason,
            elapsed_ms,
            upstream_status,
            attempt_metrics(
                safe_receipt, observer, elapsed_ms, bool(body.get("stream")), inspected
            ),
        )
