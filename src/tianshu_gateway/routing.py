"""Native payload policy and bounded observation, independent from HTTP forwarding."""

import copy
import json
import time

from .config import utcnow
from .contracts import Rejected, loads

PROTOCOL = "openai-chat-completions"
STATE_KEYS = {
    "previous_response_id",
    "response_id",
    "conversation_id",
    "conversation",
    "file_id",
    "container_id",
}


def has_state_reference(value):
    if isinstance(value, dict):
        return any(k in STATE_KEYS or has_state_reference(v) for k, v in value.items())
    if isinstance(value, list):
        return any(has_state_reference(v) for v in value)
    return False


def reasoning(body):
    # Native fields remain in the body; this is only an independent diagnostic projection.
    return {k: copy.deepcopy(body[k]) for k in ("reasoning_effort",) if k in body}


def apply_fields(raw, effective, policies):
    """Replace only policy-owned top-level values; other JSON bytes retain precision."""
    if not policies:
        return raw
    text = raw.decode("utf-8")
    decoder = json.JSONDecoder()
    fields = {policy["field"] for policy in policies}
    edits = []
    index = text.index("{") + 1
    while True:
        while text[index].isspace():
            index += 1
        if text[index] == "}":
            break
        key, index = decoder.raw_decode(text, index)
        while text[index].isspace() or text[index] == ":":
            index += 1
        start = index
        _, index = decoder.raw_decode(text, index)
        if key in fields:
            edits.append(
                (start, index, json.dumps(effective[key], ensure_ascii=False, allow_nan=False))
            )
            fields.remove(key)
        while text[index].isspace():
            index += 1
        if text[index] == ",":
            index += 1
    if fields:
        addition = "".join(
            ","
            + json.dumps(key)
            + ":"
            + json.dumps(effective[key], ensure_ascii=False, allow_nan=False)
            for key in sorted(fields)
        )
        edits.append((index, index, addition))
    for start, end, replacement in reversed(edits):
        text = text[:start] + replacement + text[end:]
    return text.encode("utf-8")


def prepare(contracts, body, config, grant, request_id, internal):
    contracts.validate("model#native_request", body)
    if "model" in body and (
        not isinstance(body["model"], str)
        or not body["model"].strip()
        or any(ord(c) < 32 or ord(c) == 127 for c in body["model"])
    ):
        raise Rejected()
    if has_state_reference(
        {k: v for k, v in body.items() if k not in {"tools", "response_format"}}
    ):
        # No state store for this slice. Never reinterpret an unknown reference on a new route.
        raise Rejected("invalid_input", 400)
    if "n" in body and (type(body["n"]) is not int or not 1 <= body["n"] <= 128):
        raise Rejected()
    binding = next(b for b in config["bindings"] if b["workload"] == "companion.text")
    if binding["provider_id"] != grant.provider_id:
        raise Rejected("forbidden", 403)
    provider = next(p for p in config["providers"] if p["provider_id"] == binding["provider_id"])
    effective = copy.deepcopy(body)
    applied = []
    for policy_name in ("model_policy", "reasoning_policy"):
        policy = provider[policy_name]
        for field, value in policy["fields"].items():
            if policy["mode"] == "force" or (
                policy["mode"] == "default_if_absent" and field not in body
            ):
                effective[field] = copy.deepcopy(value)
                applied.append(
                    {
                        "field": field,
                        "mode": policy["mode"],
                        "config_version": config["config_version"],
                    }
                )
    if "model" not in effective and internal:
        effective["model"] = binding["model_id"]
        applied.append(
            {
                "field": "model",
                "mode": "workload_binding",
                "config_version": config["config_version"],
            }
        )
    contracts.validate("model#upstream_request", effective)
    if not effective["model"].strip() or any(
        ord(c) < 32 or ord(c) == 127 for c in effective["model"]
    ):
        raise Rejected()
    receipt = {
        "schema_version": 1,
        "request_id": request_id,
        "config_version": config["config_version"],
        "provider_id": provider["provider_id"],
        "credential_namespace": provider["credential_namespace"],
        "caller_service": grant.service,
        "requested_model": body.get("model"),
        "resolved_model": effective["model"],
        "requested_reasoning": reasoning(body),
        "effective_reasoning": reasoning(effective),
        "applied_policies": applied,
        "protocol": PROTOCOL,
        "outcome": "unknown",
        "upstream_request_id": None,
        "usage": None,
        "usage_complete": False,
        "native_usage": None,
        "fallback_used": False,
        "observed_at": utcnow().isoformat().replace("+00:00", "Z"),
    }
    return effective, provider, binding, receipt


def record_usage(receipt, native, complete):
    if not isinstance(native, dict):
        return
    usage = {}
    for source, target in (
        ("prompt_tokens", "input_tokens"),
        ("completion_tokens", "output_tokens"),
    ):
        value = native.get(source)
        if type(value) is int and value >= 0:
            usage[target] = value
    receipt["native_usage"] = native
    receipt["usage"] = usage or None
    receipt["usage_complete"] = complete and len(usage) == 2


def chat_output(value):
    """First model output on the wire: a real delta, never a keepalive or a terminal.

    This is a diagnostic projection only. It never raises: an unrecognized shape simply
    does not count as output, so a provider dialect we do not model can never turn a
    forwardable stream into a failure.
    """
    choices = value.get("choices")
    if not isinstance(choices, list):
        return False
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        delta = choice.get("delta")
        if not isinstance(delta, dict):
            continue
        for key in ("content", "reasoning_content", "refusal"):
            text = delta.get(key)
            if isinstance(text, str) and text:
                return True
        calls = delta.get("tool_calls")
        if not isinstance(calls, list):
            continue
        for call in calls:
            if not isinstance(call, dict):
                continue
            if isinstance(call.get("id"), str) and call["id"]:
                return True
            function = call.get("function")
            if isinstance(function, dict) and any(
                isinstance(function.get(key), str) and function[key]
                for key in ("name", "arguments")
            ):
                return True
    return False


class StreamObserver:
    """Bounded SSE observer; never reserializes the outgoing bytes.

    Beside the completion verdict it stamps three monotonic timings from the same start
    instant as the attempt: the first upstream byte, the first complete SSE event on the
    wire, and the first recognized model output. Stamping is additive bookkeeping - it
    never rewrites, delays or drops a byte, so byte fidelity, backpressure, timeout,
    cancellation and tool-call streaming are unchanged.
    """

    def __init__(self, expected_choices=1, limit=262144, started=None):
        self.expected_choices, self.limit = expected_choices, limit
        self.started = time.monotonic() if started is None else started
        self.buffer = bytearray()
        self.data = []
        self.event_type = b""
        self.event_size = 0
        self.done = self.invalid = self.error = False
        self.finished = set()
        self.native_usage = None
        self.first_upstream_byte_ms = None
        self.first_event_ms = None
        self.first_output_ms = None

    def elapsed_ms(self):
        return int((time.monotonic() - self.started) * 1000)

    def feed(self, chunk):
        if self.invalid:
            return
        if chunk and self.first_upstream_byte_ms is None:
            self.first_upstream_byte_ms = self.elapsed_ms()
        self.buffer.extend(chunk)
        while True:
            positions = [i for i in (self.buffer.find(b"\n"), self.buffer.find(b"\r")) if i >= 0]
            if not positions:
                break
            index = min(positions)
            if self.buffer[index] == 13 and index == len(self.buffer) - 1:
                break
            length = 2 if self.buffer[index : index + 2] == b"\r\n" else 1
            line = bytes(self.buffer[:index])
            del self.buffer[: index + length]
            self.event_size += len(line)
            if self.event_size > self.limit:
                self.invalid = True
                break
            if not line:
                self.dispatch()
                self.data.clear()
                self.event_type = b""
                self.event_size = 0
            else:
                # SSE splits at the first colon and removes at most one leading space.
                # A colonless field has an empty value; comments/unknown fields are ignored.
                field, _, value = line.partition(b":")
                value = value.removeprefix(b" ")
                if field == b"data":
                    self.data.append(value)
                elif field == b"event":
                    self.event_type = value
        if len(self.buffer) > self.limit or self.invalid:
            self.invalid = True
            self.buffer.clear()
            self.data.clear()

    def dispatch(self):
        if not self.data:
            return
        if self.first_event_ms is None:
            self.first_event_ms = self.elapsed_ms()
        if self.event_type == b"error":
            self.error = True
        data = b"\n".join(self.data)
        if self.done:
            self.invalid = True
            return
        if data == b"[DONE]":
            self.done = True
            return
        try:
            value = loads(data)
            if not isinstance(value, dict):
                raise Rejected()
            if "error" in value:
                self.error = True
            # A repeated usage-bearing chunk replaces the previous observation; usage is
            # never summed or merged across fragments, so a duplicate terminal cannot
            # count the same tokens twice.
            if isinstance(value.get("usage"), dict):
                self.native_usage = value["usage"]
            if self.first_output_ms is None and chat_output(value):
                self.first_output_ms = self.elapsed_ms()
            for choice in value.get("choices", []):
                index = choice.get("index")
                finish = choice.get("finish_reason")
                if type(index) is not int or not 0 <= index < self.expected_choices:
                    raise Rejected()
                if type(index) is int and isinstance(finish, str) and finish:
                    self.finished.add(index)
        except (Rejected, AttributeError, TypeError):
            self.invalid = True

    def end(self):
        # SSE accepts a final CR without LF; normalize only the observer's line scanner.
        if self.buffer.endswith(b"\r"):
            self.feed(b"\n")

    @property
    def complete(self):
        return (
            self.done
            and not self.invalid
            and not self.error
            and self.finished == set(range(self.expected_choices))
            and not self.buffer.strip()
            and not self.data
        )


class SecretGuard:
    """Withhold a small byte tail so a split reflected credential cannot escape."""

    def __init__(self, secrets):
        self.patterns = {
            encoded for s in secrets if s for encoded in (s.encode(), json.dumps(s)[1:-1].encode())
        }
        self.tail_size = max((len(s) for s in self.patterns), default=1) - 1
        self.pending = b""

    def feed(self, chunk, final=False):
        combined = self.pending + chunk
        if any(s in combined for s in self.patterns):
            raise Rejected("result_unknown", 502, "unknown")
        split = len(combined) if final else max(0, len(combined) - self.tail_size)
        self.pending = combined[split:]
        return combined[:split]
