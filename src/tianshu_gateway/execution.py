"""One live task per owned request, with content-free execution observations.

The existing receipt remains the only durable record. This registry only identifies a
currently cancellable local task; absence never proves that a provider did not execute.
"""

import asyncio
import time
from dataclasses import dataclass, field

from .contracts import Rejected

CAPABILITIES = ("text", "stream", "tools", "vision", "reasoning")
FINISH_REASONS = frozenset({"stop", "length", "tool_calls", "function_call", "content_filter"})
MAX_COUNTER = 2**63 - 1


def requested_capabilities(body, protocol="openai-chat-completions"):
    required = {"text"}
    if body.get("stream"):
        required.add("stream")
    if body.get("tools") or body.get("tool_choice") not in (None, "none"):
        required.add("tools")
    if "reasoning_effort" in body or "reasoning" in body:
        required.add("reasoning")
    # Inspect only native content positions, never tool schemas or arbitrary JSON data.
    messages = (
        body.get("messages", []) if protocol == "openai-chat-completions" else body.get("input", [])
    )
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            if (
                message.get("role") == "tool"
                or message.get("tool_calls")
                or message.get("type")
                in {
                    "function_call",
                    "function_call_output",
                    "custom_tool_call",
                    "custom_tool_call_output",
                }
            ):
                required.add("tools")
            parts = message.get("content", [])
            if isinstance(parts, list) and any(
                isinstance(part, dict) and part.get("type") in {"image_url", "input_image"}
                for part in parts
            ):
                required.add("vision")
            if message.get("type") == "input_image":
                required.add("vision")
    return required


def check_capabilities(provider, body):
    verified = set(provider.get("verified_capabilities", []))
    unsupported = set(provider.get("unsupported_capabilities", []))
    if verified & unsupported:
        raise Rejected("invalid_input", 400)
    if requested_capabilities(body, provider["protocol"]) & unsupported:
        raise Rejected("capability_unsupported", 422)


def capability_projection(provider, request_id, version):
    verified = set(provider.get("verified_capabilities", []))
    unsupported = set(provider.get("unsupported_capabilities", []))
    if verified & unsupported:
        raise Rejected("invalid_input", 400)
    source = provider.get("capability_verification", "not_verified")
    if source not in {"fixture_only", "verified_test_account", "provider_test", "not_verified"}:
        source = "not_verified"
    return {
        "schema_version": 1,
        "request_id": request_id,
        "config_version": version,
        "provider_id": provider["provider_id"],
        "model_id": provider["model_id"],
        "protocol": provider["protocol"],
        "verification_source": source,
        "capabilities": {
            name: {
                "native": True,
                "verification": "unsupported"
                if name in unsupported
                else "verified"
                if name in verified and source in {"verified_test_account", "provider_test"}
                else "unverified",
            }
            for name in CAPABILITIES
        },
    }


@dataclass
class Execution:
    task: asyncio.Task
    receipt: dict
    secrets: tuple = field(default=(), repr=False)
    state: str = "queued"
    upstream_started: bool = False
    received_bytes: int = 0
    forwarded_bytes: int = 0
    event_count: int = 0
    output_observed: bool = False
    finish_reasons: set = field(default_factory=set)
    cancel_requested: bool = False
    error_code: str | None = None
    persisted_at: float = 0
    persisted_bytes: int = 0
    persisted_output: bool = False

    def snapshot(self):
        result = {
            "state": self.state,
            "upstream_started": self.upstream_started,
            "received_bytes": self.received_bytes,
            "forwarded_bytes": self.forwarded_bytes,
            "event_count": self.event_count,
            "output_observed": self.output_observed,
            "finish_reasons": sorted(self.finish_reasons),
            "cancel_requested": self.cancel_requested,
            "error_code": self.error_code,
        }
        self.receipt["execution"] = result
        return result

    def start(self):
        self.upstream_started = True
        self.state = "running"
        self.snapshot()

    def observe(self, received=0, forwarded=0, observer=None, choices=None):
        self.received_bytes = min(MAX_COUNTER, self.received_bytes + received)
        self.forwarded_bytes = min(MAX_COUNTER, self.forwarded_bytes + forwarded)
        if observer is not None:
            self.event_count = min(MAX_COUNTER, observer.event_count)
            self.output_observed |= observer.first_output_ms is not None
            self.finish_reasons.update(observer.finish_reasons)
        if choices is not None:
            self.output_observed = True
            for choice in choices:
                reason = choice.get("finish_reason") if isinstance(choice, dict) else None
                if isinstance(reason, str) and reason:
                    self.finish_reasons.add(reason if reason in FINISH_REASONS else "other")
        return self.snapshot()

    def checkpoint_due(self):
        # First observed output is persisted immediately; long streams checkpoint at a
        # bounded cadence rather than writing SQLite once per token or network fragment.
        now = time.monotonic()
        due = (
            not self.persisted_at
            or (self.output_observed and not self.persisted_output)
            or now - self.persisted_at >= 0.25
            or (self.forwarded_bytes - self.persisted_bytes >= 65536)
        )
        if due:
            self.persisted_at, self.persisted_bytes = now, self.forwarded_bytes
            self.persisted_output = self.output_observed
        return due

    def finish(self, reason, outcome):
        self.state = {
            "completed": "completed",
            "response_completed": "completed",
            "response_failed": "failed",
            "cancelled_unknown": "cancelled",
            "cancelled_before_start": "cancelled",
        }.get(reason, "failed" if outcome == "failed" else "unknown")
        self.error_code = {
            "timeout_unknown": "timeout",
            "cancelled_unknown": "cancelled",
            "cancelled_before_start": "cancelled",
            "incomplete_stream": "stream_incomplete",
            "observation_incomplete": "stream_incomplete",
            "response_incomplete": "stream_incomplete",
            "upstream_http_error": "upstream_http_error",
            "transport_unknown": "transport_error",
            "response_failed": "upstream_http_error",
        }.get(reason)
        return self.snapshot()


class Executions:
    def __init__(self):
        self.active = {}

    def register(self, key, receipt, secrets=()):
        if key in self.active:
            # A second live launch retains the existing public 400 refusal.
            # A durable receipt conflict remains the ledger's separate 409 outcome.
            raise Rejected("invalid_input", 400)
        execution = Execution(asyncio.current_task(), receipt, tuple(secrets))
        execution.snapshot()
        self.active[key] = execution
        return execution

    def discard(self, key, execution):
        if self.active.get(key) is execution:
            del self.active[key]
