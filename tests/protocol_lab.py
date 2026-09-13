"""TS-040 offline experiment; not a gateway, public schema, or API validator."""

from copy import deepcopy
from dataclasses import dataclass


class Rejected(ValueError):
    pass


def prepare(body, *, mode="preserve", parameters=None):
    """Exercise explicit field ownership without flattening native payloads."""
    result = deepcopy(body)
    parameters = parameters or {}
    if mode not in {"preserve", "defaults", "explicit_mapping"}:
        raise Rejected("unknown parameter policy")
    if mode == "preserve" and parameters:
        raise Rejected("preserve policy cannot override parameters")
    changes = []
    for path, value in parameters.items():
        if path not in {"model", "reasoning.effort", "reasoning_effort", "output_config.effort"}:
            raise Rejected("mapping field outside experiment scope")
        parts = path.split(".")
        parent = result
        for part in parts[:-1]:
            if part not in parent:
                parent[part] = {}
            if not isinstance(parent[part], dict):
                raise Rejected("cannot replace native null or scalar with an object")
            parent = parent[part]
        key = parts[-1]
        if mode == "defaults" and key in parent:
            continue  # Explicit null/false/zero are values, not missing fields.
        old = deepcopy(parent.get(key))
        present = key in parent
        parent[key] = deepcopy(value)
        changes.append({"path": path, "was_present": present, "before": old, "after": value})
    return result, changes


@dataclass(frozen=True)
class Route:
    provider: str
    credential_namespace: str
    config_version: str
    protocol: str
    model: str
    capabilities: frozenset[str]
    parameter_signature: str


def select_route(current, references, pins, *, principal):
    """references are already classified native state IDs, never user authority."""
    if not references:
        return current
    resolved = []
    for reference in references:
        pin = pins.get((principal, reference))
        if pin is None:
            raise Rejected("unknown or unauthorized state reference")
        resolved.append(pin)
    if any(pin != resolved[0] for pin in resolved):
        raise Rejected("conflicting state namespaces or snapshots")
    return resolved[0]


def can_fallback(primary, backup, *, configured, required, outcome, attempts,
                 limit, stream_started=False, tool_may_have_run=False, references=()):
    return bool(
        configured and 0 <= attempts < limit
        and outcome in {"not_sent", "rejected_without_execution"}
        and not stream_started and not tool_may_have_run and not references
        and primary.protocol == backup.protocol
        and primary.model == backup.model
        and primary.parameter_signature == backup.parameter_signature
        and required <= backup.capabilities
    )


def relay(chunks):
    """Opaque bytes: no buffering, SSE reserialization, or retry on exceptions."""
    yield from chunks


def usage_record(native_usage, *, completed, price_known=False):
    return {"native_usage": deepcopy(native_usage), "complete": completed,
            "usage_status": "reported" if native_usage is not None else "unknown",
            "price_status": "known" if price_known else "unknown",
            "cost": None}  # This lab never calculates or invents a charge.


def check_embedding(collection, requested):
    fields = ("provider", "model", "revision", "dimensions", "preprocessing", "space")
    if any(field not in collection or field not in requested for field in fields):
        raise Rejected("incomplete embedding identity")
    if any(collection[field] != requested[field] for field in fields):
        raise Rejected("embedding migration required")
