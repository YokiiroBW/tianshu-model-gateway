"""Offline release validator. Requires only pinned common.json and schema libraries.

No product imports, workspace discovery, environment configuration, network fetch,
or request rewriting. Trusted-access arguments are server-produced projections;
validating their shape does not authenticate a caller.
"""

import argparse
import copy
import hashlib
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry, Resource

PACKAGE = "model-protocol/v1"
COMMON_ID = "https://contracts.tianshu.invalid/text-dialogue/v1/common.json"
COMMON_SHA256 = "b296a79d7eb0218d9b444c4ddbba837ad576f46a7a4e50fbcebec7618d8ef9af"


class Invalid(ValueError):
    def __init__(self, code="invalid_input"):
        self.code = code
        super().__init__(code)


def require(condition, code="invalid_input"):
    if not condition:
        raise Invalid(code)


def normalized(path):
    return path.read_bytes().replace(b"\r\n", b"\n")


def digest(path):
    return hashlib.sha256(normalized(path)).hexdigest()


def load(path):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value)
            value[key] = item
        return value

    def number(value):
        parsed = Decimal(value)
        require(parsed.is_finite())
        return parsed

    def constant(_):
        raise Invalid()

    try:
        return json.loads(
            normalized(path), object_pairs_hook=pairs, parse_float=number, parse_constant=constant
        )
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise Invalid() from exc


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def same_json(left, right):
    """Compare parsed values without Python bool/int coercion or float rounding."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(same_json(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(same_json(a, b) for a, b in zip(left, right))
    return left == right


def relation_examples(document):
    """Expand local fixture leaf edits; not a production patch or publication API."""
    for example in document["cases"]:
        exchange = copy.deepcopy(document["base"])
        for change in example["changes"]:
            path = change["path"]
            require(path)
            target = exchange
            for key in path[:-1]:
                target = target[key]
            key = path[-1]
            if change.get("remove"):
                del target[key]
            elif isinstance(target, list) and key == len(target):
                target.append(copy.deepcopy(change["value"]))
            else:
                target[key] = copy.deepcopy(change["value"])
        yield {**example, "exchange": exchange}


def reject_state(body):
    for key in (
        "previous_response_id",
        "conversation",
        "conversation_id",
        "response_id",
        "prompt",
        "file_id",
        "container_id",
    ):
        require(body.get(key) is None, "state_reference_unsupported")
    cache = body.get("prompt_cache_options")
    if isinstance(cache, dict):
        require(cache.get("comparison_response_id") is None, "state_reference_unsupported")
    require(not body.get("background"), "unsupported_operation")
    tools = body.get("tools", [])
    if isinstance(tools, list):
        for tool in tools:
            require(isinstance(tool, dict))
            require(tool.get("type") in {"function", "custom"}, "unsupported_operation")

    def items(value):
        if not isinstance(value, list):
            return
        for item in value:
            require(isinstance(item, dict))
            require(
                not (
                    item.get("type") == "item_reference"
                    or ("id" in item and set(item) <= {"id", "type"})
                ),
                "state_reference_unsupported",
            )
            for field in ("file_id", "container_id", "response_id"):
                require(item.get(field) is None, "state_reference_unsupported")
            require(
                item.get("type")
                not in {"input_file", "input_image", "input_audio", "computer_call"},
                "unsupported_operation",
            )
            items(item.get("content"))
            if item.get("type") in {"function_call_output", "custom_tool_call_output"}:
                items(item.get("output"))

    items(body.get("input"))
    items(body.get("instructions"))


class Contract:
    def __init__(self, root, common):
        self.root = Path(root).resolve()
        manifest = load(self.root / "manifest.json")
        require(
            manifest["package"] == PACKAGE and manifest["version"] == "1.0.0", "unsupported_version"
        )
        require(
            manifest["dependencies"]
            == [
                {
                    "id": COMMON_ID,
                    "package": "text-dialogue/v1",
                    "version": "1.0.0",
                    "path": "schemas/common.json",
                    "sha256": COMMON_SHA256,
                }
            ],
            "unsupported_version",
        )
        require(digest(Path(common)) == COMMON_SHA256, "dependency_unavailable")
        entries = manifest["sha256"]
        require(isinstance(entries, dict) and entries)
        for relative, expected in entries.items():
            target = (self.root / relative).resolve()
            require(target.is_relative_to(self.root) and target != self.root, "invalid_input")
            require(digest(target) == expected, "version_conflict")
        files = {
            p.relative_to(self.root).as_posix()
            for p in self.root.rglob("*")
            if p.is_file() and "__pycache__" not in p.parts and p.name != "manifest.json"
        }
        require(files == set(entries), "version_conflict")
        self.schema = load(self.root / "schemas/model.json")
        common_schema = load(Path(common))
        require(common_schema["$id"] == COMMON_ID, "unsupported_version")
        allowed_ids = {self.schema["$id"], COMMON_ID, ""}

        def local_references(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key in {"$ref", "$dynamicRef"}:
                        require(item.split("#", 1)[0] in allowed_ids, "unsupported_version")
                    local_references(item)
            elif isinstance(value, list):
                for item in value:
                    local_references(item)

        local_references(self.schema)
        Draft202012Validator.check_schema(self.schema)
        self.registry = Registry().with_resources(
            (s["$id"], Resource.from_contents(s)) for s in (self.schema, common_schema)
        )
        self.errors = load(self.root / "errors.json")["http_status_by_code"]

    def shape(self, kind, document):
        require(kind in self.schema["$defs"], "unsupported_version")
        try:
            Draft202012Validator(
                {"$ref": self.schema["$id"] + "#/$defs/" + kind},
                registry=self.registry,
                format_checker=FormatChecker(),
            ).validate(document)
        except ValidationError as exc:
            raise Invalid() from exc

    def validate(self, kind, document):
        # State/scope errors precede shape validation so known references are explicit rejections.
        if kind in {"native_request", "upstream_request"} and isinstance(document, dict):
            reject_state(document)
        self.shape(kind, document)
        if kind in {"native_request", "upstream_request"}:
            model = document["model"]
            require(model.strip() and not any(ord(c) < 32 or ord(c) == 127 for c in model))
        if kind == "config_response":
            self.snapshot_relations(document)
        if kind == "route_receipt":
            require(document["requested_model"] == document["resolved_model"], "version_conflict")
            require(
                same_json(document["requested_reasoning"], document["effective_reasoning"]),
                "version_conflict",
            )
            self.usage(document, document["native_usage"])

    def snapshot_relations(self, snapshot):
        require(timestamp(snapshot["published_at"]) < timestamp(snapshot["usable_until"]))
        providers = {p["provider_id"]: p for p in snapshot["providers"]}
        require(len(providers) == len(snapshot["providers"]))
        workloads = set()
        for provider in providers.values():
            base = provider["base_url"]
            parts = urlsplit(base)
            require(
                parts.scheme in {"https", "http"}
                and parts.hostname
                and not parts.username
                and not parts.password
                and not parts.query
                and not parts.fragment
                and not base.endswith("/")
                and "%" not in base
                and "\\" not in base
                and not any(ord(c) <= 32 or ord(c) > 126 for c in base)
                and not any(p in {".", ".."} for p in parts.path.split("/"))
            )
        for binding in snapshot["bindings"]:
            require(binding["workload"] not in workloads)
            workloads.add(binding["workload"])
            require(binding["provider_id"] in providers)
            require(binding["model_id"] == providers[binding["provider_id"]]["model_id"])

    @staticmethod
    def usage(receipt, native):
        projection = None
        if isinstance(native, dict):
            projection = {
                key: native[key]
                for key in ("input_tokens", "output_tokens")
                if type(native.get(key)) is int and native[key] >= 0
            } or None
        require(same_json(receipt["usage"], projection), "version_conflict")
        require(
            not receipt["usage_complete"] or (projection and len(projection) == 2),
            "version_conflict",
        )

    def exchange(self, case):
        """Validate a complete fixture against a trusted authentication adapter result.

        This is executable normative relation logic, not a HTTP authentication service,
        model-config publisher, database, or source of production authorization.
        """
        auth, request = case["auth"], case["request"]
        self.shape("trusted_access", auth)
        self.validate("config_request", request)
        now = timestamp(case["now"])
        require(not auth["revoked"] and now < timestamp(auth["expires_at"]), "forbidden")
        require("config.snapshot" in auth["permissions"], "forbidden")
        require(request["query"]["origin"]["assertion_ref"] == auth["assertion_ref"], "forbidden")
        allowed = set(auth["native_config_versions"])
        require(allowed, "forbidden")  # Never read legacy config_versions, even for latest.
        version = request["native_config_version"]
        require(version is None or version in allowed, "forbidden")
        snapshots = {}
        for snapshot in case["snapshots"]:
            self.validate("config_response", snapshot)
            key = snapshot["native_config_version"]
            stable = {k: v for k, v in snapshot.items() if k != "request_id"}
            if key in snapshots:
                old_stable = {k: v for k, v in snapshots[key].items() if k != "request_id"}
                require(same_json(stable, old_stable), "version_conflict")
            snapshots[key] = snapshot
        revoked = case["revoked_native_versions"]
        if version is None:
            usable = [
                v
                for v, s in snapshots.items()
                if v in allowed
                and v not in revoked
                and timestamp(s["published_at"]) <= now < timestamp(s["usable_until"])
            ]
            require(usable, "dependency_unavailable")
            version = max(usable)
        require(version not in revoked, "forbidden")
        require(version in snapshots, "not_found")
        selected = snapshots[version]
        require(
            timestamp(selected["published_at"]) <= now < timestamp(selected["usable_until"]),
            "dependency_unavailable",
        )
        require(selected["request_id"] == request["query"]["request_id"], "version_conflict")
        context, receipt, native = case["context"], case["receipt"], case["native_request"]
        self.validate("route_context", context)
        self.validate("native_request", native)
        self.validate("upstream_request", case["upstream_request"])
        require(same_json(case["upstream_request"], native), "version_conflict")
        self.validate("route_receipt", receipt)
        for field in ("caller_service", "principal_id", "credential_namespace"):
            require(context[field] == auth[field] == receipt[field], "forbidden")
        require(
            context["native_config_version"] == version == receipt["native_config_version"],
            "version_conflict",
        )
        require(context["request_id"] == receipt["request_id"], "version_conflict")
        binding = next(b for b in selected["bindings"] if b["workload"] == context["workload"])
        provider = next(
            p for p in selected["providers"] if p["provider_id"] == binding["provider_id"]
        )
        require(provider["provider_id"] in auth["provider_ids"], "forbidden")
        require(provider["credential_namespace"] == auth["credential_namespace"], "forbidden")
        require(native["model"] == provider["model_id"], "invalid_input")
        require(receipt["provider_id"] == provider["provider_id"], "version_conflict")
        require(
            native["model"] == receipt["requested_model"] == receipt["resolved_model"],
            "version_conflict",
        )
        requested_reasoning = {"reasoning": native["reasoning"]} if "reasoning" in native else {}
        require(same_json(receipt["requested_reasoning"], requested_reasoning), "version_conflict")
        response, observation = case["native_response"], case["observation"]
        self.validate("native_response", response)
        require(receipt["response_id"] == response["id"], "version_conflict")
        require(same_json(receipt["native_usage"], response.get("usage")), "version_conflict")
        self.usage(receipt, response.get("usage"))
        complete = observation["transport_complete"] and observation["terminal"]
        require(observation["status"] == response["status"], "version_conflict")
        outcome = {"completed": "succeeded", "failed": "failed", "incomplete": "unknown"}.get(
            response["status"], "unknown"
        )
        if not complete or (
            response["status"] == "completed" and response.get("error") is not None
        ):
            outcome = "unknown"
        require(receipt["outcome"] == outcome, "version_conflict")
        require(not receipt["usage_complete"] or complete, "version_conflict")
        return version

    def verify_package(self):
        positives = load(self.root / "examples/documents.json")
        negatives = load(self.root / "examples/negative-documents.json")
        relations = list(relation_examples(load(self.root / "examples/relations.json")))
        for example in positives:
            self.validate(example["schema"], example["document"])
            if example["schema"] == "error":
                require(example["http_status"] == self.errors[example["document"]["code"]])
        for example in negatives:
            try:
                self.shape(example["schema"], example["document"])
            except Invalid:
                continue
            raise Invalid("negative_case_accepted")
        for example in relations:
            try:
                self.exchange(example["exchange"])
            except Invalid as exc:
                require(exc.code == example["expected_error"], "relation_wrong_error")
            else:
                require(example["expected_error"] is None, "negative_relation_accepted")
        return {
            "documents": len(positives),
            "negative_documents": len(negatives),
            "relations": len(relations),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--common", type=Path, required=True, help="Explicit pinned published common.json"
    )
    parser.add_argument("--document", type=Path)
    parser.add_argument("--kind", help="Optional single document definition to validate")
    args = parser.parse_args()
    try:
        contract = Contract(Path(__file__).resolve().parent, args.common)
        if args.document or args.kind:
            require(args.document is not None and args.kind is not None)
            contract.validate(args.kind, load(args.document))
            result = {"document": "valid"}
        else:
            result = contract.verify_package()
        print(json.dumps({"package": PACKAGE, "result": "valid", **result}))
        return 0
    except (Invalid, OSError, KeyError, TypeError, ValueError, RecursionError) as exc:
        print(
            "validation failed: " + (exc.code if isinstance(exc, Invalid) else "invalid_package"),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
