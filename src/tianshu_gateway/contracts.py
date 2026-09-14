"""Consume the immutable published contracts, with local-only schema resolution."""

import hashlib
import json
import math
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry, Resource

# Published text-dialogue/v1 1.0.0 (unchanged by this slice).
MANIFEST_SHA256 = "81e6cc4ddef7c6f82e055d4cb04b090db036dd5c52763473ce697aa02db478a1"
MODEL_SCHEMA_SHA256 = "9ffa9055aca897ff57292555f37674d79df687d1d6c062df48267527a8ff3a7c"
COMMON_SCHEMA_SHA256 = "b296a79d7eb0218d9b444c4ddbba837ad576f46a7a4e50fbcebec7618d8ef9af"
# Published model-protocol/v1 1.0.0 native manifest (LF-normalized SHA-256).
NATIVE_MANIFEST_SHA256 = "52711a71de56dbceebd1d5d96b2baf59a2d9551168029972d59111480f815141"
NATIVE_PACKAGE = "model-protocol/v1"
NATIVE_MODEL_ID = "https://contracts.tianshu.invalid/model-protocol/v1/model.json"
COMMON_ID = "https://contracts.tianshu.invalid/text-dialogue/v1/common.json"
CHAT_CONTRACT = "text-dialogue/v1"
CHAT_PROTOCOL = "openai-chat-completions"
CHAT_WORKLOAD = "companion.text"

# Fixed identifiers of the published native interfaces. Paths and headers are contract
# constants here so no module invents its own spelling of a cross-service name.
NATIVE_CONTRACT = "model-protocol/v1"
NATIVE_PROTOCOL = "openai-responses"
NATIVE_WORKLOAD = "native.responses"
NATIVE_VERSION_HEADER = "X-Tianshu-Native-Config-Version"
NATIVE_SNAPSHOT_PATH = "/internal/v1/model-config/native/snapshot"
NATIVE_ROUTE_PATH = "/v1/responses"
NATIVE_RECEIPT_PATH = "/internal/v1/native-model-requests/"


class Rejected(Exception):
    """Only fixed public codes, never exception details or untrusted data."""

    def __init__(self, code="invalid_input", status=400, state="not_started"):
        self.code, self.status, self.state = code, status, state
        super().__init__(code)


def loads(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(_):
        raise ValueError("non-finite JSON number")

    def finite_number(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("JSON number outside supported range")
        return result

    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        return json.loads(
            text,
            object_pairs_hook=pairs,
            parse_constant=invalid_constant,
            parse_float=finite_number,
        )
    except (ValueError, UnicodeError, RecursionError):
        raise Rejected() from None


class Contracts:
    """Verified published schemas; the Chat package is always required, native is opt-in.

    Both packages are read from explicit directories with pinned manifests and file
    digests. Nothing is fetched, discovered or defaulted at runtime, and the native
    package never reuses Chat schema constants.
    """

    def __init__(self, directory, native_directory=None):
        root = Path(directory).resolve()

        def read(path, expected):
            content = path.read_bytes().replace(b"\r\n", b"\n")
            if hashlib.sha256(content).hexdigest() != expected:
                raise ValueError("published contract hash mismatch")
            return content

        manifest = loads(read(root / "manifest.json", MANIFEST_SHA256))
        if manifest["version"] != "1.0.0":
            raise ValueError("unsupported contract release")
        entries = manifest["sha256"]
        common = read(root / "schemas/common.json", entries["text-dialogue/v1/schemas/common.json"])
        if hashlib.sha256(common).hexdigest() != COMMON_SCHEMA_SHA256:
            raise ValueError("published contract hash mismatch")
        chat_model = loads(
            read(root / "schemas/model.json", entries["text-dialogue/v1/schemas/model.json"])
        )
        if entries["text-dialogue/v1/schemas/model.json"] != MODEL_SCHEMA_SHA256:
            raise ValueError("published contract hash mismatch")
        resources = [
            (COMMON_ID, Resource.from_contents(loads(common))),
            (chat_model["$id"], Resource.from_contents(chat_model)),
        ]
        self.definitions = {"common": COMMON_ID, "model": chat_model["$id"]}
        self.native = False
        if native_directory is not None:
            native_root = Path(native_directory).resolve()
            native_manifest = loads(read(native_root / "manifest.json", NATIVE_MANIFEST_SHA256))
            if (
                native_manifest["package"] != NATIVE_PACKAGE
                or native_manifest["version"] != "1.0.0"
            ):
                raise ValueError("unsupported contract release")
            dependency = native_manifest["dependencies"]
            if dependency != [
                {
                    "id": COMMON_ID,
                    "package": CHAT_CONTRACT,
                    "version": "1.0.0",
                    "path": "schemas/common.json",
                    "sha256": COMMON_SCHEMA_SHA256,
                }
            ]:
                raise ValueError("unexpected cross-contract dependency")
            native_model = loads(
                read(
                    native_root / "schemas/model.json",
                    native_manifest["sha256"]["schemas/model.json"],
                )
            )
            if native_model["$id"] != NATIVE_MODEL_ID:
                raise ValueError("unexpected native schema identity")
            resources.append((native_model["$id"], Resource.from_contents(native_model)))
            self.definitions["native"] = NATIVE_MODEL_ID
            self.native = True
        self.registry = Registry().with_resources(resources)

    def validate(self, name, document):
        family, kind = name.split("#")
        if family not in self.definitions:
            raise ValueError("unknown contract family")
        schema = {"$ref": f"{self.definitions[family]}#/$defs/{kind}"}
        try:
            Draft202012Validator(
                schema, registry=self.registry, format_checker=FormatChecker()
            ).validate(document)
        except (ValidationError, RecursionError):
            raise Rejected() from None
