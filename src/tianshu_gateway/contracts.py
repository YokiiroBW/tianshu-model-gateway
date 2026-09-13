"""Consume the immutable published contract, with local-only schema resolution."""

import hashlib
import json
import math
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry, Resource

MANIFEST_SHA256 = "81e6cc4ddef7c6f82e055d4cb04b090db036dd5c52763473ce697aa02db478a1"
MODEL_SCHEMA_SHA256 = "9ffa9055aca897ff57292555f37674d79df687d1d6c062df48267527a8ff3a7c"


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
    def __init__(self, directory):
        root = Path(directory).resolve()

        def read(path, expected):
            content = path.read_bytes().replace(b"\r\n", b"\n")
            if hashlib.sha256(content).hexdigest() != expected:
                raise ValueError("published contract hash mismatch")
            return content

        manifest = loads(read(root / "manifest.json", MANIFEST_SHA256))
        if manifest["version"] != "1.0.0":
            raise ValueError("unsupported contract release")
        resources = []
        for filename in ("common", "model"):
            relative = f"text-dialogue/v1/schemas/{filename}.json"
            schema = loads(
                read(root / "schemas" / f"{filename}.json", manifest["sha256"][relative])
            )
            resources.append((schema["$id"], Resource.from_contents(schema)))
        self.registry = Registry().with_resources(resources)

    def validate(self, name, document):
        family, kind = name.split("#")
        schema = {
            "$ref": f"https://contracts.tianshu.invalid/text-dialogue/v1/{family}.json#/$defs/{kind}"
        }
        try:
            Draft202012Validator(
                schema, registry=self.registry, format_checker=FormatChecker()
            ).validate(document)
        except (ValidationError, RecursionError):
            raise Rejected() from None
