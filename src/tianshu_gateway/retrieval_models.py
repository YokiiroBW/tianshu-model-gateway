"""Embedding and reranking protocols through the gateway's existing bounded transport."""

import math

from .provider_adapter import ProviderFailure


def checked(condition):
    if not condition:
        raise ProviderFailure("invalid_response", "unknown")


async def execute(adapter, context, operation, payload):
    if not isinstance(payload, dict) or not adapter._identifier(context.model_id):
        raise ProviderFailure("invalid_request")
    key = "texts" if operation == "embed" else "documents"
    items = payload.get(key)
    if (
        operation not in {"embed", "rerank"}
        or not isinstance(items, list)
        or not 1 <= len(items) <= (32 if operation == "embed" else 100)
        or not all(isinstance(item, str) and 0 < len(item) <= 32768 for item in items)
    ):
        raise ProviderFailure("invalid_request")
    if operation == "embed":
        if set(payload) != {"texts"}:
            raise ProviderFailure("invalid_request")
        body = {"model": context.model_id, "input": items, "encoding_format": "float"}
        result = await adapter._request(context, "embeddings", body, "public")
        rows = result.get("data")
        field = "embedding"
    else:
        query = payload.get("query")
        if (
            set(payload) != {"query", "documents"}
            or not isinstance(query, str)
            or not 1 <= len(query) <= 32768
        ):
            raise ProviderFailure("invalid_request")
        body = {
            "model": context.model_id,
            "query": query,
            "documents": items,
            "top_n": len(items),
            "return_documents": False,
        }
        result = await adapter._request(context, "rerank", body, "public")
        rows = result.get("results")
        field = "relevance_score"
    checked(isinstance(rows, list) and len(rows) == len(items))
    ordered = [None] * len(items)
    dimension = None
    for row in rows:
        checked(isinstance(row, dict))
        index = row.get("index")
        checked(type(index) is int and 0 <= index < len(items) and ordered[index] is None)
        value = row.get(field)
        if operation == "embed":
            checked(isinstance(value, list) and 1 <= len(value) <= 8192)
            checked(all(type(n) in (int, float) and math.isfinite(n) for n in value))
            checked(0 < math.hypot(*value) < math.inf)
            dimension = dimension or len(value)
            checked(len(value) == dimension)
        else:
            checked(type(value) in (int, float) and math.isfinite(value))
        ordered[index] = value
    # Upstream usage is returned when supplied; no invented token/cost values.
    return {
        "vectors" if operation == "embed" else "scores": ordered,
        "usage": result.get("usage", result.get("meta")),
    }
