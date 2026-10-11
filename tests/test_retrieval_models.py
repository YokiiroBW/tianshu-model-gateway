import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tianshu_gateway.provider_adapter import OpenAIAdapter, ExecutionContext, ProviderFailure
from tianshu_gateway.retrieval_models import execute


class RetrievalTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.context = ExecutionContext(
            "embedding", 1, "https://models.example/v1", "synthetic", "model"
        )
        self.adapter = SimpleNamespace(_identifier=OpenAIAdapter._identifier, _request=AsyncMock())

    async def test_embeddings_restored_to_input_order(self):
        self.adapter._request.return_value = {
            "data": [{"index": 1, "embedding": [0, 1]}, {"index": 0, "embedding": [1, 0]}]
        }
        answer = await execute(self.adapter, self.context, "embed", {"texts": ["甲", "乙"]})
        self.assertEqual(answer["vectors"], [[1, 0], [0, 1]])
        self.assertEqual(self.adapter._request.call_args.args[1], "embeddings")
        self.assertIsNone(answer["usage"])

    async def test_rerank_validates_complete_unique_indices(self):
        for rows in [
            [{"index": 0, "relevance_score": 1}] * 2,
            [{"index": 0, "relevance_score": float("nan")}, {"index": 1, "relevance_score": 1}],
            [],
        ]:
            self.adapter._request.return_value = {"results": rows}
            with self.assertRaises(ProviderFailure):
                await execute(
                    self.adapter, self.context, "rerank", {"query": "问", "documents": ["甲", "乙"]}
                )
        self.adapter._request.return_value = {
            "results": [{"index": 1, "relevance_score": 0.8}, {"index": 0, "relevance_score": 0.2}]
        }
        answer = await execute(
            self.adapter, self.context, "rerank", {"query": "问", "documents": ["甲", "乙"]}
        )
        self.assertEqual(answer["scores"], [0.2, 0.8])

    async def test_malformed_vectors_and_payload_never_succeed(self):
        for vector in [[0, 0], [True], [float("inf")], "vector"]:
            self.adapter._request.return_value = {"data": [{"index": 0, "embedding": vector}]}
            with self.assertRaises(ProviderFailure):
                await execute(self.adapter, self.context, "embed", {"texts": ["甲"]})
        with self.assertRaises(ProviderFailure):
            await execute(self.adapter, self.context, "embed", {"texts": ["甲"], "url": "ignored"})
