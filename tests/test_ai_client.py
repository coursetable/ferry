import unittest
from types import ModuleType
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from ferry.ai.client import LLMClient


class RecordingCompletions:
    def __init__(self) -> None:
        self.request: dict[str, Any] | None = None

    async def create(self, **kwargs: Any) -> SimpleNamespace:
        self.request = kwargs
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="Summary"))]
        )


class LLMClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_reasoning_models_use_medium_reasoning_effort(self) -> None:
        openai = ModuleType("openai")
        openai.RateLimitError = type("RateLimitError", (Exception,), {})
        completions = RecordingCompletions()
        client = object.__new__(LLMClient)
        client._client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions)
        )
        client.model = "gpt-5.6-luna"

        with patch.dict("sys.modules", {"openai": openai}):
            await client.complete([{"role": "user", "content": "Summarize this"}])

        assert completions.request is not None
        self.assertEqual(completions.request.get("reasoning_effort"), "medium")

    async def test_legacy_models_omit_reasoning_effort(self) -> None:
        openai = ModuleType("openai")
        openai.RateLimitError = type("RateLimitError", (Exception,), {})
        completions = RecordingCompletions()
        client = object.__new__(LLMClient)
        client._client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions)
        )
        client.model = "gpt-4.1-mini"

        with patch.dict("sys.modules", {"openai": openai}):
            await client.complete([{"role": "user", "content": "Summarize this"}])

        assert completions.request is not None
        self.assertNotIn("reasoning_effort", completions.request)
