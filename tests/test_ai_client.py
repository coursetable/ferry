import unittest
from types import ModuleType
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from ferry.ai.client import LLMClient


class RecordingCompletions:
    def __init__(self, *, content: str | None = "Summary") -> None:
        super().__init__()
        self.request: dict[str, Any] | None = None
        self.content = content

    async def create(self, **kwargs: Any) -> SimpleNamespace:
        self.request = kwargs
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=self.content))]
        )


class LLMClientTests(unittest.IsolatedAsyncioTestCase):
    messages = [{"role": "user", "content": "Summarize this"}]

    async def _complete(
        self,
        model: str,
        *,
        content: str | None = "Summary",
        request_model: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        openai = ModuleType("openai")
        setattr(openai, "RateLimitError", type("RateLimitError", (Exception,), {}))
        completions = RecordingCompletions(content=content)
        client = object.__new__(LLMClient)
        client._client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions)
        )
        client.model = model

        with patch.dict("sys.modules", {"openai": openai}):
            result = await client.complete(
                self.messages,
                model=request_model,
                temperature=0.7,
                max_tokens=900,
            )

        assert completions.request is not None
        return result, completions.request

    async def test_reasoning_models_use_reasoning_parameters(self) -> None:
        for model in (
            "gpt-5.6-luna",
            "openai/gpt-5.6-luna",
            "o3",
            "openai/o3",
        ):
            with self.subTest(model=model):
                result, request = await self._complete(model)

                self.assertEqual(result, "Summary")
                self.assertEqual(
                    request,
                    {
                        "model": model,
                        "messages": self.messages,
                        "max_completion_tokens": 900,
                        "reasoning_effort": "low",
                    },
                )

    async def test_legacy_models_use_legacy_parameters(self) -> None:
        for model in (
            "gpt-4.1-mini",
            "openai/gpt-4.1-mini",
            "notgpt-5",
            "foo-o3",
        ):
            with self.subTest(model=model):
                result, request = await self._complete(model)

                self.assertEqual(result, "Summary")
                self.assertEqual(
                    request,
                    {
                        "model": model,
                        "messages": self.messages,
                        "temperature": 0.7,
                        "max_tokens": 900,
                    },
                )

    async def test_request_model_override_controls_parameters(self) -> None:
        _, request = await self._complete(
            "gpt-4.1-mini", request_model="openai/gpt-5.6-luna"
        )

        self.assertEqual(request["model"], "openai/gpt-5.6-luna")
        self.assertEqual(request["reasoning_effort"], "low")
        self.assertEqual(request["max_completion_tokens"], 900)
        self.assertNotIn("temperature", request)
        self.assertNotIn("max_tokens", request)

    async def test_blank_content_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "empty content"):
            await self._complete("gpt-5.6-luna", content="   \n")
