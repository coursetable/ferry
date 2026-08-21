import asyncio
import sys
import unittest
from types import ModuleType
from typing import Any
from unittest.mock import patch


class RecordingLLM:
    def __init__(self) -> None:
        super().__init__()
        self.request: dict[str, Any] | None = None

    async def complete(
        self, messages: list[dict[str, str]], **kwargs: Any
    ) -> str:
        self.request = {"messages": messages, **kwargs}
        return "Summary"


class SummarizeCommentsTests(unittest.IsolatedAsyncioTestCase):
    async def test_reserves_tokens_for_reasoning_and_summary_text(self) -> None:
        openai = ModuleType("openai")
        setattr(openai, "RateLimitError", type("RateLimitError", (Exception,), {}))
        sys.modules.pop("ferry.summarize.summarize_evals", None)

        with patch.dict("sys.modules", {"openai": openai}):
            from ferry.summarize.summarize_evals import _summarize_comments

            llm = RecordingLLM()
            result = await _summarize_comments(
                llm,  # type: ignore[arg-type]
                "How was the course?",
                ["Great", "Useful", "Challenging"],
                asyncio.Semaphore(1),
            )

        self.assertEqual(result, "Summary")
        assert llm.request is not None
        self.assertEqual(llm.request["max_tokens"], 1024)
