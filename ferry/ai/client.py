"""
Generic LLM client for any OpenAI-compatible chat API.

Supports OpenAI, Anthropic, Gemini, Groq, OpenRouter, and
other providers that expose an OpenAI-style API.
"""

import asyncio
import logging
import re
from typing import Any

# Default model when none is specified (OpenAI).
DEFAULT_MODEL = "gpt-4.1-mini"

# Retry config for rate limits
RATE_LIMIT_MAX_RETRIES = 5
RATE_LIMIT_INITIAL_BACKOFF_SEC = 2


def _retry_after_seconds(exc: BaseException) -> float | None:
    """Extract retry-after from error response or message. Returns None if not found."""
    response = getattr(exc, "response", None)
    if response is not None:
        retry_after = getattr(response, "headers", None)
        if retry_after and hasattr(retry_after, "get"):
            val = retry_after.get("retry-after")
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    # OpenAI error message often includes "Please try again in X.XXs"
    msg = str(exc)
    match = re.search(r"try again in (\d+(?:\.\d+)?)\s*s", msg, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except (TypeError, ValueError):
            pass
    return None


class LLMClient:
    """
    Client for OpenAI-compatible chat completion APIs.

    Use with any provider by setting `base_url` and `api_key`. Examples:
    - OpenAI: base_url=None, api_key=OPENAI_API_KEY
    - Groq: base_url="https://api.groq.com/openai/v1"
    - OpenRouter: base_url="https://openrouter.ai/api/v1"
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str | None = None,
        model: str = DEFAULT_MODEL,
    ) -> None:
        super().__init__()
        self._client = self._create_client(api_key=api_key, base_url=base_url)
        self.model = model

    @staticmethod
    def _create_client(api_key: str, base_url: str | None) -> Any:
        from openai import AsyncOpenAI

        return AsyncOpenAI(api_key=api_key, base_url=base_url)

    async def complete(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 512,
    ) -> str:
        """
        Send messages to the LLM and return the assistant reply as text.

        Parameters
        ----------
        messages
            List of {"role": "system"|"user"|"assistant", "content": str}.
        model
            Override the default model for this request.
        temperature
            Sampling temperature (0-2).
        max_tokens
            Maximum tokens in the response.

        Returns
        -------
        The assistant message content, stripped of whitespace.

        Raises
        ------
        ValueError
            If the API returns empty or None content.
        """
        from openai import RateLimitError

        model_to_use = model or self.model
        last_exc: BaseException | None = None

        for attempt in range(RATE_LIMIT_MAX_RETRIES):
            try:
                response = await self._client.chat.completions.create(
                    model=model_to_use,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                break
            except RateLimitError as exc:
                last_exc = exc
                if attempt == RATE_LIMIT_MAX_RETRIES - 1:
                    raise
                delay = _retry_after_seconds(exc)
                if delay is None:
                    delay = RATE_LIMIT_INITIAL_BACKOFF_SEC * (2**attempt)
                delay = min(delay, 60.0)
                logging.warning(
                    "Rate limit hit (attempt %d/%d), retrying in %.1fs",
                    attempt + 1,
                    RATE_LIMIT_MAX_RETRIES,
                    delay,
                )
                await asyncio.sleep(delay)
        else:
            if last_exc is not None:
                raise last_exc
            raise RuntimeError("Unexpected retry loop exit")

        content = response.choices[0].message.content
        if content is None:
            logging.warning("LLM returned None content")
            raise ValueError("LLM API returned empty content")
        return content.strip()
