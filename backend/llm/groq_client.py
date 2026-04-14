import logging
from typing import AsyncGenerator

from groq import AsyncGroq, AsyncStream
from groq.types.chat import ChatCompletionChunk

logger = logging.getLogger(__name__)

_MODEL = "llama-3.3-70b-versatile"
_MAX_TOKENS = 1024


class GroqClient:
    def __init__(self, api_key: str):
        self._client = AsyncGroq(api_key=api_key)

    async def stream(self, messages: list[dict]) -> AsyncGenerator[str, None]:
        """Stream token text from the Groq API.

        Yields each non-empty content string as it arrives.
        """
        stream: AsyncStream[ChatCompletionChunk] = await self._client.chat.completions.create(
            model=_MODEL,
            messages=messages,
            max_tokens=_MAX_TOKENS,
            stream=True,
        )
        try:
            async for chunk in stream:
                delta = chunk.choices[0].delta if chunk.choices else None
                if delta and delta.content:
                    yield delta.content
        except Exception as exc:
            logger.error("Groq streaming error: %s", exc)
            raise
        finally:
            await stream.close()
