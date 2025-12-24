from __future__ import annotations

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential


def build_client(api_key: str) -> OpenAI:
    return OpenAI(api_key=api_key)


@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(4))
def responses_parse(*, client: OpenAI, model: str, input_messages: list[dict], text_format, temperature: float):
    """
    Wrapper with retries around client.responses.parse.
    """
    return client.responses.parse(
        model=model,
        input=input_messages,
        text_format=text_format,
        temperature=temperature,
        store=False,
    )
