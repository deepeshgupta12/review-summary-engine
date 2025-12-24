from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    openai_api_key: str
    model: str
    max_reviews_per_project: int
    chunk_tokens: int
    max_review_chars: int
    temperature: float
    out_dir: str

    @staticmethod
    def from_env(out_dir: str) -> "Settings":
        key = os.getenv("OPENAI_API_KEY", "").strip()
        if not key:
            raise ValueError("OPENAI_API_KEY is missing. Set it in .env")

        model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

        def _int(name: str, default: int) -> int:
            v = os.getenv(name, str(default)).strip()
            try:
                return int(v)
            except ValueError:
                return default

        return Settings(
            openai_api_key=key,
            model=model,
            max_reviews_per_project=_int("OPENAI_MAX_REVIEWS_PER_PROJECT", 400),
            chunk_tokens=_int("OPENAI_CHUNK_TOKENS", 12000),
            max_review_chars=_int("OPENAI_MAX_REVIEW_CHARS", 1200),
            temperature=float(os.getenv("OPENAI_TEMPERATURE", "0.2")),
            out_dir=out_dir,
        )
