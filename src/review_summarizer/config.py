from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    openai_api_key: str
    model: str

    # Project summary settings (Step 1)
    max_reviews_per_project: int
    chunk_tokens: int
    max_review_chars: int
    temperature: float

    # Review tag settings (Step 2)
    tag_batch_tokens: int
    tag_batch_max_reviews: int
    tag_temperature: float

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

        def _float(name: str, default: float) -> float:
            v = os.getenv(name, str(default)).strip()
            try:
                return float(v)
            except ValueError:
                return default

        return Settings(
            openai_api_key=key,
            model=model,

            max_reviews_per_project=_int("OPENAI_MAX_REVIEWS_PER_PROJECT", 400),
            chunk_tokens=_int("OPENAI_CHUNK_TOKENS", 12000),
            max_review_chars=_int("OPENAI_MAX_REVIEW_CHARS", 1200),
            temperature=_float("OPENAI_TEMPERATURE", 0.2),

            tag_batch_tokens=_int("OPENAI_TAG_BATCH_TOKENS", 8000),
            tag_batch_max_reviews=_int("OPENAI_TAG_BATCH_MAX_REVIEWS", 25),
            tag_temperature=_float("OPENAI_TAG_TEMPERATURE", 0.1),

            out_dir=out_dir,
        )
