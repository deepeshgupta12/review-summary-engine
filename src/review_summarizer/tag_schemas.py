from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class ReviewTagItem(BaseModel):
    review_uid: str = Field(..., description="Deterministic unique id for the review row")
    tags: list[str] = Field(..., min_length=3, max_length=3, description="Exactly 3 UI tags")

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: list[str]) -> list[str]:
        if len(v) != 3:
            raise ValueError("tags must have exactly 3 items")

        clean: list[str] = []
        for t in v:
            t = (t or "").strip().strip('"').strip("'")
            if not t:
                raise ValueError("empty tag")
            if len(t) > 28:
                raise ValueError("tag too long (>28 chars)")
            clean.append(t)

        return clean


class ReviewTagBatch(BaseModel):
    items: list[ReviewTagItem] = Field(..., description="Tag outputs for the input reviews")
