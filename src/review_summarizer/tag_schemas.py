from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class ReviewTagItem(BaseModel):
    review_uid: str = Field(..., description="Deterministic unique id for the review row")
    tags: list[str] = Field(..., min_length=3, max_length=3, description="Exactly 3 UI tags (raw, may be long)")

    @field_validator("tags")
    @classmethod
    def validate_tags_len(cls, v: list[str]) -> list[str]:
        # Only enforce count here so parsing doesn't fail due to length.
        if len(v) != 3:
            raise ValueError("tags must have exactly 3 items")
        # Also ensure non-empty
        for t in v:
            if not (t or "").strip():
                raise ValueError("empty tag")
        return v


class ReviewTagBatch(BaseModel):
    items: list[ReviewTagItem] = Field(..., description="Tag outputs for the input reviews")
