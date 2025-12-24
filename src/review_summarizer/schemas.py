from __future__ import annotations

from pydantic import BaseModel, Field


class ChunkSummary(BaseModel):
    chunk_id: int = Field(..., description="1-based chunk index within the project")
    chunk_summary: str = Field(..., description="Concise summary of what reviewers said in this chunk")
    common_positives: list[str] = Field(..., description="Most common positives mentioned in this chunk")
    watchouts_or_gaps: list[str] = Field(..., description="Neutral watch-outs / gaps / constraints mentioned")


class ProjectSummary(BaseModel):
    project_id: str
    project_name: str

    headline: str = Field(..., description="Short, crisp headline (<= 90 chars)")
    overall_summary: str = Field(..., description="150-250 words, neutral, factual, review-grounded")

    top_highlights: list[str] = Field(..., min_length=4, max_length=7, description="Key USPs across reviews")
    watchouts_or_gaps: list[str] = Field(..., min_length=2, max_length=6, description="Neutral risks / limitations")

    best_for: list[str] = Field(..., min_length=2, max_length=5, description="Who this project suits best, inferred from reviews")
    not_ideal_for: list[str] = Field(..., min_length=1, max_length=4, description="Who may not like it, inferred from reviews")

    evidence_notes: list[str] = Field(..., min_length=3, max_length=8, description="Short evidence statements grounded in reviews")
