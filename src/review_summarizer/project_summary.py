from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from rich import print
from rich.progress import track

from review_summarizer.config import Settings
from review_summarizer.io import read_reviews_csv
from review_summarizer.openai_client import build_client, responses_parse
from review_summarizer.schemas import ChunkSummary, ProjectSummary
from review_summarizer.tokenizer import chunk_texts


SYSTEM_CHUNK = """You summarize user reviews for a real-estate project in India.
Rules:
- Be neutral, factual, and review-grounded.
- Do not invent facts. If something isn't in reviews, don't claim it.
- Convert negatives into neutral 'watch-outs' phrasing.
- Avoid marketing fluff.
Return structured output exactly matching the schema.
"""

SYSTEM_FINAL = """You produce a single project-level summary from chunk-level summaries of user reviews.
Rules:
- Consolidate repeated points.
- Be neutral, factual, and review-grounded.
- Do not invent facts. If something isn't supported by chunk summaries, don't claim it.
- Convert negatives into neutral 'watch-outs' phrasing.
Return structured output exactly matching the schema.
"""


def _prepare_project_reviews(df: pd.DataFrame, *, project_id: str, project_name: str, max_reviews: int, max_review_chars: int) -> list[str]:
    """
    Returns review snippets as strings (each snippet is one review).
    """
    # Prefer most recent reviews if CreatedOn exists and is parseable.
    if "CreatedOn" in df.columns:
        dt = pd.to_datetime(df["CreatedOn"], errors="coerce", utc=True)
        df = df.assign(_created_on=dt).sort_values("_created_on", ascending=False, na_position="last")

    df = df.head(max_reviews)

    snippets: list[str] = []
    for _, r in df.iterrows():
        rating = r["Rating"] if "Rating" in df.columns else None
        text = str(r["Description"]) if "Description" in df.columns else ""
        text = " ".join(text.split()).strip()

        if not text:
            continue

        if len(text) > max_review_chars:
            text = text[: max_review_chars].rstrip() + "…"

        if pd.notna(rating):
            snippets.append(f"- (Rating: {rating}) {text}")
        else:
            snippets.append(f"- {text}")

    return snippets


def generate_project_summaries(
    *,
    csv_path: str,
    out_dir: str,
    limit_projects: int | None = None,
    only_project_id: str | None = None,
) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    settings = Settings.from_env(out_dir=out_dir)
    client = build_client(settings.openai_api_key)

    df, cols = read_reviews_csv(csv_path)

    # Group projects
    grp = (
        df.groupby([cols.project_id, cols.project_name], dropna=False)
        .size()
        .reset_index(name="review_count")
        .sort_values("review_count", ascending=False)
    )

    if only_project_id:
        grp = grp[grp[cols.project_id].astype(str) == str(only_project_id)]

    if limit_projects is not None:
        grp = grp.head(limit_projects)

    jsonl_file = out_path / "project_summaries.jsonl"
    csv_file = out_path / "project_summaries.csv"
    chunks_file = out_path / "project_chunk_summaries.jsonl"

    # Clear old outputs for clean runs
    for fp in (jsonl_file, csv_file, chunks_file):
        if fp.exists():
            fp.unlink()

    all_rows_for_csv: list[dict[str, Any]] = []

    for _, row in track(grp.iterrows(), total=len(grp), description="Summarizing projects"):
        project_id = str(row[cols.project_id])
        project_name = str(row[cols.project_name])

        project_df = df[(df[cols.project_id].astype(str) == project_id) & (df[cols.project_name].astype(str) == project_name)]
        snippets = _prepare_project_reviews(
            project_df,
            project_id=project_id,
            project_name=project_name,
            max_reviews=settings.max_reviews_per_project,
            max_review_chars=settings.max_review_chars,
        )

        if not snippets:
            continue

        # Chunk reviews
        chunks = chunk_texts(snippets, max_tokens=settings.chunk_tokens)

        chunk_summaries: list[ChunkSummary] = []
        for ch in chunks:
            user_prompt = f"""Project: {project_name} (ProjectId: {project_id})
You will be given a chunk of user review snippets. Summarize ONLY what is present.

REVIEW_SNIPPETS_CHUNK:
{ch.text}
"""
            resp = responses_parse(
                client=client,
                model=settings.model,
                input_messages=[
                    {"role": "system", "content": SYSTEM_CHUNK},
                    {"role": "user", "content": user_prompt},
                ],
                text_format=ChunkSummary,
                temperature=settings.temperature,
            )

            parsed: ChunkSummary = resp.output_parsed
            # enforce chunk_id alignment
            parsed.chunk_id = ch.chunk_id  # type: ignore[attr-defined]
            chunk_summaries.append(parsed)

            with chunks_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "project_id": project_id,
                    "project_name": project_name,
                    "chunk_id": ch.chunk_id,
                    "chunk_token_estimate": ch.token_estimate,
                    "chunk_summary": parsed.model_dump(),
                }, ensure_ascii=False) + "\n")

        # Final aggregation
        chunk_payload = "\n\n".join(
            [f"CHUNK {c.chunk_id}:\nSummary: {c.chunk_summary}\nPositives: {c.common_positives}\nWatchouts: {c.watchouts_or_gaps}"
             for c in chunk_summaries]
        )

        final_user = f"""Project: {project_name} (ProjectId: {project_id})

Below are chunk-level summaries extracted from user reviews. Generate ONE consolidated project-level summary.

CHUNK_SUMMARIES:
{chunk_payload}
"""

        resp_final = responses_parse(
            client=client,
            model=settings.model,
            input_messages=[
                {"role": "system", "content": SYSTEM_FINAL},
                {"role": "user", "content": final_user},
            ],
            text_format=ProjectSummary,
            temperature=settings.temperature,
        )

        final_parsed: ProjectSummary = resp_final.output_parsed
        # ensure ids are set correctly even if model echoes wrong
        final_parsed.project_id = project_id  # type: ignore[attr-defined]
        final_parsed.project_name = project_name  # type: ignore[attr-defined]

        record = final_parsed.model_dump()

        with jsonl_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        # flatten for CSV
        flat = {
            "project_id": record["project_id"],
            "project_name": record["project_name"],
            "headline": record["headline"],
            "overall_summary": record["overall_summary"],
            "top_highlights": " | ".join(record["top_highlights"]),
            "watchouts_or_gaps": " | ".join(record["watchouts_or_gaps"]),
            "best_for": " | ".join(record["best_for"]),
            "not_ideal_for": " | ".join(record["not_ideal_for"]),
            "evidence_notes": " | ".join(record["evidence_notes"]),
        }
        all_rows_for_csv.append(flat)

    if all_rows_for_csv:
        pd.DataFrame(all_rows_for_csv).to_csv(csv_file, index=False, encoding="utf-8")

    print(f"[bold green]Done.[/bold green] Outputs:")
    print(f"- {jsonl_file}")
    print(f"- {csv_file}")
    print(f"- {chunks_file}")
