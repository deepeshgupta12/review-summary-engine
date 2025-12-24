from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
from rich import print
from rich.progress import track

from review_summarizer.config import Settings
from review_summarizer.io import read_reviews_csv
from review_summarizer.openai_client import build_client, responses_parse
from review_summarizer.resume import load_processed_project_ids
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


def _prepare_project_reviews(
    df: pd.DataFrame,
    *,
    max_reviews: int,
    max_review_chars: int,
) -> list[str]:
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


def _rebuild_csv_from_jsonl(jsonl_file: Path, csv_file: Path) -> None:
    if not jsonl_file.exists():
        return
    records: list[dict[str, Any]] = []
    with jsonl_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                continue

    if not records:
        return

    rows: list[dict[str, Any]] = []
    for r in records:
        rows.append({
            "project_id": r.get("project_id", ""),
            "project_name": r.get("project_name", ""),
            "headline": r.get("headline", ""),
            "overall_summary": r.get("overall_summary", ""),
            "top_highlights": " | ".join(r.get("top_highlights", []) or []),
            "watchouts_or_gaps": " | ".join(r.get("watchouts_or_gaps", []) or []),
            "best_for": " | ".join(r.get("best_for", []) or []),
            "not_ideal_for": " | ".join(r.get("not_ideal_for", []) or []),
            "evidence_notes": " | ".join(r.get("evidence_notes", []) or []),
        })

    pd.DataFrame(rows).to_csv(csv_file, index=False, encoding="utf-8")


def generate_project_summaries(
    *,
    csv_path: str,
    out_dir: str,
    limit_projects: int | None = None,
    only_project_id: str | None = None,
    resume: bool = True,
    batch_size: int | None = None,
    sleep_s: float = 0.0,
) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    settings = Settings.from_env(out_dir=out_dir)
    client = build_client(settings.openai_api_key)

    df, cols = read_reviews_csv(csv_path)

    # Group projects by volume (largest first)
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

    if not resume:
        # Clean run
        for fp in (jsonl_file, csv_file, chunks_file):
            if fp.exists():
                fp.unlink()

    processed_ids = load_processed_project_ids(jsonl_file) if resume else set()

    # Apply resume skip
    if resume and processed_ids:
        before = len(grp)
        grp = grp[~grp[cols.project_id].astype(str).isin(processed_ids)]
        after = len(grp)
        print(f"[bold]Resume:[/bold] skipping {before - after} already processed projects.")

    # Apply batch sizing (process N then exit)
    if batch_size is not None:
        grp = grp.head(batch_size)

    if len(grp) == 0:
        print("[yellow]Nothing to process (all done or filtered out).[/yellow]")
        return

    processed_now = 0

    for _, row in track(grp.iterrows(), total=len(grp), description="Summarizing projects"):
        project_id = str(row[cols.project_id])
        project_name = str(row[cols.project_name])

        project_df = df[
            (df[cols.project_id].astype(str) == project_id)
            & (df[cols.project_name].astype(str) == project_name)
        ]

        snippets = _prepare_project_reviews(
            project_df,
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
            # Keep chunk id consistent with our chunker
            try:
                parsed.chunk_id = ch.chunk_id  # type: ignore[attr-defined]
            except Exception:
                pass

            chunk_summaries.append(parsed)

            with chunks_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "project_id": project_id,
                    "project_name": project_name,
                    "chunk_id": ch.chunk_id,
                    "chunk_token_estimate": ch.token_estimate,
                    "chunk_summary": parsed.model_dump(),
                }, ensure_ascii=False) + "\n")

            if sleep_s > 0:
                time.sleep(sleep_s)

        # Final aggregation
        chunk_payload = "\n\n".join(
            [
                f"CHUNK {c.chunk_id}:\n"
                f"Summary: {c.chunk_summary}\n"
                f"Positives: {c.common_positives}\n"
                f"Watchouts: {c.watchouts_or_gaps}"
                for c in chunk_summaries
            ]
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
        # Ensure IDs are correct
        try:
            final_parsed.project_id = project_id  # type: ignore[attr-defined]
            final_parsed.project_name = project_name  # type: ignore[attr-defined]
        except Exception:
            pass

        record = final_parsed.model_dump()

        with jsonl_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        processed_now += 1

        if sleep_s > 0:
            time.sleep(sleep_s)

    # Rebuild CSV from JSONL at end (dedup-safe & resume-safe)
    _rebuild_csv_from_jsonl(jsonl_file, csv_file)

    print(f"[bold green]Done.[/bold green] Processed in this run: {processed_now}")
    print("[bold]Outputs:[/bold]")
    print(f"- {jsonl_file}")
    print(f"- {csv_file}")
    print(f"- {chunks_file}")
