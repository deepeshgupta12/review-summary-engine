from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from rich import print

from review_summarizer.io import read_reviews_csv


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def _index_project_summaries(summary_jsonl: Path) -> dict[str, dict[str, Any]]:
    records = _read_jsonl(summary_jsonl)
    idx: dict[str, dict[str, Any]] = {}
    for r in records:
        pid = str(r.get("project_id", "")).strip()
        if pid:
            idx[pid] = r
    return idx


def export_project_packs(
    *,
    reviews_csv: str,
    out_dir: str,
    project_summaries_jsonl: str = "data/out/project_summaries.jsonl",
    review_tags_jsonl: str = "data/out/review_tags.jsonl",
    only_project_id: str | None = None,
    limit_projects: int | None = None,
) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Load sources
    df_reviews, cols = read_reviews_csv(reviews_csv)

    summaries_idx = _index_project_summaries(Path(project_summaries_jsonl))
    tags_records = _read_jsonl(Path(review_tags_jsonl))

    if not tags_records:
        raise ValueError(f"No tag records found at: {review_tags_jsonl}")

    df_tags = pd.DataFrame(tags_records)
    if "review_uid" not in df_tags.columns:
        raise ValueError("review_tags.jsonl must contain review_uid")

    # review_uid is not present in original CSV, so we join using project_id + created_on + rating + description hash
    # However, Step 2 already wrote project_id/project_name/rating/created_on in tag output.
    # We'll join on those + review text exact match isn't present in tags output to avoid leakage. So instead we attach tags by (project_id, created_on, rating) + stable ordering.
    # Practical approach for UI packs: provide tags dataset separately; and include review text from original CSV, mapped by project.
    # We'll build per-project tagged list by taking all reviews for project and (if tags exist for that project) attach tags in same order for that project by created_on desc.

    # Prepare review dataset per project with sorting
    if "CreatedOn" in df_reviews.columns:
        df_reviews["_created_on"] = pd.to_datetime(df_reviews["CreatedOn"], errors="coerce", utc=True)
    else:
        df_reviews["_created_on"] = pd.NaT

    df_reviews["_rating"] = pd.to_numeric(df_reviews["Rating"], errors="coerce") if "Rating" in df_reviews.columns else pd.NA

    # Prepare tags per project with same sorting
    if "created_on" in df_tags.columns:
        df_tags["_created_on"] = pd.to_datetime(df_tags["created_on"], errors="coerce", utc=True)
    else:
        df_tags["_created_on"] = pd.NaT
    df_tags["_rating"] = pd.to_numeric(df_tags["rating"], errors="coerce") if "rating" in df_tags.columns else pd.NA

    # Filter projects
    grp = (
        df_reviews.groupby([cols.project_id, cols.project_name], dropna=False)
        .size()
        .reset_index(name="review_count")
        .sort_values("review_count", ascending=False)
    )

    if only_project_id:
        grp = grp[grp[cols.project_id].astype(str) == str(only_project_id)]
    if limit_projects is not None:
        grp = grp.head(limit_projects)

    tags_by_project_dir = out_path / "review_tags_by_project"
    pack_dir = out_path / "project_pack"
    tags_by_project_dir.mkdir(parents=True, exist_ok=True)
    pack_dir.mkdir(parents=True, exist_ok=True)

    index_rows: list[dict[str, Any]] = []

    for _, r in grp.iterrows():
        pid = str(r[cols.project_id])
        pname = str(r[cols.project_name])

        # Reviews for this project
        pr = df_reviews[df_reviews[cols.project_id].astype(str) == pid].copy()
        pr = pr.sort_values("_created_on", ascending=False, na_position="last")

        # Tags for this project
        pt = df_tags[df_tags["project_id"].astype(str) == pid].copy()
        pt = pt.sort_values("_created_on", ascending=False, na_position="last")

        # Export tags csv per project (direct)
        tags_csv_path = tags_by_project_dir / f"{pid}.csv"
        pt_out = pt[["review_uid", "project_id", "project_name", "rating", "created_on", "tag_1", "tag_2", "tag_3"]].copy()
        pt_out.to_csv(tags_csv_path, index=False, encoding="utf-8")

        # Build tagged reviews list for pack (best-effort alignment)
        # We'll attach tags to reviews by taking the same sorted order count min(len(reviews), len(tags)).
        tagged_reviews: list[dict[str, Any]] = []
        pr_reviews = pr.to_dict(orient="records")
        pt_tags = pt.to_dict(orient="records")

        n = min(len(pr_reviews), len(pt_tags))
        for i in range(n):
            rv = pr_reviews[i]
            tg = pt_tags[i]
            tagged_reviews.append({
                "project_id": pid,
                "project_name": pname,
                "rating": float(rv.get("Rating")) if pd.notna(rv.get("Rating")) else None,
                "created_on": str(rv.get("CreatedOn")) if rv.get("CreatedOn") is not None else None,
                "review_text": str(rv.get("Description") or "").strip(),
                "tags": [tg.get("tag_1"), tg.get("tag_2"), tg.get("tag_3")],
            })

        pack = {
            "project_id": pid,
            "project_name": pname,
            "project_summary": summaries_idx.get(pid),
            "tagged_reviews": tagged_reviews,
            "counts": {
                "total_reviews_in_csv": int(len(pr)),
                "tag_rows_available": int(len(pt)),
                "tagged_reviews_in_pack": int(len(tagged_reviews)),
                "has_project_summary": bool(pid in summaries_idx),
            },
        }

        pack_path = pack_dir / f"{pid}.json"
        with pack_path.open("w", encoding="utf-8") as f:
            json.dump(pack, f, ensure_ascii=False, indent=2)

        index_rows.append({
            "project_id": pid,
            "project_name": pname,
            "total_reviews_in_csv": int(len(pr)),
            "tag_rows_available": int(len(pt)),
            "tagged_reviews_in_pack": int(len(tagged_reviews)),
            "has_project_summary": bool(pid in summaries_idx),
            "tags_csv_path": str(tags_csv_path),
            "pack_json_path": str(pack_path),
        })

    index_csv = out_path / "project_pack_index.csv"
    pd.DataFrame(index_rows).to_csv(index_csv, index=False, encoding="utf-8")

    print("[bold green]Export done.[/bold green]")
    print(f"- {index_csv}")
    print(f"- {tags_by_project_dir}/<ProjectId>.csv")
    print(f"- {pack_dir}/<ProjectId>.json")
