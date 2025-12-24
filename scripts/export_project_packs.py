from __future__ import annotations

import argparse
from review_summarizer.exporter import export_project_packs


def main() -> None:
    p = argparse.ArgumentParser(description="Export project packs (summary + tagged reviews).")
    p.add_argument("--csv", required=True, help="Path to reviews CSV")
    p.add_argument("--out", default="data/out", help="Output directory")

    p.add_argument("--only-project-id", default=None, help="Export only one ProjectId")
    p.add_argument("--limit-projects", type=int, default=None, help="Export top N projects by review count")

    p.add_argument("--summaries-jsonl", default="data/out/project_summaries.jsonl", help="Project summaries JSONL path")
    p.add_argument("--tags-jsonl", default="data/out/review_tags.jsonl", help="Review tags JSONL path")
    args = p.parse_args()

    export_project_packs(
        reviews_csv=args.csv,
        out_dir=args.out,
        project_summaries_jsonl=args.summaries_jsonl,
        review_tags_jsonl=args.tags_jsonl,
        only_project_id=args.only_project_id,
        limit_projects=args.limit_projects,
    )


if __name__ == "__main__":
    main()
