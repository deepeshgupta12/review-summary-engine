from __future__ import annotations

import argparse
from review_summarizer.project_summary import generate_project_summaries


def main() -> None:
    p = argparse.ArgumentParser(description="Generate project-wise AI summaries from reviews CSV.")
    p.add_argument("--csv", required=True, help="Path to reviews CSV (ProjectId, ProjectName, Description required)")
    p.add_argument("--out", default="data/out", help="Output directory")
    p.add_argument("--limit-projects", type=int, default=None, help="Only process top N projects by review volume")
    p.add_argument("--only-project-id", default=None, help="Process a single ProjectId")
    args = p.parse_args()

    generate_project_summaries(
        csv_path=args.csv,
        out_dir=args.out,
        limit_projects=args.limit_projects,
        only_project_id=args.only_project_id,
    )


if __name__ == "__main__":
    main()
