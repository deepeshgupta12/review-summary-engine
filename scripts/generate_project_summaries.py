from __future__ import annotations

import argparse
from review_summarizer.project_summary import generate_project_summaries


def main() -> None:
    p = argparse.ArgumentParser(description="Generate project-wise AI summaries from reviews CSV.")
    p.add_argument("--csv", required=True, help="Path to reviews CSV (ProjectId, ProjectName, Description required)")
    p.add_argument("--out", default="data/out", help="Output directory")

    p.add_argument("--limit-projects", type=int, default=None, help="Only consider top N projects by review volume")
    p.add_argument("--only-project-id", default=None, help="Process a single ProjectId")

    p.add_argument("--resume", action="store_true", help="Resume: skip projects already present in project_summaries.jsonl")
    p.add_argument("--no-resume", dest="resume", action="store_false", help="Clean run: delete outputs first")
    p.set_defaults(resume=True)

    p.add_argument("--batch-size", type=int, default=None, help="Process only N projects and exit (for batching)")
    p.add_argument("--sleep-s", type=float, default=0.0, help="Sleep seconds between API calls (optional)")
    args = p.parse_args()

    generate_project_summaries(
        csv_path=args.csv,
        out_dir=args.out,
        limit_projects=args.limit_projects,
        only_project_id=args.only_project_id,
        resume=args.resume,
        batch_size=args.batch_size,
        sleep_s=args.sleep_s,
    )


if __name__ == "__main__":
    main()
