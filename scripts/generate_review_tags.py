from __future__ import annotations

import argparse
from review_summarizer.review_tags import generate_review_tags


def main() -> None:
    p = argparse.ArgumentParser(description="Generate 3 tags per review from reviews CSV.")
    p.add_argument("--csv", required=True, help="Path to reviews CSV")
    p.add_argument("--out", default="data/out", help="Output directory")

    p.add_argument("--resume", action="store_true", help="Resume: skip review_uids already in review_tags.jsonl")
    p.add_argument("--no-resume", dest="resume", action="store_false", help="Clean run: delete outputs first")
    p.set_defaults(resume=True)

    p.add_argument("--limit-rows", type=int, default=None, help="Process only first N rows (cost control)")
    p.add_argument("--only-project-id", default=None, help="Process reviews for one ProjectId only")
    p.add_argument("--batch-size", type=int, default=None, help="Process only N reviews then exit (for batching)")
    p.add_argument("--sleep-s", type=float, default=0.0, help="Sleep seconds between API calls (optional)")
    args = p.parse_args()

    generate_review_tags(
        csv_path=args.csv,
        out_dir=args.out,
        resume=args.resume,
        limit_rows=args.limit_rows,
        only_project_id=args.only_project_id,
        batch_size=args.batch_size,
        sleep_s=args.sleep_s,
    )


if __name__ == "__main__":
    main()
