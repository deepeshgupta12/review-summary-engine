from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from rich import print
from rich.table import Table

from review_summarizer.io import read_reviews_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect reviews CSV (project-wise).")
    parser.add_argument("--csv", required=True, help="Path to reviews CSV")
    parser.add_argument("--top", type=int, default=15, help="Top projects by review count to display")
    args = parser.parse_args()

    df, cols = read_reviews_csv(args.csv)

    print(f"[bold]Rows:[/bold] {len(df):,}")
    print(f"[bold]Columns:[/bold] {list(df.columns)}")

    # Basic sanity checks
    empty_reviews = (df[cols.review_text].isna() | (df[cols.review_text].str.len() == 0)).sum()
    print(f"[bold]Empty review texts:[/bold] {empty_reviews:,}")

    # Project grouping
    grp = df.groupby([cols.project_id, cols.project_name], dropna=False).size().reset_index(name="review_count")
    print(f"[bold]Unique projects:[/bold] {len(grp):,}")

    # Top projects
    top = grp.sort_values("review_count", ascending=False).head(args.top)

    table = Table(title=f"Top {len(top)} projects by review count")
    table.add_column("ProjectId", justify="left")
    table.add_column("ProjectName", justify="left")
    table.add_column("ReviewCount", justify="right")
    for _, r in top.iterrows():
        table.add_row(str(r[cols.project_id]), str(r[cols.project_name]), f"{int(r['review_count']):,}")
    print(table)

    # Rating distribution (if available)
    if cols.rating in df.columns:
        rating_counts = df[cols.rating].dropna().round(0).value_counts().sort_index()
        rt = Table(title="Rating distribution (rounded)")
        rt.add_column("Rating", justify="right")
        rt.add_column("Count", justify="right")
        for rating, cnt in rating_counts.items():
            rt.add_row(str(int(rating)), f"{int(cnt):,}")
        print(rt)

    # Show a small sample
    sample = df[[cols.project_id, cols.project_name, cols.review_text] + ([cols.rating] if cols.rating in df.columns else [])].head(5)
    print("[bold]Sample rows:[/bold]")
    print(sample.to_string(index=False))


if __name__ == "__main__":
    main()
