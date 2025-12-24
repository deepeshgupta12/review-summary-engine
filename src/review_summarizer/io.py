from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class ReviewColumns:
    project_id: str = "ProjectId"
    project_name: str = "ProjectName"
    review_text: str = "Description"
    rating: str = "Rating"
    created_on: str = "CreatedOn"


def read_reviews_csv(
    csv_path: str | Path,
    *,
    columns: ReviewColumns = ReviewColumns(),
) -> Tuple[pd.DataFrame, ReviewColumns]:
    """
    Reads the uploaded reviews CSV robustly.
    - Uses pandas 'python' engine to tolerate messy quoting
    - Skips malformed lines (pandas will warn)
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(
        csv_path,
        engine="python",
        on_bad_lines="warn",
    )

    missing = [c for c in [columns.project_id, columns.project_name, columns.review_text] if c not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns in CSV: "
            + ", ".join(missing)
            + f"\nFound columns: {list(df.columns)}"
        )

    # Normalize types
    df[columns.project_id] = df[columns.project_id].astype(str).str.strip()
    df[columns.project_name] = df[columns.project_name].astype(str).str.strip()

    # Review text cleanup
    df[columns.review_text] = (
        df[columns.review_text]
        .astype(str)
        .fillna("")
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    # Optional rating cleanup
    if columns.rating in df.columns:
        df[columns.rating] = pd.to_numeric(df[columns.rating], errors="coerce")

    return df, columns
