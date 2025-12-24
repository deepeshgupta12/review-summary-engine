from __future__ import annotations

import json
from pathlib import Path
from typing import Set


def load_processed_project_ids(jsonl_path: str | Path) -> Set[str]:
    """
    Reads project_summaries.jsonl and returns a set of processed project_ids.
    Safe against partially written lines.
    """
    p = Path(jsonl_path)
    if not p.exists():
        return set()

    processed: Set[str] = set()
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                pid = str(obj.get("project_id", "")).strip()
                if pid:
                    processed.add(pid)
            except Exception:
                # ignore corrupted/partial line
                continue
    return processed
