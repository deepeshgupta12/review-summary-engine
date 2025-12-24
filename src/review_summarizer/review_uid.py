from __future__ import annotations

import hashlib
from typing import Any


def make_review_uid(*, project_id: str, user_id: Any, created_on: Any, description: str) -> str:
    """
    Creates a stable UID even if CSV doesn't have ReviewId.
    Uses SHA1 of key fields (enough for dedupe + resume).
    """
    raw = f"{project_id}|{str(user_id or '').strip()}|{str(created_on or '').strip()}|{str(description or '').strip()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
