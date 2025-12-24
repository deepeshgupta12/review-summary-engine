from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from rich import print
from rich.progress import track

from review_summarizer.config import Settings
from review_summarizer.io import read_reviews_csv
from review_summarizer.openai_client import build_client, responses_parse
from review_summarizer.review_uid import make_review_uid
from review_summarizer.tag_schemas import ReviewTagBatch
from review_summarizer.tokenizer import count_tokens


SYSTEM_TAGS = """You generate exactly 3 short UI tags for each user review of a real-estate project.

Hard rules:
- Output EXACTLY 3 tags per review.
- Each tag is 2–4 words.
- Title Case.
- No emojis.
- No personal names.
- Tags must be grounded ONLY in what the review text says.
- Tags should capture: (1) Persona/Intent, (2) Primary USP, (3) Secondary USP or Experience.
- Avoid harsh negativity; if needed, phrase as neutral ("Needs Better Maintenance", "Traffic Consideration").

Important:
- Keep tags short; aim <= 28 characters.
Return structured output exactly matching the schema.
"""


def _load_processed_review_uids(jsonl_path: Path) -> set[str]:
    if not jsonl_path.exists():
        return set()
    out: set[str] = set()
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                uid = str(obj.get("review_uid", "")).strip()
                if uid:
                    out.add(uid)
            except Exception:
                continue
    return out


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
    pd.DataFrame(records).to_csv(csv_file, index=False, encoding="utf-8")


def _title_case_preserve_acronyms(s: str) -> str:
    words = [w for w in re.split(r"\s+", s.strip()) if w]
    out = []
    for w in words:
        # keep acronyms like "UPI", "RERA" as-is
        if len(w) <= 5 and w.isupper():
            out.append(w)
        elif w.islower():
            out.append(w[:1].upper() + w[1:])
        else:
            out.append(w[:1].upper() + w[1:])
    return " ".join(out)


def _clean_tag(tag: str) -> str:
    tag = (tag or "").strip().strip('"').strip("'")
    tag = re.sub(r"[^\w\s&/-]", "", tag)  # remove weird punctuation, keep &, /, -
    tag = re.sub(r"\s+", " ", tag).strip()
    tag = _title_case_preserve_acronyms(tag)
    return tag


def _shorten_tag(tag: str, max_len: int = 28) -> str:
    """
    Enforce UI max length without breaking too much meaning.
    - Try removing filler words
    - Then truncate cleanly
    """
    tag = _clean_tag(tag)
    if len(tag) <= max_len:
        return tag

    filler = {"Very", "Highly", "Really", "Quite", "Mostly", "Generally", "Appreciating", "Noting", "Finding"}
    parts = [p for p in tag.split() if p not in filler]
    tag2 = " ".join(parts).strip()
    if tag2 and len(tag2) <= max_len:
        return tag2

    # Truncate to max_len at word boundary
    if len(tag2) > max_len:
        tag2 = tag2[:max_len].rstrip()
    if len(tag2) < 6:  # too short to be meaningful after truncation
        tag2 = tag[:max_len].rstrip()

    return tag2


def _pack_reviews_for_batch(reviews: List[Dict[str, Any]], max_tokens: int) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    buf: List[Dict[str, Any]] = []
    buf_tokens = 0

    for r in reviews:
        payload = json.dumps(r, ensure_ascii=False)
        t = count_tokens(payload)

        if buf and buf_tokens + t > max_tokens:
            batches.append(buf)
            buf = []
            buf_tokens = 0

        buf.append(r)
        buf_tokens += t

    if buf:
        batches.append(buf)

    return batches


def _regen_single_review_tags(*, client, model: str, temperature: float, review_obj: Dict[str, Any]) -> List[str]:
    """
    If post-processing still violates constraints, regenerate strictly for a single review.
    """
    strict_system = SYSTEM_TAGS + "\nSTRICT: Each tag MUST be <= 28 characters. No exceptions."
    one_prompt = f"""Generate tags for this single review.

InputReviewJSON:
{json.dumps(review_obj, ensure_ascii=False)}

Return:
- items: array with exactly 1 object for this review_uid.
"""
    resp = responses_parse(
        client=client,
        model=model,
        input_messages=[
            {"role": "system", "content": strict_system},
            {"role": "user", "content": one_prompt},
        ],
        text_format=ReviewTagBatch,
        temperature=temperature,
    )
    parsed: ReviewTagBatch = resp.output_parsed
    raw = parsed.items[0].tags
    fixed = [_shorten_tag(t, 28) for t in raw]
    # Final hard clamp
    fixed = [t[:28].rstrip() for t in fixed]
    return fixed


def generate_review_tags(
    *,
    csv_path: str,
    out_dir: str,
    resume: bool = True,
    limit_rows: int | None = None,
    only_project_id: str | None = None,
    batch_size: int | None = None,
    sleep_s: float = 0.0,
) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    settings = Settings.from_env(out_dir=out_dir)
    client = build_client(settings.openai_api_key)

    df, cols = read_reviews_csv(csv_path)

    if only_project_id:
        df = df[df[cols.project_id].astype(str) == str(only_project_id)]

    if limit_rows is not None:
        df = df.head(limit_rows)

    jsonl_file = out_path / "review_tags.jsonl"
    csv_file = out_path / "review_tags.csv"

    if not resume:
        for fp in (jsonl_file, csv_file):
            if fp.exists():
                fp.unlink()

    df["_review_uid"] = df.apply(
        lambda r: make_review_uid(
            project_id=str(r[cols.project_id]),
            user_id=r["UserId"] if "UserId" in df.columns else None,
            created_on=r["CreatedOn"] if "CreatedOn" in df.columns else None,
            description=r[cols.review_text],
        ),
        axis=1,
    )

    processed = _load_processed_review_uids(jsonl_file) if resume else set()
    if resume and processed:
        before = len(df)
        df = df[~df["_review_uid"].isin(processed)]
        after = len(df)
        print(f"[bold]Resume:[/bold] skipping {before - after} already processed reviews.")

    if len(df) == 0:
        print("[yellow]Nothing to process (all done or filtered out).[/yellow]")
        return

    payloads: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        text = str(r[cols.review_text]).strip()
        if not text:
            continue
        payloads.append({
            "review_uid": str(r["_review_uid"]),
            "project_id": str(r[cols.project_id]),
            "project_name": str(r[cols.project_name]),
            "rating": float(r["Rating"]) if ("Rating" in df.columns and pd.notna(r["Rating"])) else None,
            "created_on": str(r["CreatedOn"]) if "CreatedOn" in df.columns else None,
            "review_text": text,
        })

    if batch_size is not None:
        payloads = payloads[:batch_size]

    if not payloads:
        print("[yellow]No non-empty reviews to process.[/yellow]")
        return

    packed = _pack_reviews_for_batch(payloads, max_tokens=settings.tag_batch_tokens)

    # Also cap count per batch
    final_batches: List[List[Dict[str, Any]]] = []
    for b in packed:
        for i in range(0, len(b), settings.tag_batch_max_reviews):
            final_batches.append(b[i:i + settings.tag_batch_max_reviews])

    written = 0

    for b in track(final_batches, total=len(final_batches), description="Generating tags"):
        user_prompt = f"""Generate tags for each review item below.

InputReviewsJSON:
{json.dumps(b, ensure_ascii=False)}

Return:
- items: array of objects, each with review_uid and tags (exactly 3 tags).
- One output per input review_uid.
"""
        resp = responses_parse(
            client=client,
            model=settings.model,
            input_messages=[
                {"role": "system", "content": SYSTEM_TAGS},
                {"role": "user", "content": user_prompt},
            ],
            text_format=ReviewTagBatch,
            temperature=settings.tag_temperature,
        )

        parsed: ReviewTagBatch = resp.output_parsed
        out_map: Dict[str, List[str]] = {}

        for it in parsed.items:
            raw_tags = it.tags
            fixed = [_shorten_tag(t, 28) for t in raw_tags]
            fixed = [t[:28].rstrip() for t in fixed]  # final clamp
            out_map[it.review_uid] = fixed

        # Ensure every input uid got output; regenerate missing individually
        missing = [x for x in b if x["review_uid"] not in out_map]
        if missing:
            print(f"[yellow]Batch missing {len(missing)} items. Retrying individually.[/yellow]")
            for one in missing:
                fixed = _regen_single_review_tags(
                    client=client,
                    model=settings.model,
                    temperature=settings.tag_temperature,
                    review_obj=one,
                )
                out_map[one["review_uid"]] = fixed
                if sleep_s > 0:
                    time.sleep(sleep_s)

        # If any tags still violate length or emptiness, regenerate individually (rare)
        for one in b:
            uid = one["review_uid"]
            tags = out_map.get(uid)
            if not tags or len(tags) != 3 or any((not t.strip()) for t in tags) or any(len(t) > 28 for t in tags):
                out_map[uid] = _regen_single_review_tags(
                    client=client,
                    model=settings.model,
                    temperature=settings.tag_temperature,
                    review_obj=one,
                )

        with jsonl_file.open("a", encoding="utf-8") as f:
            for one in b:
                uid = one["review_uid"]
                tags = out_map.get(uid)
                if not tags:
                    continue
                rec = {
                    "review_uid": uid,
                    "project_id": one["project_id"],
                    "project_name": one["project_name"],
                    "rating": one.get("rating"),
                    "created_on": one.get("created_on"),
                    "tag_1": tags[0],
                    "tag_2": tags[1],
                    "tag_3": tags[2],
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1

        if sleep_s > 0:
            time.sleep(sleep_s)

    _rebuild_csv_from_jsonl(jsonl_file, csv_file)

    print(f"[bold green]Done.[/bold green] Wrote {written} review tag rows.")
    print("[bold]Outputs:[/bold]")
    print(f"- {jsonl_file}")
    print(f"- {csv_file}")
