from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from rich import print
from rich.progress import track

from review_summarizer.config import Settings
from review_summarizer.io import read_reviews_csv
from review_summarizer.openai_client import build_client, responses_parse
from review_summarizer.resume import load_processed_project_ids  # kept for backwards compat; not used here
from review_summarizer.review_uid import make_review_uid
from review_summarizer.tag_schemas import ReviewTagBatch, ReviewTagItem
from review_summarizer.tokenizer import count_tokens


SYSTEM_TAGS = """You generate exactly 3 short UI tags for each user review of a real-estate project.

Hard rules:
- Output EXACTLY 3 tags per review.
- Each tag is 2–4 words, Title Case, max 28 characters.
- No emojis.
- No personal names.
- Tags must be grounded ONLY in what the review text says.
- Tags should capture: (1) Persona/Intent, (2) Primary USP, (3) Secondary USP or Experience.
- Avoid harsh negativity; if needed, phrase as neutral ("Needs Better Maintenance", "Traffic Consideration").

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


def _sanitize_tag(tag: str) -> str:
    tag = (tag or "").strip().strip('"').strip("'")
    # Title-case without overdoing acronyms
    words = [w for w in tag.split() if w]
    return " ".join([w[:1].upper() + w[1:] if w.islower() else w for w in words])


def _pack_reviews_for_batch(reviews: List[Dict[str, Any]], max_tokens: int) -> List[List[Dict[str, Any]]]:
    """
    Packs review dicts into multiple batches based on token estimate of JSON payload.
    """
    batches: List[List[Dict[str, Any]]] = []
    buf: List[Dict[str, Any]] = []
    buf_tokens = 0

    for r in reviews:
        # token estimate of the review payload
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

    # Optional filter to a project
    if only_project_id:
        df = df[df[cols.project_id].astype(str) == str(only_project_id)]

    # Optional row limit (cost control)
    if limit_rows is not None:
        df = df.head(limit_rows)

    jsonl_file = out_path / "review_tags.jsonl"
    csv_file = out_path / "review_tags.csv"

    if not resume:
        for fp in (jsonl_file, csv_file):
            if fp.exists():
                fp.unlink()

    processed = _load_processed_review_uids(jsonl_file) if resume else set()
    if resume and processed:
        before = len(df)
        # create uids first to filter
        df["_review_uid"] = df.apply(
            lambda r: make_review_uid(
                project_id=str(r[cols.project_id]),
                user_id=r["UserId"] if "UserId" in df.columns else None,
                created_on=r["CreatedOn"] if "CreatedOn" in df.columns else None,
                description=r[cols.review_text],
            ),
            axis=1,
        )
        df = df[~df["_review_uid"].isin(processed)]
        after = len(df)
        print(f"[bold]Resume:[/bold] skipping {before - after} already processed reviews.")
    else:
        df["_review_uid"] = df.apply(
            lambda r: make_review_uid(
                project_id=str(r[cols.project_id]),
                user_id=r["UserId"] if "UserId" in df.columns else None,
                created_on=r["CreatedOn"] if "CreatedOn" in df.columns else None,
                description=r[cols.review_text],
            ),
            axis=1,
        )

    if len(df) == 0:
        print("[yellow]Nothing to process (all done or filtered out).[/yellow]")
        return

    # Prepare review payloads
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

    # Optional batch limit (process N reviews then exit)
    if batch_size is not None:
        payloads = payloads[:batch_size]

    if not payloads:
        print("[yellow]No non-empty reviews to process.[/yellow]")
        return

    # Pack into API batches by token estimate; also enforce max reviews per batch from env
    packed = _pack_reviews_for_batch(payloads, max_tokens=settings.tag_batch_tokens)
    # Further split if a packed batch exceeds max reviews
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

        # Map results by uid
        out_map: Dict[str, ReviewTagItem] = {it.review_uid: it for it in parsed.items}

        # Validate completeness; if missing, retry missing ones individually (strict)
        missing = [x for x in b if x["review_uid"] not in out_map]
        if missing:
            print(f"[yellow]Batch missing {len(missing)} items. Retrying individually.[/yellow]")
            for one in missing:
                one_prompt = f"""Generate tags for this single review.

InputReviewJSON:
{json.dumps(one, ensure_ascii=False)}

Return:
- items: array with exactly 1 object for this review_uid.
"""
                resp_one = responses_parse(
                    client=client,
                    model=settings.model,
                    input_messages=[
                        {"role": "system", "content": SYSTEM_TAGS},
                        {"role": "user", "content": one_prompt},
                    ],
                    text_format=ReviewTagBatch,
                    temperature=settings.tag_temperature,
                )
                parsed_one: ReviewTagBatch = resp_one.output_parsed
                if parsed_one.items:
                    out_map[parsed_one.items[0].review_uid] = parsed_one.items[0]
                if sleep_s > 0:
                    time.sleep(sleep_s)

        # Write outputs line-by-line for resume safety
        with jsonl_file.open("a", encoding="utf-8") as f:
            for inp in b:
                uid = inp["review_uid"]
                if uid not in out_map:
                    continue
                tags = [_sanitize_tag(t) for t in out_map[uid].tags]

                rec = {
                    "review_uid": uid,
                    "project_id": inp["project_id"],
                    "project_name": inp["project_name"],
                    "rating": inp.get("rating"),
                    "created_on": inp.get("created_on"),
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
