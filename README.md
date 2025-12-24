# Review Summary & Tag Generation Engine 🤖🏢

A local, step-by-step **AI engine** to ingest a CSV of real-estate project reviews and generate:

1) **Project-wise AI summaries** (one consolidated summary per project)  
2) **Per-review 3 UI tags** that capture the review’s **persona + key USPs**  
3) **Resume + batch processing** so you can process large datasets (e.g., ~12k reviews) safely and incrementally

Built for a local Mac setup (VS Code + Terminal) and uses the **OpenAI API**.

---

## 🎯 Objectives

This engine helps Square Yards–style product surfaces and internal tools to:

- **Summarize** what reviewers collectively say about each project (review-grounded, neutral).
- **Tag** each review with exactly **3 short UI tags** that capture:
  - Persona / intent (who is speaking / what they care about)
  - Primary USP mentioned
  - Secondary USP or experience cue
- **Scale safely** using resume + batching: long runs don’t restart from scratch.

---

## 📌 Scope

### ✅ In scope
- Input: a **single CSV** containing project review rows (ProjectId + ProjectName + Description required).
- Project-wise summarization:
  - Chunked summarization for large review volumes
  - Final consolidated project summary using chunk summaries
- Per-review tag generation:
  - Exactly **3 tags per review**
  - Tags are post-processed to satisfy UI constraints (e.g., max length)
- Outputs written locally:
  - JSONL for append/resume safety
  - CSV for easy viewing/import

### 🚫 Out of scope (for now)
- Hosting as a production API service (we can add FastAPI later)
- UI rendering layer
- Multi-lingual tagging/summaries
- Fully deterministic review ↔ tag joining in “project packs” if the CSV doesn’t contain a stable ReviewId
  - (Roadmap item: add/standardize a stable review identifier / hash-based join)

---

## 🧱 Repo Structure

```
review-summary-engine/
  src/
    review_summarizer/
      config.py
      io.py
      openai_client.py
      project_summary.py
      resume.py
      review_tags.py
      review_uid.py
      schemas.py
      tag_schemas.py
      tokenizer.py
  scripts/
    inspect_csv.py
    generate_project_summaries.py
    generate_review_tags.py
    run_project_summary_batches.sh
    run_review_tag_batches.sh
    export_project_packs.py
  data/
    in/
      reviews.csv               # your input CSV (example name)
    out/
      ...                       # generated outputs
  .env.example
  pyproject.toml
  requirements.txt
  README.md
```

---

## 🗂️ Input CSV Requirements

Your CSV should include (minimum):

- `ProjectId`
- `ProjectName`
- `Description` (review text)

Optional (recommended):
- `Rating`
- `UserId`
- `CreatedOn`

Example columns seen in your dataset:
`ProjectId, ProjectName, UserId, Description, Rating, ReviewerName, Source, CreatedOn`

---

## 🔐 OpenAI Setup

### 1) Create `.env`
Copy template:
```bash
cp .env.example .env
```

Set your key inside `.env`:
```bash
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-4.1-mini
```

Key settings (optional):
```bash
OPENAI_MAX_REVIEWS_PER_PROJECT=400
OPENAI_CHUNK_TOKENS=12000
OPENAI_MAX_REVIEW_CHARS=1200

OPENAI_TAG_BATCH_TOKENS=8000
OPENAI_TAG_BATCH_MAX_REVIEWS=25
OPENAI_TAG_TEMPERATURE=0.1
```

---

## 🧪 Installation (Mac / Terminal)

From repo root:
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

# Editable install (so imports work cleanly)
python -m pip install -e .
```

---

## 🔍 Step 0: Inspect the CSV

Put your file here:
```bash
cp "/path/to/writeturn project reviews.csv" data/in/reviews.csv
```

Inspect:
```bash
python scripts/inspect_csv.py --csv "data/in/reviews.csv"
```

This prints:
- row count
- column list
- empty review count
- top projects by review volume
- a small row sample

---

## 🧠 Step 1: Generate Project-wise AI Summaries

### Run a cost-safe test (top 2 projects)
```bash
python scripts/generate_project_summaries.py \
  --csv "data/in/reviews.csv" \
  --out "data/out" \
  --limit-projects 2
```

### Resume + batch processing (recommended for big datasets)
Process 10 projects at a time:
```bash
python scripts/generate_project_summaries.py \
  --csv "data/in/reviews.csv" \
  --out "data/out" \
  --batch-size 10 \
  --resume
```

Or run batches continuously until done:
```bash
./scripts/run_project_summary_batches.sh "data/in/reviews.csv" "data/out" 25 0
```

---

## 🏷️ Step 2: Generate 3 Tags Per Review

### Run a small test (first 50 rows)
```bash
python scripts/generate_review_tags.py \
  --csv "data/in/reviews.csv" \
  --out "data/out" \
  --limit-rows 50 \
  --no-resume
```

### Run batches with resume (recommended)
Process 500 reviews per batch:
```bash
python scripts/generate_review_tags.py \
  --csv "data/in/reviews.csv" \
  --out "data/out" \
  --batch-size 500 \
  --resume
```

Or run continuously until done:
```bash
./scripts/run_review_tag_batches.sh "data/in/reviews.csv" "data/out" 500 0
```

⚠️ Note: If you rerun with `--limit-rows 50` again, resume will skip those same 50 rows and show “Nothing to process”. Use `--batch-size` or remove `--limit-rows` for full runs.

---

## 📦 Step 2.4: Export Project Packs (Summary + Tagged Reviews)

Export top 3 projects:
```bash
python scripts/export_project_packs.py \
  --csv "data/in/reviews.csv" \
  --out "data/out" \
  --limit-projects 3
```

This produces a UI-friendly bundle per project.

---

## 🧾 Outputs

### Step 1 Outputs (Project summaries)
- `data/out/project_summaries.jsonl`  
  One JSON object per project, append/resume-friendly
- `data/out/project_summaries.csv`  
  Flattened summary for Excel/Sheets
- `data/out/project_chunk_summaries.jsonl`  
  Chunk-level summaries (debuggable)

**Sample JSONL record structure (high-level):**
```json
{
  "project_id": "123",
  "project_name": "ABC Heights",
  "headline": "Good connectivity with strong amenities",
  "overall_summary": "...",
  "top_highlights": ["...", "..."],
  "watchouts_or_gaps": ["...", "..."],
  "best_for": ["...", "..."],
  "not_ideal_for": ["..."],
  "evidence_notes": ["...", "..."]
}
```

### Step 2 Outputs (Review tags)
- `data/out/review_tags.jsonl`  
  One JSON per review_uid with `tag_1..tag_3`
- `data/out/review_tags.csv`  
  CSV version of the same

**Sample CSV columns:**
- `review_uid`
- `project_id`, `project_name`
- `rating`, `created_on`
- `tag_1`, `tag_2`, `tag_3`

### Step 2.4 Outputs (Project packs)
- `data/out/project_pack_index.csv`
- `data/out/project_pack/<ProjectId>.json`
- `data/out/review_tags_by_project/<ProjectId>.csv`

---

## 🧷 Code Snippets

### 1) Project summary generation (CLI)
```bash
python scripts/generate_project_summaries.py --csv "data/in/reviews.csv" --out "data/out" --batch-size 10 --resume
```

### 2) Review tag generation (CLI)
```bash
python scripts/generate_review_tags.py --csv "data/in/reviews.csv" --out "data/out" --batch-size 500 --resume
```

### 3) Run tag batches until completion
```bash
./scripts/run_review_tag_batches.sh "data/in/reviews.csv" "data/out" 500 0
```

---

## 🛠️ Troubleshooting

### “does not appear to be a Python project” (no setup.py/pyproject.toml)
This repo uses `pyproject.toml`. Ensure it exists and run:
```bash
python -m pip install -e .
```

### Pydantic ValidationError on tags (tag too long)
Fixed by:
- relaxing schema to parse “raw tags”
- post-processing tags to satisfy UI constraints (<= 28 chars)
- strict single-review regeneration fallback if needed

### Rate limits / throttling
Add sleeps between calls:
```bash
python scripts/generate_review_tags.py --csv "data/in/reviews.csv" --out "data/out" --batch-size 500 --resume --sleep-s 0.2
```

---

## 🗺️ Roadmap

- ✅ Deterministic review ↔ tag joining using a stable review id / hash strategy end-to-end
- ✅ FastAPI service wrapper (`/summaries`, `/tags`, `/project-pack/<id>`)
- ✅ Optional caching layer (e.g., SQLite / DuckDB) to speed incremental runs
- ✅ Automated eval checks (tag validity, summary consistency, profanity filtering)
