from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence

import tiktoken


def get_encoder():
    # Prefer newest encoding if available; fall back safely.
    for name in ("o200k_base", "cl100k_base"):
        try:
            return tiktoken.get_encoding(name)
        except Exception:
            continue
    return tiktoken.get_encoding("cl100k_base")


ENC = get_encoder()


def count_tokens(text: str) -> int:
    return len(ENC.encode(text or ""))


@dataclass(frozen=True)
class Chunk:
    chunk_id: int
    text: str
    token_estimate: int


def chunk_texts(texts: Sequence[str], *, max_tokens: int) -> List[Chunk]:
    """
    Greedy pack texts into chunks up to max_tokens (estimated).
    Returns chunks with 1-based chunk_id.
    """
    chunks: List[Chunk] = []
    buf: list[str] = []
    buf_tokens = 0
    chunk_id = 1

    for t in texts:
        t_tokens = count_tokens(t)
        # If a single item is too large, hard-split by chars to avoid failing.
        if t_tokens > max_tokens:
            # naive char split; still safe for token estimator
            step = max(500, len(t) // 4)
            for i in range(0, len(t), step):
                part = t[i : i + step]
                part_tokens = count_tokens(part)
                if buf_tokens + part_tokens > max_tokens and buf:
                    text = "\n".join(buf)
                    chunks.append(Chunk(chunk_id=chunk_id, text=text, token_estimate=buf_tokens))
                    chunk_id += 1
                    buf, buf_tokens = [], 0
                buf.append(part)
                buf_tokens += part_tokens
            continue

        if buf_tokens + t_tokens > max_tokens and buf:
            text = "\n".join(buf)
            chunks.append(Chunk(chunk_id=chunk_id, text=text, token_estimate=buf_tokens))
            chunk_id += 1
            buf, buf_tokens = [], 0

        buf.append(t)
        buf_tokens += t_tokens

    if buf:
        text = "\n".join(buf)
        chunks.append(Chunk(chunk_id=chunk_id, text=text, token_estimate=buf_tokens))

    return chunks
