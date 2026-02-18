# reason_bench/providers/anthropic/_client.py
from __future__ import annotations
import os
from anthropic import Anthropic


def make_client() -> Anthropic:
    return Anthropic(
        # This is the default and can be omitted
        api_key=os.environ.get("ANTHROPIC_API_KEY"),
    )


def call_text(
    client: Anthropic,
    *,
    model: str,
    prompt: str,
    temperature: float = 0.0,
    max_tokens: int = 2048,
) -> str:
    msg = client.messages.create(
        model=model,
        # temperature=temperature,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    # msg.content is list of blocks; take text blocks
    out = []
    for b in msg.content:
        t = getattr(b, "text", None)
        if t:
            out.append(t)
    return "".join(out).strip()
