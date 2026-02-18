# reason_bench/providers/gemini/_client.py
from __future__ import annotations


def make_client():
    # google-genai SDK style
    from google import genai

    return genai.Client()  # reads GOOGLE_API_KEY or ADC depending on setup


def call_text(client, *, model: str, prompt: str, temperature: float = 0.0) -> str:
    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        # config={"temperature": temperature},
    )
    return (resp.text or "").strip()
