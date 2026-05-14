from __future__ import annotations

import re
import unicodedata
from typing import Any


_PAREN_RE = re.compile(r"\([^)]*\)")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_PLURAL_EXCEPTIONS = {
    "acidosis",
    "arthritis",
    "diabetes",
    "sclerosis",
}


def normalize_relation_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold().strip()
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = _PAREN_RE.sub(" ", text)
    text = _NON_ALNUM_RE.sub(" ", text)
    tokens = [_singularize_token(token) for token in text.split()]
    return " ".join(token for token in tokens if token)


def _singularize_token(token: str) -> str:
    if token in _PLURAL_EXCEPTIONS:
        return token
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 4 and token.endswith("es") and token[-3] in {"s", "x", "z"}:
        return token[:-2]
    if len(token) > 4 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token
