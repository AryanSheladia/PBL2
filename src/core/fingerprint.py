from __future__ import annotations
import hashlib
import re

def normalize_for_hash(text: str) -> str:
    t = text.lower()
    t = re.sub(r"\s+", " ", t).strip()
    return t

def fingerprint(text: str) -> str:
    norm = normalize_for_hash(text)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()