from __future__ import annotations
import re
from typing import Literal

DocType = Literal["PRD", "TDD", "API", "RUNBOOK", "DATA", "GENERIC"]

def detect_doc_type(text: str) -> DocType:
    t = text.lower()

    # API signals
    api_hits = sum(bool(re.search(p, t)) for p in [
        r"\bget\b|\bpost\b|\bput\b|\bdelete\b",
        r"\bendpoint\b|\broutes?\b",
        r"\brequest\b.*\bresponse\b",
        r"\bstatus code\b|\berror code\b"
    ])
    if api_hits >= 2:
        return "API"

    # Runbook/Ops signals
    ops_hits = sum(k in t for k in ["runbook", "rollback", "on-call", "escalation", "incident", "alert"])
    if ops_hits >= 2:
        return "RUNBOOK"

    # PRD signals
    prd_hits = sum(k in t for k in ["goals", "non-goals", "acceptance criteria", "user stories", "success criteria"])
    if prd_hits >= 2:
        return "PRD"

    # TDD/ADR signals
    tdd_hits = sum(k in t for k in ["trade-off", "alternatives", "architecture", "data flow", "observability"])
    if tdd_hits >= 2:
        return "TDD"

    # Data doc signals
    data_hits = sum(k in t for k in ["schema", "columns", "data dictionary", "dataset", "etl", "pipeline"])
    if data_hits >= 2:
        return "DATA"

    return "GENERIC"