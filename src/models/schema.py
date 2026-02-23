from __future__ import annotations
from typing import List, Optional, Tuple, Dict, Any
from pydantic import BaseModel

class Section(BaseModel):
    section_id: str                 # canonical section id
    heading: str                    # detected heading text
    anchor: str                     # stable anchor for diffs (e.g., api:/v1/x or heading slug)
    content: str
    char_range: Tuple[int, int]
    page_range: Optional[Tuple[int, int]] = None
    confidence: float = 0.0
    fingerprint: str = ""           # hash of normalized content
    meta: Dict[str, Any] = {}

class ParsedDocument(BaseModel):
    source_path: str
    template_name: str
    doc_type: str                   # PRD/TDD/API/RUNBOOK/DATA/GENERIC
    full_text: str
    sections: List[Section]