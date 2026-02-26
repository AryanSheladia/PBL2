from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import fitz  # PyMuPDF
import docx
import pandas as pd
from rapidfuzz import fuzz

from src.models.schema import ParsedDocument, Section
from src.core.doc_type import detect_doc_type
from src.core.fingerprint import fingerprint


# -------------------------------------------------------------------
# Templates
# -------------------------------------------------------------------

TEMPLATE_MAP = {
    "PRD": "src/templates/pubmatic/template_prd.json",
    "TDD": "src/templates/pubmatic/template_tdd_adr.json",
    "API": "src/templates/pubmatic/template_api_spec.json",
    "RUNBOOK": "src/templates/pubmatic/template_runbook.json",
    "DATA": "src/templates/pubmatic/template_data_doc.json",
    "GENERIC": "src/templates/pubmatic/template_generic.json",  # add this file
}


def load_template(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_all_templates() -> List[Dict[str, Any]]:
    return [load_template(p) for p in TEMPLATE_MAP.values()]


def _merge_templates(t1: Dict[str, Any], t2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge section lists by section_id; combine aliases.
    Useful for mixed docs (API+Runbook etc.).
    """
    merged = {
        "template_name": f"{t1.get('template_name','t1')}+{t2.get('template_name','t2')}",
        "doc_type": f"{t1.get('doc_type','') }+{t2.get('doc_type','')}".strip("+"),
        "sections": [],
    }

    by_id: Dict[str, set] = {}
    for t in (t1, t2):
        for s in t.get("sections", []):
            sid = s["section_id"]
            by_id.setdefault(sid, set()).update(s.get("aliases", []))

    for sid, aliases in by_id.items():
        merged["sections"].append({"section_id": sid, "aliases": sorted(list(aliases))})

    return merged


# -------------------------------------------------------------------
# Text utils + heading detection
# -------------------------------------------------------------------

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _heading_like(line: str) -> bool:
    line = line.strip()
    if not line or len(line) > 140:
        return False
    if line.endswith("."):
        return False
    if len(line.split()) > 14:
        return False

    # numbered headings: "1. Title", "2.1 Something"
    if re.match(r"^(\d+(\.\d+)*)\s+.+", line):
        return True

    # all-caps / title-ish
    letters = [c for c in line if c.isalpha()]
    if letters:
        caps_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
        if caps_ratio >= 0.65:
            return True

    words = [w for w in line.split() if any(ch.isalpha() for ch in w)]
    if words:
        starts_caps = sum(1 for w in words if w[:1].isupper())
        if starts_caps / len(words) >= 0.6:
            return True

    return False


def _match_section_id(heading: str, template: Dict[str, Any]) -> Tuple[str, float]:
    """
    Fuzzy-match a heading to template section aliases.
    Returns (section_id, confidence_0_to_1). section_id may be UNMAPPED.
    """
    h = _norm(heading)
    best_id = "UNMAPPED"
    best_score = 0

    for sec in template.get("sections", []):
        sid = sec["section_id"]
        for a in sec.get("aliases", []):
            score = fuzz.partial_ratio(h, _norm(a))
            if score > best_score:
                best_score = score
                best_id = sid

    conf = best_score / 100.0
    if best_score < 75:
        return "UNMAPPED", conf
    return best_id, conf


def _anchor_from_heading(section_id: str, heading: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", _norm(heading)).strip("-")
    if not slug:
        slug = section_id.lower()
    return f"{section_id.lower()}::{slug}"


# -------------------------------------------------------------------
# Template scoring / selection (robust to doc-type mistakes)
# -------------------------------------------------------------------

def _template_score(template: Dict[str, Any], headings: List[str], full_text: str) -> float:
    """
    Score template against doc by:
    - how many headings match aliases (high weight)
    - how many alias keywords exist in full text (low weight)
    """
    score = 0.0
    ft = _norm(full_text)

    # headings match weight
    for h in headings:
        sid, conf = _match_section_id(h, template)
        if sid != "UNMAPPED":
            score += 2.0 * conf

    # text keyword hits weight
    for sec in template.get("sections", []):
        for a in sec.get("aliases", []):
            if _norm(a) in ft:
                score += 0.25

    return score


def _choose_template(full_text: str, candidates: List[Tuple[int, str, Optional[int]]]) -> Tuple[str, Dict[str, Any]]:
    """
    Returns (doc_type_guess, chosen_template_dict).
    Uses detect_doc_type for reporting, but selects template by scoring,
    and merges top two if they are close (mixed docs).
    """
    doc_type_guess = detect_doc_type(full_text)
    headings = [h for (_, h, _) in candidates] if candidates else []

    templates = _load_all_templates()
    scored = [(t, _template_score(t, headings, full_text)) for t in templates]
    scored.sort(key=lambda x: x[1], reverse=True)

    best_t, best_score = scored[0]
    second_t, second_score = scored[1] if len(scored) > 1 else (None, -1)

    # Mixed doc: scores close enough (and meaningful)
    if second_t and best_score > 0 and second_score >= 0.85 * best_score:
        merged = _merge_templates(best_t, second_t)
        return doc_type_guess, merged

    return doc_type_guess, best_t


# -------------------------------------------------------------------
# PDF extraction (font-aware headings)
# -------------------------------------------------------------------

def _extract_pdf(pdf_path: Path) -> Tuple[str, List[Dict[str, Any]], List[Tuple[int, int, int]]]:
    """
    Returns:
      full_text,
      lines: [{text, page, size, bold}]
      page_map: [(start_char, end_char, page_no)]
    """
    doc = fitz.open(str(pdf_path))
    all_lines: List[Dict[str, Any]] = []
    parts: List[str] = []
    page_map: List[Tuple[int, int, int]] = []
    cursor = 0

    for page_no, page in enumerate(doc, start=1):
        d = page.get_text("dict")
        page_lines = []

        for block in d.get("blocks", []):
            for line in block.get("lines", []):
                spans = line.get("spans", [])
                if not spans:
                    continue
                text = "".join(s.get("text", "") for s in spans).strip()
                if not text:
                    continue

                max_size = max(float(s.get("size", 0)) for s in spans)
                bold = any("bold" in str(s.get("font", "")).lower() for s in spans)

                all_lines.append({"text": text, "page": page_no, "size": max_size, "bold": bold})
                page_lines.append(text)

        page_text = "\n".join(page_lines) + "\n"
        start = cursor
        parts.append(page_text)
        cursor += len(page_text)
        end = cursor
        page_map.append((start, end, page_no))

    return "".join(parts), all_lines, page_map


def _pdf_heading_candidates(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not lines:
        return []
    sizes = sorted([l["size"] for l in lines])
    body = sizes[len(sizes) // 2]  # median
    out = []

    for l in lines:
        if not _heading_like(l["text"]):
            continue
        # font threshold for headings
        if l["size"] >= body + 2 or (l["bold"] and l["size"] >= body + 0.5):
            out.append(l)

    return out


def _build_candidates_pdf(full_text: str, lines: List[Dict[str, Any]]) -> List[Tuple[int, str, Optional[int]]]:
    candidates: List[Tuple[int, str, Optional[int]]] = []
    hc = _pdf_heading_candidates(lines)
    for h in hc:
        idx = full_text.find(h["text"])
        if idx != -1:
            candidates.append((idx, h["text"], h["page"]))
    return sorted(set(candidates), key=lambda x: x[0])


def _char_range_to_pages(page_map: Optional[List[Tuple[int, int, int]]], start: int, end: int) -> Optional[Tuple[int, int]]:
    if not page_map:
        return None
    pages = set()
    for s, e, p in page_map:
        if e <= start:
            continue
        if s >= end:
            break
        pages.add(p)
    return (min(pages), max(pages)) if pages else None


# -------------------------------------------------------------------
# DOCX extraction (structure-aware headings)
# -------------------------------------------------------------------

def _extract_docx_with_structure(docx_path: Path) -> Tuple[str, List[Dict[str, Any]]]:
    d = docx.Document(str(docx_path))

    lines: List[Dict[str, Any]] = []
    full_parts: List[str] = []

    for p in d.paragraphs:
        text = p.text.strip()
        if not text:
            continue

        style_name = p.style.name if p.style else ""
        is_heading_style = "heading" in style_name.lower()

        # bold heading detection (common in messy docs)
        bold_run = any((run.text and run.bold) for run in p.runs)

        is_short = len(text.split()) <= 12
        numbered = bool(re.match(r"^(\d+(\.\d+)*)\s+.+", text))

        is_heading = is_heading_style or (bold_run and is_short) or numbered or _heading_like(text)

        lines.append({"text": text, "is_heading": is_heading})
        full_parts.append(text + "\n")

    full_text = "".join(full_parts)
    return full_text, lines


def _build_candidates_docx(full_text: str, docx_lines: List[Dict[str, Any]]) -> List[Tuple[int, str, Optional[int]]]:
    candidates: List[Tuple[int, str, Optional[int]]] = []
    cursor = 0
    for item in docx_lines:
        text = item["text"]
        idx = full_text.find(text, cursor)
        if idx != -1 and item["is_heading"]:
            candidates.append((idx, text, None))
            cursor = idx + len(text)
        elif idx != -1:
            cursor = idx + len(text)
    return sorted(set(candidates), key=lambda x: x[0])


# -------------------------------------------------------------------
# TXT / CSV extraction
# -------------------------------------------------------------------

def _extract_txt(txt_path: Path) -> str:
    return txt_path.read_text(encoding="utf-8", errors="ignore")


def _extract_csv(csv_path: Path) -> str:
    df = pd.read_csv(csv_path)
    lines = []
    lines.append("DATASET OVERVIEW")
    lines.append(f"Rows: {len(df)}")
    lines.append(
        f"Columns ({len(df.columns)}): {', '.join(map(str, df.columns[:30]))}"
        + ("..." if len(df.columns) > 30 else "")
    )
    lines.append("\nSCHEMA\n" + "\n".join([f"- {c}" for c in df.columns[:50]]))
    lines.append("\nSAMPLE\n" + df.head(5).to_string(index=False))
    return "\n".join(lines) + "\n"


# -------------------------------------------------------------------
# Semantic fallback (for trash formatting)
# -------------------------------------------------------------------

def _split_into_chunks(full_text: str) -> List[str]:
    """
    More robust than splitting only on blank lines:
    - split on blank lines first
    - if too few chunks, split on bullet/number patterns
    """
    chunks = [p.strip() for p in re.split(r"\n\s*\n", full_text) if p.strip()]
    if len(chunks) >= 3:
        return chunks

    chunks = []
    buff: List[str] = []
    for line in full_text.splitlines():
        line = line.strip()
        if not line:
            continue

        # new chunk if bullet/number line begins and we already have text
        if re.match(r"^(\-|\*|•|\d+\.|\d+\))\s+", line) and buff:
            chunks.append(" ".join(buff).strip())
            buff = [line]
        else:
            buff.append(line)

    if buff:
        chunks.append(" ".join(buff).strip())

    return [c for c in chunks if c]


def _semantic_bucket(full_text: str, template: Dict[str, Any]) -> List[Section]:
    chunks = _split_into_chunks(full_text)

    # If template lacks MISC_NOTES, create a safe fallback bucket id
    template_section_ids = [s["section_id"] for s in template.get("sections", [])]
    misc_id = "MISC_NOTES" if "MISC_NOTES" in template_section_ids else template_section_ids[-1] if template_section_ids else "UNMAPPED"

    buckets: Dict[str, List[str]] = {sid: [] for sid in template_section_ids}
    if misc_id not in buckets:
        buckets[misc_id] = []

    def score_chunk_to_section(chunk: str, sec: Dict[str, Any]) -> int:
        ct = _norm(chunk)
        score = 0
        for a in sec.get("aliases", []):
            if _norm(a) in ct:
                score += 1
        return score

    for chunk in chunks:
        best_sid = misc_id
        best_score = 0
        for sec in template.get("sections", []):
            sid = sec["section_id"]
            sc = score_chunk_to_section(chunk, sec)
            if sc > best_score:
                best_score = sc
                best_sid = sid

        # If nothing matched, dump into misc (prevents everything going to SUMMARY/AUTH etc.)
        if best_score == 0:
            best_sid = misc_id

        buckets.setdefault(best_sid, []).append(chunk)

    sections: List[Section] = []
    for sec in template.get("sections", []):
        sid = sec["section_id"]
        if not buckets.get(sid):
            continue
        content = "\n\n".join(buckets[sid]).strip()
        sections.append(
            Section(
                section_id=sid,
                heading=sid.replace("_", " ").title(),
                anchor=f"{sid.lower()}::semantic",
                content=content,
                char_range=(0, 0),
                confidence=0.55,
                fingerprint=fingerprint(content),
                meta={"mode": "semantic_fallback"},
            )
        )

    # If misc got content but section not present in template (rare), still emit it
    if misc_id not in template_section_ids and buckets.get(misc_id):
        content = "\n\n".join(buckets[misc_id]).strip()
        sections.append(
            Section(
                section_id=misc_id,
                heading=misc_id.replace("_", " ").title(),
                anchor=f"{misc_id.lower()}::semantic",
                content=content,
                char_range=(0, 0),
                confidence=0.50,
                fingerprint=fingerprint(content),
                meta={"mode": "semantic_fallback"},
            )
        )

    return sections


# -------------------------------------------------------------------
# Main parser
# -------------------------------------------------------------------

def parse_any(file_path: str | Path) -> ParsedDocument:
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    full_text: str
    page_map: Optional[List[Tuple[int, int, int]]] = None
    candidates: List[Tuple[int, str, Optional[int]]] = []

    # 1) Extract + heading candidates (PDF/DOCX)
    if suffix == ".pdf":
        full_text, lines, page_map = _extract_pdf(file_path)
        candidates = _build_candidates_pdf(full_text, lines)

    elif suffix == ".docx":
        full_text, docx_lines = _extract_docx_with_structure(file_path)
        page_map = None
        candidates = _build_candidates_docx(full_text, docx_lines)

    elif suffix in [".txt", ".md"]:
        full_text = _extract_txt(file_path)
        page_map = None
        candidates = []

    elif suffix == ".csv":
        full_text = _extract_csv(file_path)
        page_map = None
        candidates = []

    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    # 2) Choose template robustly (score + merge)
    doc_type_guess, template = _choose_template(full_text, candidates)

    # 3) If no headings available, semantic fallback
    if len(candidates) == 0:
        sections = _semantic_bucket(full_text, template)
        return ParsedDocument(
            source_path=str(file_path),
            template_name=template.get("template_name", "unknown_template"),
            doc_type=doc_type_guess,
            full_text=full_text,
            sections=sections,
        )

    # 4) Heading split
    sections: List[Section] = []
    for i, (start, heading, page) in enumerate(candidates):
        end = candidates[i + 1][0] if i + 1 < len(candidates) else len(full_text)
        chunk = full_text[start:end].strip()

        sid, conf = _match_section_id(heading, template)
        anchor = _anchor_from_heading(sid, heading)
        pr = _char_range_to_pages(page_map, start, end) if page_map else None

        sections.append(
            Section(
                section_id=sid,
                heading=heading,
                anchor=anchor,
                content=chunk,
                char_range=(start, end),
                page_range=pr,
                confidence=max(0.60, conf),
                fingerprint=fingerprint(chunk),
                meta={"mode": "heading_split", "doc_type_guess": doc_type_guess},
            )
        )

    return ParsedDocument(
        source_path=str(file_path),
        template_name=template.get("template_name", "unknown_template"),
        doc_type=doc_type_guess,
        full_text=full_text,
        sections=sections,
    )