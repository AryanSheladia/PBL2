"""
Change Detection Agent
======================
Responsible for detecting changes (additions, modifications, deletions) between
two versions of a parsed document at the section level.

Pipeline position:
    Document Upload → Parse → [ Change Detection Agent ] → Dependency Agent → Update Agent

What it does:
1. Loads the previously stored parsed document from MongoDB (the "old" version).
2. Compares it section-by-section against a newly parsed document (the "new" version).
3. Classifies each changed section as ADDED / MODIFIED / DELETED / UNCHANGED.
4. Persists the change report to MongoDB (collection: change_reports).
5. Returns a structured ChangeReport that downstream agents can consume.

Key design decisions:
- Comparison is fingerprint-based (SHA-256 of normalised content), so whitespace
  and capitalisation differences are ignored – only meaningful content changes trigger events.
- Section identity is determined by `section_id` (canonical template slot), which
  is stable across versions regardless of heading rewording.
- The agent is stateless: every method is pure-functional except `run()`, which
  reads/writes to the database.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

from src.core.fingerprint import fingerprint
from src.database.collections import parsed_documents_collection
from src.services.document_service import get_last_two_parsed_versions
from src.core.hash_utils import hash_text
from src.services.qdrant_service import find_similar_sections


# ---------------------------------------------------------------------------
# Optional MongoDB collection for persisting change reports.
# We create it lazily from the same DB connection already used elsewhere.
# ---------------------------------------------------------------------------
try:
    from src.database.connection import db as _mongo_db
    change_reports_collection = _mongo_db["change_reports"]
except Exception:          # running without DB (tests, demos)
    change_reports_collection = None

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

class ChangeType(str, Enum):
    ADDED = "ADDED"          # section exists in new doc but not in old
    MODIFIED = "MODIFIED"    # section exists in both but content differs
    DELETED = "DELETED"      # section exists in old doc but not in new
    UNCHANGED = "UNCHANGED"  # section exists in both with identical content


@dataclass
class SectionChange:
    """Represents a change event for a single section."""
    section_id: str
    change_type: ChangeType
    heading_old: Optional[str] = None
    heading_new: Optional[str] = None
    fingerprint_old: Optional[str] = None
    fingerprint_new: Optional[str] = None
    content_old: Optional[str] = None    # stored for audit; may be None to save space
    content_new: Optional[str] = None
    confidence: float = 1.0              # parser confidence of the new section (if available)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["change_type"] = self.change_type.value
        return d


@dataclass
class ChangeReport:
    """
    Top-level output of the Change Detection Agent.

    Attributes
    ----------
    document_id : str
        MongoDB ObjectId (as string) of the document record.
    old_version_id : str
        ObjectId of the previously stored parsed_document record.
    new_version_id : str
        ObjectId of the newly stored parsed_document record (may be empty if
        the new version has not been persisted yet).
    doc_type : str
        PRD / TDD / API / RUNBOOK / DATA / GENERIC
    detected_at : str
        ISO-8601 UTC timestamp.
    changes : list[SectionChange]
        One entry per section that is ADDED / MODIFIED / DELETED.
        UNCHANGED sections are omitted by default (see include_unchanged flag).
    summary : dict
        Counts by change type for quick inspection.
    """
    document_id: str
    old_version_id: str
    new_version_id: str
    doc_type: str
    detected_at: str
    changes: List[SectionChange] = field(default_factory=list)
    summary: Dict[str, int] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    @property
    def has_changes(self) -> bool:
        return any(c.change_type != ChangeType.UNCHANGED for c in self.changes)

    def changed_section_ids(self) -> List[str]:
        return [c.section_id for c in self.changes if c.change_type != ChangeType.UNCHANGED]

    def to_dict(self) -> dict:
        d = {
            "document_id": self.document_id,
            "old_version_id": self.old_version_id,
            "new_version_id": self.new_version_id,
            "doc_type": self.doc_type,
            "detected_at": self.detected_at,
            "summary": self.summary,
            "changes": [c.to_dict() for c in self.changes],
        }
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

class ChangeDetectionAgent:
    """
    Compares two versions of a parsed document and produces a ChangeReport.

    Usage
    -----
    Typical (via the pipeline):

        agent = ChangeDetectionAgent()
        report = agent.run(document_id="...", new_parsed_doc=parsed_doc)

    Standalone (for testing / demo without a DB):

        report = agent.compare(old_sections=[...], new_sections=[...],
                               document_id="demo", doc_type="PRD")

    Parameters
    ----------
    include_unchanged : bool
        If True, UNCHANGED sections are included in the report's `changes` list.
        Defaults to False (only meaningful changes are reported).
    store_content : bool
        If True, old and new content strings are embedded in the report for audit.
        Defaults to True.
    """

    def __init__(
        self,
        include_unchanged: bool = False,
        store_content: bool = True,
    ):
        self.include_unchanged = include_unchanged
        self.store_content = store_content

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        document_id: str,
        new_parsed_doc,                          # ParsedDocument (pydantic model)
        new_version_id: str = "",
    ) -> ChangeReport:
        """
        Full pipeline run.
        1. Loads old parsed document from MongoDB.
        2. Compares with new_parsed_doc.
        3. Persists the ChangeReport to MongoDB.
        4. Returns the ChangeReport.

        Parameters
        ----------
        document_id : str
            The document's _id in the `documents` collection (as string).
        new_parsed_doc : ParsedDocument
            Freshly parsed document to compare against the stored version.
        new_version_id : str
            Optional ObjectId string of the new parsed_document record
            (populated after store_parsed_document has run).
        """
        logger.info("[ChangeDetectionAgent] Starting for document_id=%s", document_id)

        # --- Load old version from MongoDB ---
        old_record = self._load_old_version(document_id)

        if old_record is None:
            # First time this document is parsed — nothing to compare against.
            logger.info("[ChangeDetectionAgent] No previous version found. "
                        "Treating all sections as ADDED.")
            old_sections: List[dict] = []
            old_version_id = ""
        else:
            old_sections = old_record.get("sections", [])
            old_version_id = str(old_record.get("_id", ""))

        # --- Convert new parsed doc to section dicts ---
        new_sections = self._pydantic_sections_to_dicts(new_parsed_doc.sections)

        # --- Run comparison ---
        report = self.compare(
            old_sections=old_sections,
            new_sections=new_sections,
            document_id=document_id,
            doc_type=new_parsed_doc.doc_type,
            old_version_id=old_version_id,
            new_version_id=new_version_id,
        )

        # --- Persist report ---
        self._persist_report(report)

        logger.info(
            "[ChangeDetectionAgent] Done. Summary: %s", report.summary
        )
        return report

    def compare(
        self,
        old_sections: List[dict],
        new_sections: List[dict],
        document_id: str = "",
        doc_type: str = "GENERIC",
        old_version_id: str = "",
        new_version_id: str = "",
    ) -> ChangeReport:
        """
        Pure comparison — no database I/O. Useful for unit tests.

        Each section dict must contain at minimum:
            - section_id  : str
            - content     : str
        Optionally:
            - heading     : str
            - fingerprint : str   (recomputed from content if absent)
            - confidence  : float
        """
        # Index by section_id for O(1) lookup
        old_index: Dict[str, dict] = {s["section_id"]: s for s in old_sections}
        new_index: Dict[str, dict] = {s["section_id"]: s for s in new_sections}

        changes: List[SectionChange] = []

        # --- Detect MODIFIED and DELETED sections (iterate over old) ---
        for sid, old_sec in old_index.items():
            fp_old = old_sec.get("fingerprint") or fingerprint(old_sec.get("content", ""))

            if sid not in new_index:
                # Section was removed in the new version
                changes.append(SectionChange(
                    section_id=sid,
                    change_type=ChangeType.DELETED,
                    heading_old=old_sec.get("heading"),
                    fingerprint_old=fp_old,
                    content_old=old_sec.get("content") if self.store_content else None,
                ))
            else:
                new_sec = new_index[sid]
                fp_new = new_sec.get("fingerprint") or fingerprint(new_sec.get("content", ""))

                if fp_old == fp_new:
                    if self.include_unchanged:
                        changes.append(SectionChange(
                            section_id=sid,
                            change_type=ChangeType.UNCHANGED,
                            heading_old=old_sec.get("heading"),
                            heading_new=new_sec.get("heading"),
                            fingerprint_old=fp_old,
                            fingerprint_new=fp_new,
                        ))
                else:
                    changes.append(SectionChange(
                        section_id=sid,
                        change_type=ChangeType.MODIFIED,
                        heading_old=old_sec.get("heading"),
                        heading_new=new_sec.get("heading"),
                        fingerprint_old=fp_old,
                        fingerprint_new=fp_new,
                        content_old=old_sec.get("content") if self.store_content else None,
                        content_new=new_sec.get("content") if self.store_content else None,
                        confidence=new_sec.get("confidence", 1.0),
                    ))

        # --- Detect ADDED sections (in new but not in old) ---
        for sid, new_sec in new_index.items():
            if sid not in old_index:
                fp_new = new_sec.get("fingerprint") or fingerprint(new_sec.get("content", ""))
                changes.append(SectionChange(
                    section_id=sid,
                    change_type=ChangeType.ADDED,
                    heading_new=new_sec.get("heading"),
                    fingerprint_new=fp_new,
                    content_new=new_sec.get("content") if self.store_content else None,
                    confidence=new_sec.get("confidence", 1.0),
                ))

        # --- Build summary ---
        summary = {ct.value: 0 for ct in ChangeType}
        for c in changes:
            summary[c.change_type.value] += 1

        report = ChangeReport(
            document_id=document_id,
            old_version_id=old_version_id,
            new_version_id=new_version_id,
            doc_type=doc_type,
            detected_at=datetime.now(timezone.utc).isoformat(),
            changes=changes,
            summary=summary,
        )
        return report

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_old_version(self, document_id: str) -> Optional[dict]:
        """
        Fetch the most recently stored parsed document for this document_id.
        Returns None if no previous version exists.
        """
        if parsed_documents_collection is None:
            return None
        try:
            record = parsed_documents_collection.find_one(
                {"document_id": document_id},
                sort=[("created_at", -1)],  # latest first
            )
            return record
        except Exception as exc:
            logger.warning("[ChangeDetectionAgent] Could not load old version: %s", exc)
            return None

    def _pydantic_sections_to_dicts(self, sections) -> List[dict]:
        """Convert a list of Section pydantic models to plain dicts."""
        result = []
        for s in sections:
            result.append({
                "section_id": getattr(s, "section_id", ""),
                "heading": getattr(s, "heading", ""),
                "content": getattr(s, "content", ""),
                "fingerprint": getattr(s, "fingerprint", ""),
                "confidence": getattr(s, "confidence", 1.0),
                "anchor": getattr(s, "anchor", ""),
            })
        return result

    def _persist_report(self, report: ChangeReport) -> None:
        """Save the ChangeReport to the change_reports collection."""
        if change_reports_collection is None:
            logger.debug("[ChangeDetectionAgent] No DB — skipping persist.")
            return
        try:
            change_reports_collection.update_one(
                {
                    "document_id": report.document_id,
                    "new_version_id": report.new_version_id,
                },
                {"$set": report.to_dict()},
                upsert=True,
            )
            logger.info("[ChangeDetectionAgent] Report persisted to MongoDB.")
        except Exception as exc:
            logger.error("[ChangeDetectionAgent] Failed to persist report: %s", exc)


# ---------------------------------------------------------------------------
# Convenience function (mirrors the pattern used in parser_service.py)
# ---------------------------------------------------------------------------

def map_sections(sections):
    return {sec["section_id"]: sec["content"] for sec in sections}


def detect_changes(old_doc, new_doc):

    # 🔧 Build full section maps (NOT just content)
    old_map = {sec["section_id"]: sec for sec in old_doc["sections"]}
    new_map = {sec["section_id"]: sec for sec in new_doc["sections"]}

    changes = []
    matched_old = set()

    THRESHOLD = 0.88
    document_id = old_doc["document_id"]

    import re
    def get_num(h):
        if not h:
            return None
        m = re.match(r"\d+", h)
        return m.group() if m else None

    # =============================
    # 🔹 PROCESS NEW SECTIONS
    # =============================
    for new_key, new_sec in new_map.items():

        new_text = new_sec["content"]

        # ✅ 1. Exact ID match
        if new_key in old_map:
            old_sec = old_map[new_key]
            old_text = old_sec["content"]

            if old_text != new_text:
                changes.append({
                    "section_id": new_key,
                    "change_type": "modified",
                    "old_text": old_text,
                    "new_text": new_text
                })

            matched_old.add(new_key)

        else:
            # 🔥 2. Semantic fallback
            payload, score = find_similar_sections(
                new_text,
                document_id,
                old_doc["file_version_path"]
            )

            if payload and score > THRESHOLD:

                old_heading = payload.get("heading", "")
                new_heading = new_sec.get("heading", "")

                # 🔥 critical rule → SAME SECTION NUMBER
                if get_num(old_heading) == get_num(new_heading):

                    changes.append({
                        "section_id": payload["section_id"],
                        "change_type": "modified",
                        "old_text": payload["content"],
                        "new_text": new_text,
                        "similarity_score": round(score, 3)
                    })

                    matched_old.add(payload["section_id"])

                else:
                    # 🆕 true new section
                    changes.append({
                        "section_id": new_key,
                        "change_type": "added",
                        "new_text": new_text
                    })

            else:
                # 🆕 no match found
                changes.append({
                    "section_id": new_key,
                    "change_type": "added",
                    "new_text": new_text
                })

    # =============================
    # 🔹 DETECT DELETED SECTIONS
    # =============================
    for old_key, old_sec in old_map.items():
        if old_key not in matched_old:
            changes.append({
                "section_id": old_key,
                "change_type": "deleted",
                "old_text": old_sec["content"]
            })

    return changes
