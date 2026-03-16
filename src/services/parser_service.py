from datetime import datetime
from src.database.collections import parsed_documents_collection


def store_parsed_document(document_id, parsed_doc):

    sections = []

    for i, section in enumerate(parsed_doc.sections):
        sections.append({
            "section_id": getattr(section, "section_id", None),
            "heading": getattr(section, "heading", None),
            "content": section.content,
            "confidence": getattr(section, "confidence", None),
            "anchor": getattr(section, "anchor", None),
            "section_order": i
        })

    parsed_document = {
        "document_id": document_id,
        "doc_type": parsed_doc.doc_type,
        "template_name": parsed_doc.template_name,
        "section_count": len(sections),
        "sections": sections,
        "created_at": datetime.utcnow()
    }

    parsed_documents_collection.update_one(
        {"document_id": document_id},
        {"$set": parsed_document},
        upsert=True
    )