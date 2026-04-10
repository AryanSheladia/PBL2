from datetime import datetime
from src.database.collections import parsed_documents_collection
from src.services.embedding_service import embed_and_store
from src.services.graph_service import store_graph


def store_parsed_document(document_id, parsed_doc, version_path):

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
        "created_at": datetime.utcnow(),
        "file_version_path": version_path
    }

    # ✅ Mongo
    parsed_documents_collection.insert_one(parsed_document)
    print("✅ Stored in Mongo")

    # ✅ Qdrant
    embed_and_store(document_id, sections, version_path)

    # ✅ Neo4j
    store_graph(document_id, sections)

    print("✅ Full pipeline complete (Mongo + Qdrant + Neo4j)")