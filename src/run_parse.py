from pathlib import Path

from src.parsers.universal_parser import parse_any
from src.services.document_service import create_document, update_document_status
from src.services.parser_service import store_parsed_document


def run_parse(file_path: str):

    file = Path(file_path)

    # 1️⃣ store metadata
    document_id = create_document(
        file_name=file.name,
        file_size=file.stat().st_size,
        storage_path=str(file)
    )

    # mark parsing started
    update_document_status(document_id, "parsing")

    # 2️⃣ parse document
    parsed_doc = parse_any(file)

    # 3️⃣ store parsed output
    store_parsed_document(document_id, parsed_doc)

    # mark parsing complete
    update_document_status(document_id, "parsed")

    return {
        "document_id": str(document_id),
        "doc_type": parsed_doc.doc_type,
        "template": parsed_doc.template_name,
        "sections": len(parsed_doc.sections)
    }