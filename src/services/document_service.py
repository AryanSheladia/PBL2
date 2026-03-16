from datetime import datetime
from src.database.collections import documents_collection


def create_document(file_name, file_size, storage_path):
    """
    Create document metadata when file is uploaded.
    Prevent duplicate records.
    """

    existing = documents_collection.find_one({"file_name": file_name})

    if existing:
        return existing["_id"]

    file_type = file_name.split(".")[-1].lower()

    document = {
        "file_name": file_name,
        "file_type": file_type,
        "file_size": file_size,
        "storage_path": storage_path,
        "upload_date": datetime.utcnow(),
        "status": "uploaded",
        "pipeline_stage": "uploaded"
    }

    result = documents_collection.insert_one(document)

    return result.inserted_id


def get_document_by_filename(file_name):
    """
    Fetch existing document record
    """

    return documents_collection.find_one({"file_name": file_name})


def update_document_status(document_id, status, stage=None):
    """
    Update document processing status
    """

    update_fields = {"status": status}

    if stage:
        update_fields["pipeline_stage"] = stage

    documents_collection.update_one(
        {"_id": document_id},
        {"$set": update_fields}
    )