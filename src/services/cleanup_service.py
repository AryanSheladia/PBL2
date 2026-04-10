import shutil
import os


def delete_document_logs(document_id):
    folder = f"logs/versioned_docs/{document_id}"

    if os.path.exists(folder):
        shutil.rmtree(folder)
        print(f"🗑️ Deleted logs for document {document_id}")
    else:
        print(f"⚠️ No logs found for {document_id}")