import os
import shutil
from datetime import datetime


def save_versioned_file(file_path, document_id):
    folder = f"logs/versioned_docs/{document_id}"
    os.makedirs(folder, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    ext = file_path.split(".")[-1]
    new_path = os.path.join(folder, f"{timestamp}.{ext}")

    shutil.copy(file_path, new_path)

    return new_path