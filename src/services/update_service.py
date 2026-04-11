import os
from datetime import datetime
from copy import deepcopy
from pymongo import MongoClient


# ---------------- APPLY UPDATES ----------------

def apply_updates(document, updates, approved_ids):

    new_doc = deepcopy(document)
    new_sections = []

    for sec in new_doc["sections"]:
        sid = sec["section_id"]

        skip_section = False

        for upd in updates:

            if upd["target_section_id"] != sid:
                continue

            if sid not in approved_ids:
                continue

            # 🔴 DELETE → REMOVE SECTION
            if upd["decision"] == "delete":
                skip_section = True
                break

            # 🟢 MODIFY
            elif upd["decision"] == "modify":
                sec["content"] = upd["updated_text"]

        if not skip_section:
            new_sections.append(sec)

    new_doc["sections"] = new_sections

    return new_doc


# ---------------- SAVE VERSION ----------------

def save_new_version(document_id, doc):

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # 📁 SAVE CLEAN TEXT FILE
    folder = f"logs/final_versions/{document_id}"
    os.makedirs(folder, exist_ok=True)

    path = f"{folder}/{timestamp}.txt"

    with open(path, "w", encoding="utf-8") as f:
        for sec in doc["sections"]:

            heading = sec.get("heading", "").strip()
            content = sec.get("content", "").strip()

            # 🔥 REMOVE DUPLICATE HEADING INSIDE CONTENT
            if content.startswith(heading):
                content = content[len(heading):].strip()

            f.write(heading + "\n")
            f.write(content + "\n\n")

    print(f"📁 Saved FINAL version → {path}")

    # 🧠 SAVE IN DB (VERSION CONTROL)
    client = MongoClient("mongodb://localhost:27017/")
    db = client["PBL2"]
    collection = db["parsed_documents"]

    # find current latest
    latest_doc = collection.find_one({
        "document_id": document_id,
        "is_latest": True
    })

    parent_version = None
    new_version_number = 1

    if latest_doc:
        parent_version = latest_doc.get("version", "v1")

        try:
            new_version_number = int(parent_version.replace("v", "")) + 1
        except:
            new_version_number = 2

        # mark old as not latest
        collection.update_one(
            {"_id": latest_doc["_id"]},
            {"$set": {"is_latest": False}}
        )

    new_version = f"v{new_version_number}"

    # insert new version
    collection.insert_one({
        "document_id": document_id,
        "version": new_version,
        "parent_version": parent_version,
        "sections": doc["sections"],
        "source": "update_agent",
        "created_at": datetime.utcnow(),
        "is_latest": True
    })

    print(f"🧠 New version stored in DB → {new_version}")
    print("🚀 This is now the ACTIVE version")