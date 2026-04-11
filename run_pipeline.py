from collections import defaultdict

from src.agents.change_detection_agent import detect_changes
from src.services.document_service import (
    get_last_two_parsed_versions,
    get_all_documents
)
from src.agents.dependency_agent import DependencyAgent
from src.agents.update_agent import UpdateAgent
from src.services.update_service import apply_updates, save_new_version


# ---------------- HELPERS ----------------

def normalize(s):
    return s.strip().lower()


def build_global_lookup(all_docs):
    lookup = {}

    for doc in all_docs:
        for sec in doc.get("sections", []):
            lookup[normalize(sec["section_id"])] = sec["content"]

    return lookup


# ---------------- PRINT HELPERS ----------------

def print_changes(changes):
    print("\n🔍 CHANGE SUMMARY")
    print("-" * 40)

    if not changes:
        print("No changes detected.")
        return

    for c in changes:
        print(f"\n📌 {c['section_id']}")
        print(f"   Type : {c['change_type']}")


def print_dependencies(dep_map):
    print("\n🕸️ IMPACT ANALYSIS")
    print("-" * 40)

    for section, impacted in dep_map.items():
        print(f"\n📌 {section}")

        if not impacted:
            print("   → No impact")
            continue

        for i in impacted:
            print(f"   → {i['section_id']}")


def print_updates(updates):
    print("\n🤖 PROPOSED UPDATES")
    print("-" * 40)

    if not updates:
        print("No updates required.")
        return

    for u in updates:
        print(f"\n📌 {u['target_section_id']}")
        print(f"   Action     : {u['decision']}")
        print(f"   Confidence : {round(u['confidence'], 2)}")
        print(f"   Reason     : {u['reasoning']}")

        print("   --- OLD ---")
        print(f"   {u['old_text'][:80]}")

        print("   --- NEW ---")
        print(f"   {u['updated_text'][:80]}")


# ---------------- MAIN PIPELINE ----------------

if __name__ == "__main__":

    document_id = "69d95d8d53bd81ca202c85dc"  # 🔥 change if needed

    # STEP 1 — GET SOURCE DOC (ONLY FOR CHANGE DETECTION)
    old_doc, new_doc = get_last_two_parsed_versions(document_id)

    # STEP 2 — CHANGE DETECTION
    changes = detect_changes(old_doc, new_doc)
    print_changes(changes)

    if not changes:
        print("\n⚠️ No changes → stopping pipeline")
        exit()

    # STEP 3 — DEPENDENCY ANALYSIS
    dep_agent = DependencyAgent()
    dep_map = {}

    for c in changes:
        impacted = dep_agent.get_impacted_sections(
            section_id=c["section_id"],
            change_type=c["change_type"],
            new_content=c.get("new_text"),
            old_content=c.get("old_text")
        )
        dep_map[c["section_id"]] = impacted

    print_dependencies(dep_map)

    # STEP 4 — LOAD ALL DOCUMENTS (CRITICAL FIX)
    all_docs = get_all_documents()

    print(f"\n🧠 TOTAL DOCUMENTS LOADED: {len(all_docs)}")

    # STEP 5 — GLOBAL LOOKUP (ALL SECTIONS)
    lookup = build_global_lookup(all_docs)

    print("🧠 TOTAL SECTIONS LOADED:", len(lookup))

    # STEP 6 — UPDATE AGENT (LLM)
    update_agent = UpdateAgent()

    updates = update_agent.generate_updates(
        changes,
        dep_map,
        lookup
    )

    print_updates(updates)

    if not updates:
        print("\n⚠️ No updates needed → stopping")
        exit()

    # STEP 7 — HUMAN VERIFICATION
    print("\n🧑 Approve updates? (y/n): ", end="")
    choice = input().strip().lower()

    if choice != "y":
        print("❌ Updates rejected. No changes applied.")
        exit()

    # STEP 8 — GROUP UPDATES BY DOCUMENT
    doc_updates = defaultdict(list)

    for upd in updates:
        doc_name = upd["target_section_id"].split("::")[0]
        doc_updates[doc_name].append(upd)

    # STEP 9 — MAP DOC NAME → ACTUAL DOCUMENT
    doc_map = {}

    for doc in all_docs:
        if doc.get("sections"):
            doc_name = doc["sections"][0]["section_id"].split("::")[0]
            doc_map[doc_name] = doc

    # STEP 10 — APPLY ONLY TO IMPACTED DOCS
    for doc_name, upd_list in doc_updates.items():

        if doc_name not in doc_map:
            print(f"⚠️ Doc not found: {doc_name}")
            continue

        target_doc = doc_map[doc_name]

        approved_ids = [u["target_section_id"] for u in upd_list]

        updated_doc = apply_updates(
            target_doc,
            upd_list,
            approved_ids
        )

        real_doc_id = target_doc["document_id"]  # 🔥 ORIGINAL ObjectId

        save_new_version(real_doc_id, updated_doc)

    print("\n🚀 System updated successfully!")
    print("📌 Only impacted documents were versioned")
    print("📌 Latest versions are now active in system")