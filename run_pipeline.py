from src.agents.change_detection_agent import detect_changes
from src.agents.update_agent import generate_updates
from src.services.document_service import get_last_two_parsed_versions
from src.agents.dependency_agent import DependencyAgent


def print_changes(changes):
    print("\n🔍 CHANGE SUMMARY\n" + "-" * 40)

    for change in changes:
        print(f"\n📌 Section: {change['section_id']}")
        print(f"   Type   : {change['change_type']}")

        if change["change_type"] == "modified":
            print(f"   OLD    : {change['old_text'][:80]}")
            print(f"   NEW    : {change['new_text'][:80]}")

        elif change["change_type"] == "added":
            print(f"   NEW    : {change['new_text'][:80]}")

        elif change["change_type"] == "deleted":
            print(f"   OLD    : {change['old_text'][:80]}")


def print_dependencies(dep_map):
    print("\n🕸️ DEPENDENCY IMPACT\n" + "-" * 40)

    for sec_id, impacted in dep_map.items():
        print(f"\n📌 Changed Section: {sec_id}")

        if not impacted:
            print("   → No impacted sections")
            continue

        for imp in impacted:
            print(f"   → {imp['section_id']} ({imp.get('reason', 'unknown')})")


if __name__ == "__main__":

    # 🔥 PUT YOUR DOCUMENT ID HERE
    document_id = "69d8f0627fc93a1c3def3390"

    # STEP 1 — FETCH VERSIONS
    old_doc, new_doc = get_last_two_parsed_versions(document_id)

    # STEP 2 — CHANGE DETECTION
    changes = detect_changes(old_doc, new_doc)
    print_changes(changes)

    # STEP 3 — DEPENDENCY ANALYSIS
    agent = DependencyAgent()

    dependency_map = {}

    for change in changes:
        impacted = agent.get_impacted_sections(
            section_id=change["section_id"],
            change_type=change["change_type"],
            new_content=change.get("new_text"),
            old_content=change.get("old_text")
        )

        dependency_map[change["section_id"]] = impacted

    print_dependencies(dependency_map)

    # STEP 4 — UPDATE AGENT (we’ll enable after this works)
    # updates = generate_updates(changes, dependency_map)
    # print("\n🤖 GENERATED UPDATES\n", updates)