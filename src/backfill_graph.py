import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.collections import parsed_documents_collection
from src.services.graph_service import store_graph


def backfill():
    docs = parsed_documents_collection.find()

    count = 0

    for doc in docs:
        sections = doc.get("sections", [])

        if not sections:
            continue

        store_graph(sections)
        count += 1

    print(f"✅ Backfilled {count} documents into Neo4j")


if __name__ == "__main__":
    backfill()