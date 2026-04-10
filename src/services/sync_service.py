import os
from pathlib import Path
from src.database.connection import get_db
from src.services.cleanup_service import delete_document_logs

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

from neo4j import GraphDatabase


# =============================
# CONFIG
# =============================

DATA_DIR = Path("data")

QDRANT_COLLECTION = "document_sections"
qdrant_client = QdrantClient(host="localhost", port=6333)

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"  # 🔴 change if needed

neo4j_driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)


# =============================
# NEO4J DELETE FUNCTION
# =============================

def delete_from_neo4j(document_id):
    with neo4j_driver.session() as session:

        # 🔥 delete all section nodes + relationships
        session.run("""
            MATCH (s:Section {document_id: $doc_id})
            DETACH DELETE s
        """, doc_id=str(document_id))

        # 🔥 delete document node
        session.run("""
            MATCH (d:Document {document_id: $doc_id})
            DETACH DELETE d
        """, doc_id=str(document_id))


# =============================
# MAIN SYNC FUNCTION
# =============================

def sync_deleted_files():
    db = get_db()

    documents_col = db["documents"]
    parsed_col = db["parsed_documents"]

    # Files currently present in data folder
    existing_files = {f.name for f in DATA_DIR.iterdir() if f.is_file()}

    # All documents in DB
    all_docs = list(documents_col.find())

    deleted_count = 0

    for doc in all_docs:
        file_name = doc.get("file_name")

        if file_name not in existing_files:
            document_id = doc["_id"]

            print(f"🗑️ Sync deleting: {file_name}")

            # =============================
            # 🔥 1. Mongo cleanup
            # =============================
            parsed_col.delete_many({"document_id": document_id})
            documents_col.delete_one({"_id": document_id})

            # =============================
            # 🔥 2. Qdrant cleanup
            # =============================
            try:
                qdrant_client.delete(
                    collection_name=QDRANT_COLLECTION,
                    points_selector=Filter(
                        must=[
                            FieldCondition(
                                key="document_id",
                                match=MatchValue(value=str(document_id))
                            )
                        ]
                    )
                )
                print("   → Qdrant cleaned")
            except Exception as e:
                print(f"   ⚠️ Qdrant delete failed: {e}")

            # =============================
            # 🔥 3. Neo4j cleanup
            # =============================
            try:
                delete_from_neo4j(document_id)
                print("   → Neo4j cleaned")
            except Exception as e:
                print(f"   ⚠️ Neo4j delete failed: {e}")

            # =============================
            # 🔥 4. Logs cleanup
            # =============================
            delete_document_logs(document_id)

            deleted_count += 1

    # =============================
    # 🔥 GLOBAL CLEANUP (ghost nodes)
    # =============================
    clean_orphan_neo4j_nodes()

    print(f"\n✅ Sync complete. Deleted {deleted_count} documents.")


# =============================
# 🔥 REMOVE GHOST NODES (IMPORTANT)
# =============================

def clean_orphan_neo4j_nodes():
    """
    Removes any leftover nodes not tied to active documents.
    This ensures ZERO ghost nodes remain.
    """

    db = get_db()
    documents_col = db["documents"]

    # Get all valid document_ids
    valid_ids = {str(doc["_id"]) for doc in documents_col.find()}

    with neo4j_driver.session() as session:

        # Delete Section nodes not linked to valid docs
        session.run("""
            MATCH (s:Section)
            WHERE NOT s.document_id IN $valid_ids
            DETACH DELETE s
        """, valid_ids=list(valid_ids))

        # Delete Document nodes not valid
        session.run("""
            MATCH (d:Document)
            WHERE NOT d.document_id IN $valid_ids
            DETACH DELETE d
        """, valid_ids=list(valid_ids))

    print("   → Ghost nodes cleaned from Neo4j")