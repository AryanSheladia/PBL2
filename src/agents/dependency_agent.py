from typing import List, Dict
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# ---------------- CONFIG ----------------

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

QDRANT_COLLECTION = "document_sections"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
qdrant = QdrantClient(host="localhost", port=6333)
model = SentenceTransformer("all-MiniLM-L6-v2")


# ---------------- AGENT ----------------

class DependencyAgent:

    # 🔴 DELETE CASE → graph traversal
    def _handle_delete(self, section_id: str) -> List[Dict]:

        with driver.session() as session:
            result = session.run(
                """
                MATCH (s:Section {section_id: $sid})
                MATCH (s)<-[:DEPENDS_ON|RELATED_TO*1..3]-(impacted)
                RETURN DISTINCT impacted.section_id AS section_id
                """,
                sid=section_id
            )

            return [{"section_id": r["section_id"], "reason": "upstream dependency"} for r in result]


    # 🟢 ADD / MODIFY → semantic + graph
    def _handle_add_modify(self, section_id: str, content: str) -> List[Dict]:

        if not content:
            return []

        vector = model.encode(content).tolist()

        results = qdrant.query_points(
            collection_name=QDRANT_COLLECTION,
            query=vector,
            limit=8,
            with_payload=True
        )

        impacted = []

        for r in results.points:
            target_id = r.payload.get("section_id")
            score = r.score

            if target_id == section_id:
                continue

            if score < 0.70:
                continue

            impacted.append({
                "section_id": target_id,
                "score": round(score, 3),
                "reason": "semantic similarity"
            })

        return impacted


    # ---------------- MAIN ----------------

    def get_impacted_sections(
        self,
        section_id: str,
        change_type: str,
        new_content: str = None,
        old_content: str = None
    ) -> List[Dict]:

        if change_type == "deleted":
            return self._handle_delete(section_id)

        if change_type in ["added", "modified"]:
            return self._handle_add_modify(section_id, new_content)

        return []