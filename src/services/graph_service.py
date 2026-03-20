from neo4j import GraphDatabase
from qdrant_client import QdrantClient

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

# Qdrant client
qdrant = QdrantClient(host="localhost", port=6333)
COLLECTION_NAME = "document_sections"


# -------------------- NODE CREATION --------------------

def create_document(tx, document_id):
    tx.run(
        """
        MERGE (d:Document {document_id: $document_id})
        """,
        document_id=str(document_id),
    )


def create_section(tx, section, document_id):
    tx.run(
        """
        MERGE (s:Section {section_id: $section_id})
        SET s.heading = $heading,
            s.content = $content

        MERGE (d:Document {document_id: $document_id})
        MERGE (d)-[:HAS_SECTION]->(s)
        """,
        section_id=section["section_id"],
        heading=section.get("heading"),
        content=section.get("content"),
        document_id=str(document_id),
    )


# -------------------- RELATIONSHIP --------------------

def create_relationship(tx, from_id, to_id, relation, score=None):
    if score:
        tx.run(
            f"""
            MATCH (a:Section {{section_id: $from_id}})
            MATCH (b:Section {{section_id: $to_id}})
            MERGE (a)-[r:{relation}]->(b)
            SET r.score = $score
            """,
            from_id=from_id,
            to_id=to_id,
            score=score,
        )
    else:
        tx.run(
            f"""
            MATCH (a:Section {{section_id: $from_id}})
            MATCH (b:Section {{section_id: $to_id}})
            MERGE (a)-[:{relation}]->(b)
            """,
            from_id=from_id,
            to_id=to_id,
        )


# -------------------- RULE ENGINE --------------------

def infer_rule_relationships(sections):
    relationships = []

    problem_sections = []
    solution_sections = []
    procedure_sections = []

    for sec in sections:
        heading = (sec.get("heading") or "").lower()

        if "problem" in heading:
            problem_sections.append(sec)

        elif "solution" in heading:
            solution_sections.append(sec)

        elif "procedure" in heading or "runbook" in heading:
            procedure_sections.append(sec)

    for p in problem_sections:
        for s in solution_sections:
            relationships.append((p["section_id"], s["section_id"], "RESOLVED_BY"))

    for p in problem_sections:
        for pr in procedure_sections:
            relationships.append((p["section_id"], pr["section_id"], "INVESTIGATED_BY"))

    # Sequential
    for i in range(len(sections) - 1):
        relationships.append(
            (sections[i]["section_id"], sections[i + 1]["section_id"], "NEXT")
        )

    return relationships


# -------------------- SEMANTIC (QDRANT) --------------------

def infer_semantic_relationships(sections):
    relationships = []

    for sec in sections:
        query_text = sec.get("content", "")
        section_id = sec.get("section_id")

        if not query_text:
            continue

        try:
            results = qdrant.search(
                collection_name=COLLECTION_NAME,
                query_vector=None,  # auto handled if using embeddings stored
                query_filter={
                    "must_not": [
                        {"key": "section_id", "match": {"value": section_id}}
                    ]
                },
                limit=3,
                with_payload=True,
                with_vectors=False,
            )

            for r in results:
                target_id = r.payload.get("section_id")
                score = r.score

                if score > 0.8:
                    relationships.append((section_id, target_id, "DEPENDS_ON", score))
                elif score > 0.7:
                    relationships.append((section_id, target_id, "SIMILAR_TO", score))

        except Exception as e:
            print("⚠️ Qdrant search failed:", e)

    return relationships


# -------------------- MAIN --------------------

def store_graph(document_id, sections):
    try:
        with driver.session() as session:

            # 1️⃣ Create document node
            session.execute_write(create_document, document_id)

            # 2️⃣ Create section nodes + HAS_SECTION
            for sec in sections:
                if not sec.get("section_id"):
                    continue
                session.execute_write(create_section, sec, document_id)

            # 3️⃣ Rule-based relationships
            rule_rels = infer_rule_relationships(sections)

            # 4️⃣ Semantic relationships (Qdrant)
            semantic_rels = infer_semantic_relationships(sections)

            all_rels = rule_rels + semantic_rels

            # 5️⃣ Create relationships
            for rel in all_rels:
                if len(rel) == 4:
                    session.execute_write(create_relationship, *rel)
                else:
                    session.execute_write(create_relationship, *rel)

        print("✅ Graph stored in Neo4j (FULLY CONNECTED)")

    except Exception as e:
        print("❌ Neo4j error:", str(e))