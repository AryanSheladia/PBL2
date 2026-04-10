from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

qdrant = QdrantClient(host="localhost", port=6333)
COLLECTION_NAME = "document_sections"

model = SentenceTransformer("all-MiniLM-L6-v2")


# -------------------- NODES --------------------

def create_document(tx, document_id):
    tx.run(
        """
        MERGE (d:Document {document_id: $document_id})
        SET d.type = "document"
        """,
        document_id=str(document_id),
    )


def create_section(tx, section, document_id):
    tx.run(
        """
        MERGE (s:Section {
            section_id: $section_id,
            document_id: $document_id
        })
        SET s.heading = $heading,
            s.content = $content,
            s.type = "section"

        MERGE (d:Document {document_id: $document_id})
        MERGE (d)-[:HAS_SECTION]->(s)
        """,
        section_id=section["section_id"],
        heading=section.get("heading"),
        content=section.get("content"),
        document_id=str(document_id),
    )


# -------------------- RELATIONSHIPS --------------------

def create_relationship(tx, from_id, to_id, relation, document_id, score=None):
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


# -------------------- RULE RELATIONSHIPS --------------------

def infer_rule_relationships(sections):
    relationships = []

    for i in range(len(sections) - 1):
        relationships.append(
            (sections[i]["section_id"], sections[i + 1]["section_id"], "NEXT", None)
        )

    return relationships


# -------------------- SEMANTIC RELATIONSHIPS --------------------

def infer_semantic_relationships(sections, document_id):
    relationships = []

    SAME_DOC_TOP_K = 2
    CROSS_DOC_TOP_K = 2

    STRONG_THRESHOLD = 0.82
    MEDIUM_THRESHOLD = 0.72

    for sec in sections:
        content = sec.get("content", "")
        heading = sec.get("heading", "")
        section_id = sec.get("section_id")

        text = f"{heading}\n{content}"

        if not text.strip():
            continue

        try:
            vector = model.encode(text).tolist()

            results = qdrant.query_points(
                collection_name=COLLECTION_NAME,
                query=vector,
                limit=12,
                with_payload=True
            )

            same_doc = []
            cross_doc = []

            for r in results.points:
                target_id = r.payload.get("section_id")
                target_doc = r.payload.get("document_id")
                score = r.score

                if target_id == section_id:
                    continue

                # 🔥 relation strength
                if score >= STRONG_THRESHOLD:
                    rel_type = "DEPENDS_ON"
                elif score >= MEDIUM_THRESHOLD:
                    rel_type = "RELATED_TO"
                else:
                    continue

                if target_doc == str(document_id):
                    same_doc.append((target_id, rel_type, score))
                else:
                    cross_doc.append((target_id, rel_type, score))

            same_doc = sorted(same_doc, key=lambda x: x[2], reverse=True)
            cross_doc = sorted(cross_doc, key=lambda x: x[2], reverse=True)

            # 🔥 SAME DOC
            for tgt, rel, score in same_doc[:SAME_DOC_TOP_K]:
                relationships.append((section_id, tgt, rel, score))
                relationships.append((tgt, section_id, rel, score))  # bidirectional

            # 🔥 CROSS DOC
            for tgt, rel, score in cross_doc[:CROSS_DOC_TOP_K]:
                relationships.append((section_id, tgt, rel, score))
                relationships.append((tgt, section_id, rel, score))  # bidirectional

        except Exception as e:
            print("⚠️ Qdrant failed:", e)

    return relationships


# -------------------- MAIN --------------------

def store_graph(document_id, sections):
    try:
        with driver.session() as session:

            session.execute_write(create_document, document_id)

            for sec in sections:
                if not sec.get("section_id"):
                    continue
                session.execute_write(create_section, sec, document_id)

            rule_rels = infer_rule_relationships(sections)
            semantic_rels = infer_semantic_relationships(sections, document_id)

            all_rels = rule_rels + semantic_rels

            for rel in all_rels:
                session.execute_write(
                    create_relationship,
                    rel[0],
                    rel[1],
                    rel[2],
                    document_id,
                    rel[3]
                )

        print("🔥 FINAL BALANCED GRAPH CREATED")

    except Exception as e:
        print("❌ Neo4j error:", str(e))