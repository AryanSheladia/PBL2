import uuid
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

client = QdrantClient(host="localhost", port=6333)
model = SentenceTransformer("all-MiniLM-L6-v2")

COLLECTION_NAME = "document_sections"


def embed_and_store(document_id, sections, version_id):

    points = []

    for section in sections:
        content = section.get("content", "")
        heading = section.get("heading", "")

        # 🔥 KEY: heading + content
        text = f"{heading}\n{content}"

        if not text.strip():
            continue

        vector = model.encode(text).tolist()

        points.append({
            "id": str(uuid.uuid4()),
            "vector": vector,
            "payload": {
                "document_id": str(document_id),
                "section_id": section.get("section_id"),
                "heading": heading,
                "content": content,
                "version_id": str(version_id)
            }
        })

    if not points:
        print("⚠️ No valid sections to embed")
        return

    try:
        client.get_collection(COLLECTION_NAME)
    except:
        client.recreate_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={"size": 384, "distance": "Cosine"}
        )

    client.upsert(collection_name=COLLECTION_NAME, points=points)

    print(f"✅ Stored {len(points)} embeddings in Qdrant")