from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
from qdrant_client.models import Filter, FieldCondition, MatchValue

client = QdrantClient(host="localhost", port=6333)
model = SentenceTransformer("all-MiniLM-L6-v2")

COLLECTION_NAME = "document_sections"


def find_similar_sections(text, document_id, target_version_id, top_k=1):
    vector = model.encode(text).tolist()

    query_filter = Filter(
        must=[
            FieldCondition(
                key="document_id",
                match=MatchValue(value=str(document_id))
            ),
            FieldCondition(
                key="version_id",
                match=MatchValue(value=str(target_version_id))
            )
        ]
    )

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=vector,  # ✅ FIXED
        limit=top_k,
        query_filter=query_filter
    )

    # ✅ FIXED result handling
    if not results.points:
        return None, 0

    best = results.points[0]
    return best.payload, best.score
    