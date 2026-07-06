# scripts/05_embed_to_qdrant.py
# Embeddings run locally via fastembed (ONNX, CPU) — no remote/Kaggle service.
#   pip install fastembed qdrant-client
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from fastembed import TextEmbedding

# BAAI/bge-small-en-v1.5 → 384-dim, matches the Qdrant collection below.
embedder = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
qdrant = QdrantClient(host="localhost", port=6333)

# Tạo collection
qdrant.recreate_collection(
    collection_name="documents",
    vectors_config=VectorParams(size=384, distance=Distance.COSINE),
)


def embed_and_store(records: list[dict]):
    # Embed locally (returns a generator of numpy arrays)
    embeddings = [vec.tolist() for vec in embedder.embed([r["text"] for r in records])]

    points = [
        PointStruct(id=i, vector=emb, payload=rec)
        for i, (emb, rec) in enumerate(zip(embeddings, records))
    ]
    qdrant.upsert(collection_name="documents", points=points)
    print(f"Integration 5 OK: {len(points)} vectors stored in Qdrant (local fastembed)")


# Test với sample data
embed_and_store([
    {"id": "doc_001", "text": "AI platform integration test"},
    {"id": "doc_002", "text": "Kafka to Airflow pipeline"},
])
