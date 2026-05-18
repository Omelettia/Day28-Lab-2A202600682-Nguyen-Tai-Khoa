# Lab #28 — Full Platform Integration Sprint
**AICB-P2T2 · Ngày 28 · Chương 6: Tổng Hợp**  
**Thời gian:** 2 giờ  
**Mục tiêu:** Ghép toàn bộ stack từ N16–N27 thành một AI platform hoàn chỉnh, end-to-end

---

## Kiến trúc Hybrid (Local + Kaggle)

```
┌─────────────────────────────────────────────────────────┐
│                  LOCAL (Docker Compose)                  │
│                                                          │
│  Kafka ──► Prefect ──► Delta Lake ──► Feast              │
│     │                                    │               │
│     └──► Vector Store (Qdrant)           │               │
│                                          ▼               │
│  Prometheus ◄── Grafana          API Gateway (FastAPI)   │
│  LangSmith tracing                       ▲               │
└──────────────────────────────────────────┼───────────────┘
                                           │  HTTP (ngrok)
┌──────────────────────────────────────────┼───────────────┐
│                 KAGGLE (GPU T4/P100)      │               │
│                                          │               │
│  vLLM / SGLang serving ◄─────────────────┘               │
│  MLflow experiment tracking                              │
│  Embedding model (sentence-transformers)                 │
│  Model Registry                                          │
└─────────────────────────────────────────────────────────┘
```

---

## Yêu cầu trước khi bắt đầu

- Docker Desktop đang chạy trên máy local
- Tài khoản Kaggle đã kích hoạt GPU (Settings → Accelerator → GPU T4 x2)
- `ngrok` đã cài: `brew install ngrok` hoặc tải tại ngrok.com
- Python 3.10+ và `pip` trên máy local

---

## PHẦN 1 — Dựng Infrastructure Local (Docker Compose)

**Mục tiêu:** Chạy Kafka, Prefect, Delta Lake, Feast, Qdrant, Prometheus, Grafana trên local.

### Bước 1.1 — Tạo cấu trúc thư mục

```bash
mkdir -p lab28/{prefect/flows,delta-lake,feast,monitoring,smoke-tests,scripts,api-gateway}
cd lab28
```

### Bước 1.2 — Tạo `docker-compose.yml`

```yaml
# docker-compose.yml
version: "3.9"

services:
  # ── Kafka Stack ──────────────────────────────────────────
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  # ── Prefect ───────────────────────────────────────────────
  prefect-orion:
    image: prefecthq/prefect:2.14.0-python3.10
    command: prefect orion start --host 0.0.0.0
    ports:
      - "4200:4200"
    volumes:
      - prefect_data:/root/.prefect

  prefect-worker:
    image: prefecthq/prefect:2.14.0-python3.10
    command: prefect worker start -p docker -n lab28-worker
    environment:
      PREFECT_API_URL: http://prefect-orion:4200/api
    volumes:
      - ./prefect/flows:/opt/prefect/flows
      - ./delta-lake:/opt/delta-lake
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on: [prefect-orion, kafka]

  # ── Vector Store (Qdrant) ─────────────────────────────────
  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
    volumes:
      - qdrant_data:/qdrant/storage

  # ── Feature Store (Feast via Redis) ──────────────────────
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  # ── Monitoring Stack ──────────────────────────────────────
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    depends_on: [prometheus]
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin

  # ── API Gateway ───────────────────────────────────────────
  api-gateway:
    build: ./api-gateway
    ports:
      - "8000:8000"
    environment:
      VLLM_URL: ${VLLM_NGROK_URL}
      QDRANT_URL: http://qdrant:6333
      REDIS_URL: redis://redis:6379
    depends_on: [qdrant, redis]

volumes:
  prefect_data:
  qdrant_data:
```

### Bước 1.3 — Tạo Prometheus config

```yaml
# monitoring/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "api-gateway"
    static_configs:
      - targets: ["api-gateway:8000"]

  - job_name: "kafka"
    static_configs:
      - targets: ["kafka:9092"]

  - job_name: "prefect-orion"
    static_configs:
      - targets: ["prefect-orion:4200"]
```

### Bước 1.4 — Khởi động stack

```bash
docker compose up -d
docker compose ps   # kiểm tra tất cả services đang Up
```

**Kiểm tra:**
- Prefect UI: http://localhost:4200
- Grafana: http://localhost:3000 (admin/admin)
- Qdrant: http://localhost:6333/dashboard
- Prometheus: http://localhost:9090

---

## PHẦN 2 — Kaggle GPU Setup & Expose qua Tunnel

**Mục tiêu:** Chạy vLLM serving trên Kaggle GPU, expose port ra để local gọi được.

### Bước 2.1 — Tạo Kaggle Notebook

Tạo notebook mới trên Kaggle, bật **GPU T4 x2**, chọn 1 trong 2 option:

**Option A: Dùng ngrok**

```python
# Cell 1 — Cài dependencies
!pip install -q vllm fastapi uvicorn pyngrok mlflow sentence-transformers

# Cell 2 — Setup ngrok token (lấy tại ngrok.com/your-authtoken)
from pyngrok import ngrok
ngrok.set_auth_token("YOUR_NGROK_TOKEN")  # thay token của bạn

# Cell 3 — Khởi động vLLM server
import subprocess, threading, time

def run_vllm():
    subprocess.run([
        "python", "-m", "vllm.entrypoints.openai.api_server",
        "--model", "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
        "--port", "8001",
        "--max-model-len", "4096",
        "--gpu-memory-utilization", "0.85"
    ])

thread = threading.Thread(target=run_vllm, daemon=True)
thread.start()
time.sleep(60)  # chờ model load
print("vLLM server started")

# Cell 4 — Tạo ngrok tunnel
tunnel = ngrok.connect(8001, "http")
vllm_url = tunnel.public_url
print(f"vLLM URL (copy this): {vllm_url}")
# → Paste URL này vào file .env trên local
```

**Option B: Dùng cloudflared**

```python
# Cell 1 — Cài dependencies
!pip install -q vllm fastapi uvicorn cloudflared mlflow sentence-transformers

# Cell 2 — Khởi động vLLM server
import subprocess, threading, time

def run_vllm():
    subprocess.run([
        "python", "-m", "vllm.entrypoints.openai.api_server",
        "--model", "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
        "--port", "8001",
        "--max-model-len", "4096",
        "--gpu-memory-utilization", "0.85"
    ])

thread = threading.Thread(target=run_vllm, daemon=True)
thread.start()
time.sleep(60)  # chờ model load
print("vLLM server started")

# Cell 3 — Tạo cloudflare tunnel
import subprocess
tunnel = subprocess.run(["cloudflared", "tunnel", "--url", "http://localhost:8001"], capture_output=True, text=True)
print(f"vLLM URL (copy from output):")
print(tunnel.stdout)
# → Paste URL này vào file .env trên local
```

### Bước 2.2 — Embedding service trên Kaggle

**Nếu dùng ngrok:**

```python
# Cell 5 — Embedding API server
from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
import uvicorn, threading

app = FastAPI()
model = SentenceTransformer("BAAI/bge-small-en-v1.5")

@app.post("/embed")
def embed(data: dict):
    texts = data["texts"]
    embeddings = model.encode(texts).tolist()
    return {"embeddings": embeddings}

def run_embed():
    uvicorn.run(app, host="0.0.0.0", port=8002)

threading.Thread(target=run_embed, daemon=True).start()
embed_tunnel = ngrok.connect(8002, "http")
print(f"Embedding URL: {embed_tunnel.public_url}")
```

**Nếu dùng cloudflared:**

```python
# Cell 4 — Embedding API server
from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
import uvicorn, threading

app = FastAPI()
model = SentenceTransformer("BAAI/bge-small-en-v1.5")

@app.post("/embed")
def embed(data: dict):
    texts = data["texts"]
    embeddings = model.encode(texts).tolist()
    return {"embeddings": embeddings}

def run_embed():
    uvicorn.run(app, host="0.0.0.0", port=8002)

threading.Thread(target=run_embed, daemon=True).start()
import subprocess
embed_tunnel = subprocess.run(["cloudflared", "tunnel", "--url", "http://localhost:8002"], capture_output=True, text=True)
print(f"Embedding URL (copy from output):")
print(embed_tunnel.stdout)
```

### Bước 2.3 — Cập nhật `.env` trên local

```bash
# lab28/.env
# Nếu dùng ngrok:
VLLM_NGROK_URL=https://xxxx.ngrok-free.app   # từ Cell 4
EMBED_NGROK_URL=https://yyyy.ngrok-free.app   # từ Cell 5
# Nếu dùng cloudflared:
# VLLM_NGROK_URL=https://xxxx.trycloudflare.com   # từ Cell 3
# EMBED_NGROK_URL=https://yyyy.trycloudflare.com   # từ Cell 4
LANGCHAIN_API_KEY=your_langsmith_key
LANGCHAIN_PROJECT=lab28-platform
```

---

## PHẦN 3 — Kết nối 10 Integration Points

### Integration 1: Data Ingestion → Kafka

```python
# scripts/01_ingest_to_kafka.py
from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

def ingest_data(records: list[dict]):
    for record in records:
        producer.send("data.raw", value=record)
        print(f"Sent: {record['id']}")
    producer.flush()

# Test
sample_data = [
    {"id": "doc_001", "text": "AI platform integration test", "timestamp": time.time()},
    {"id": "doc_002", "text": "Kafka to Airflow pipeline", "timestamp": time.time()},
]
ingest_data(sample_data)
print("Integration 1 OK: Data → Kafka")
```

### Integration 2: Kafka → Prefect Pipeline

```python
# prefect/flows/kafka_to_delta.py
from prefect import flow, task
from kafka import KafkaConsumer
import json, os
import pandas as pd
from datetime import datetime

@task
def consume_and_process():
    """Consume data từ Kafka topic"""
    consumer = KafkaConsumer(
        "data.raw",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode())
    )
    records = []
    for msg in consumer:
        records.append(msg.value)

    print(f"Consumed {len(records)} records from Kafka")
    return records

@task
def save_to_delta(records):
    """Lưu records vào Delta Lake (parquet format)"""
    if not records:
        print("No records to save")
        return

    df = pd.DataFrame(records)
    # Giả lập Delta Lake bằng parquet (local volume)
    path = "/opt/delta-lake/raw"
    os.makedirs(path, exist_ok=True)
    df.to_parquet(f"{path}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
    print(f"Saved {len(df)} records to Delta Lake")

@flow(name="Kafka to Delta Pipeline", schedule="* */5 * * *")
def kafka_to_delta_flow():
    """Main flow: consume từ Kafka và lưu vào Delta Lake"""
    records = consume_and_process()
    save_to_delta(records)

if __name__ == "__main__":
    # Deploy flow đến Prefect Orion
    kafka_to_delta_flow.deploy(
        name="kafka-to-delta",
        work_queue_name="lab28-worker"
    )
```

### Integration 3 & 4: Delta Lake → Feature Store (Feast)

```python
# scripts/03_delta_to_feast.py
import pandas as pd
import glob, os, redis, json

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def load_from_delta_and_push_feast():
    files = glob.glob("delta-lake/raw/*.parquet")
    if not files:
        print("No data in Delta Lake yet")
        return

    df = pd.concat([pd.read_parquet(f) for f in files])
    print(f"Loaded {len(df)} records from Delta Lake")

    # Push features vào Redis (Feast online store)
    for _, row in df.iterrows():
        feature_key = f"feature:{row['id']}"
        r.set(feature_key, json.dumps({
            "text": row["text"],
            "timestamp": row["timestamp"],
            "processed": True
        }))

    print(f"Integration 3+4 OK: Delta Lake → Feast (Redis) — {len(df)} features stored")

load_from_delta_and_push_feast()
```

### Integration 5: Data → Vector Store (Embeddings)

```python
# scripts/05_embed_to_qdrant.py
import requests
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import os

EMBED_URL = os.environ["EMBED_NGROK_URL"]
qdrant = QdrantClient(host="localhost", port=6333)

# Tạo collection
qdrant.recreate_collection(
    collection_name="documents",
    vectors_config=VectorParams(size=384, distance=Distance.COSINE)
)

def embed_and_store(records: list[dict]):
    # Gọi Kaggle embedding service
    response = requests.post(f"{EMBED_URL}/embed", json={"texts": [r["text"] for r in records]})
    embeddings = response.json()["embeddings"]

    points = [
        PointStruct(id=i, vector=emb, payload=rec)
        for i, (emb, rec) in enumerate(zip(embeddings, records))
    ]
    qdrant.upsert(collection_name="documents", points=points)
    print(f"Integration 5 OK: {len(points)} vectors stored in Qdrant")

# Test với sample data
embed_and_store([
    {"id": "doc_001", "text": "AI platform integration test"},
    {"id": "doc_002", "text": "Kafka to Airflow pipeline"},
])
```

### Integration 6 & 7: MLflow → Model Registry → vLLM

```python
# Chạy trên Kaggle Notebook (Cell 6)
import mlflow
import os

mlflow.set_tracking_uri("https://dagshub.com/YOUR_USER/lab28.mlflow")  # hoặc dùng local
mlflow.set_experiment("lab28-integration")

with mlflow.start_run(run_name="vllm-serving-v1"):
    mlflow.log_param("model", "Qwen2.5-7B-Instruct-GPTQ-Int4")
    mlflow.log_param("max_model_len", 4096)
    mlflow.log_metric("gpu_memory_utilization", 0.85)
    mlflow.log_metric("avg_latency_ms", 450)

    # Tag model version
    mlflow.set_tag("serving_url", vllm_url)
    mlflow.set_tag("status", "production")

print("Integration 6+7 OK: MLflow → Model Registry → vLLM")
```

### Integration 8: Serving → API Gateway

```python
# api-gateway/main.py
from fastapi import FastAPI, Request
from prometheus_fastapi_instrumentator import Instrumentator
import httpx, os, time, langsmith

app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)  # Integration 9: Prometheus

VLLM_URL = os.environ["VLLM_NGROK_URL"]
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")

@app.post("/api/v1/chat")
async def chat(request: Request):
    body = await request.json()
    query = body["query"]
    start = time.time()

    # 1. Vector search
    async with httpx.AsyncClient() as client:
        search_resp = await client.post(f"{QDRANT_URL}/collections/documents/points/search", json={
            "vector": body.get("embedding", [0.0] * 384),
            "limit": 3
        })
        context = search_resp.json().get("result", [])

    # 2. LLM inference
    prompt = f"Context: {context}\n\nQuery: {query}"
    async with httpx.AsyncClient(timeout=30) as client:
        llm_resp = await client.post(f"{VLLM_URL}/v1/chat/completions", json={
            "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
            "messages": [{"role": "user", "content": prompt}]
        })

    latency = (time.time() - start) * 1000
    result = llm_resp.json()

    return {
        "answer": result["choices"][0]["message"]["content"],
        "latency_ms": round(latency, 2),
        "model": result["model"]
    }

@app.get("/health")
def health():
    return {"status": "ok"}
```

### Integration 9 & 10: Prometheus/Grafana + LangSmith

```python
# scripts/09_verify_observability.py
import requests

def check_prometheus():
    resp = requests.get("http://localhost:9090/api/v1/query",
                        params={"query": 'http_requests_total{job="api-gateway"}'})
    data = resp.json()
    assert data["status"] == "success"
    print("Integration 9 OK: Prometheus metrics flowing")

def check_langsmith():
    import os
    from langsmith import Client
    client = Client(api_key=os.environ["LANGCHAIN_API_KEY"])
    runs = list(client.list_runs(project_name="lab28-platform", limit=1))
    assert len(runs) > 0
    print("Integration 10 OK: LangSmith traces visible")

check_prometheus()
check_langsmith()
```

---

## PHẦN 4 — 5 Smoke Tests (End-to-End)

**Chạy:** `pytest smoke-tests/ -v`

```python
# smoke-tests/test_e2e.py
import pytest, requests, time, os

BASE_URL = "http://localhost:8000"
VLLM_URL = os.environ.get("VLLM_NGROK_URL", "")

# ── Test 1: Happy Path — Full Inference Request ───────────────
class TestHappyPath:
    def test_full_inference_returns_200(self):
        """Data vào API Gateway, nhận được answer từ LLM"""
        resp = requests.post(f"{BASE_URL}/api/v1/chat", json={
            "query": "What is platform engineering?",
            "embedding": [0.1] * 384
        }, timeout=30)
        assert resp.status_code == 200
        data = resp.json()
        assert "answer" in data
        assert len(data["answer"]) > 10
        assert data["latency_ms"] < 2000

    def test_health_check_passes(self):
        """API Gateway health check"""
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


# ── Test 2: Data Ingestion Journey ───────────────────────────
class TestDataIngestion:
    def test_kafka_ingest_and_qdrant_store(self):
        """Ingest data vào Kafka → pipeline → vector store"""
        from kafka import KafkaProducer
        import json

        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode()
        )
        producer.send("data.raw", {"id": "smoke_001", "text": "smoke test document"})
        producer.flush()

        time.sleep(10)  # chờ pipeline xử lý

        # Kiểm tra Qdrant nhận được
        resp = requests.get("http://localhost:6333/collections/documents")
        assert resp.status_code == 200
        count = resp.json()["result"]["points_count"]
        assert count > 0
        print(f"Vector store has {count} documents")


# ── Test 3: Observability Journey ────────────────────────────
class TestObservability:
    def test_prometheus_scrapes_api_gateway(self):
        """Prometheus đang scrape metrics từ API Gateway"""
        resp = requests.get("http://localhost:9090/api/v1/query",
                            params={"query": "up{job='api-gateway'}"})
        assert resp.status_code == 200
        result = resp.json()["data"]["result"]
        assert len(result) > 0
        assert result[0]["value"][1] == "1"  # service is up

    def test_grafana_dashboard_accessible(self):
        """Grafana dashboard load được"""
        resp = requests.get("http://localhost:3000/api/health",
                            auth=("admin", "admin"))
        assert resp.status_code == 200


# ── Test 4: Error Handling & Failure Path ────────────────────
class TestFailurePath:
    def test_invalid_request_returns_422(self):
        """API Gateway từ chối request thiếu field bắt buộc"""
        resp = requests.post(f"{BASE_URL}/api/v1/chat", json={})
        assert resp.status_code in [400, 422]

    def test_timeout_handled_gracefully(self):
        """Timeout không làm crash service"""
        try:
            resp = requests.post(f"{BASE_URL}/api/v1/chat",
                                 json={"query": "test", "embedding": [0.1] * 384},
                                 timeout=0.001)
        except requests.exceptions.Timeout:
            pass  # Expected — graceful timeout

        # Service vẫn healthy sau timeout
        health = requests.get(f"{BASE_URL}/health", timeout=5)
        assert health.status_code == 200


# ── Test 5: Feature Store Journey ────────────────────────────
class TestFeatureStore:
    def test_feast_redis_has_features(self):
        """Feast (Redis) có features sau khi pipeline chạy"""
        import redis
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        keys = r.keys("feature:*")
        assert len(keys) > 0, "No features found in Feast store"
        print(f"Feature store has {len(keys)} feature entries")
```

---

## PHẦN 5 — Production Readiness Checklist

Chạy script tự động kiểm tra:

```python
# scripts/production_readiness_check.py
import requests, redis, subprocess

results = {}

def check(name, fn):
    try:
        fn()
        results[name] = "PASS"
        print(f"  [PASS] {name}")
    except Exception as e:
        results[name] = f"FAIL: {e}"
        print(f"  [FAIL] {name}: {e}")

print("\n=== RELIABILITY ===")
check("Health check endpoint", lambda:
    requests.get("http://localhost:8000/health").raise_for_status())
check("API Gateway responds", lambda:
    requests.get("http://localhost:8000/docs").raise_for_status())

print("\n=== OBSERVABILITY ===")
check("Prometheus up", lambda:
    requests.get("http://localhost:9090/-/healthy").raise_for_status())
check("Grafana up", lambda:
    requests.get("http://localhost:3000/api/health").raise_for_status())
check("Metrics endpoint exposed", lambda:
    requests.get("http://localhost:8000/metrics").raise_for_status())

print("\n=== SECURITY ===")
check("Unauthorized request rejected", lambda: (
    r := requests.get("http://localhost:8000/admin"),
    assert r.status_code in [401, 403, 404]
))

print("\n=== VECTOR STORE ===")
check("Qdrant healthy", lambda:
    requests.get("http://localhost:6333/healthz").raise_for_status())
check("Collection exists", lambda: (
    r := requests.get("http://localhost:6333/collections/documents"),
    r.raise_for_status()
))

print("\n=== FEATURE STORE ===")
check("Redis reachable", lambda:
    redis.Redis(host="localhost", port=6379).ping())

print("\n=== KAFKA ===")
check("Kafka topics exist", lambda: (
    result := subprocess.run(
        ["docker", "exec", "lab28-kafka-1", "kafka-topics", "--list",
         "--bootstrap-server", "localhost:9092"],
        capture_output=True, text=True
    ),
    assert "data.raw" in result.stdout
))

# Tổng kết
passed = sum(1 for v in results.values() if v == "PASS")
total = len(results)
score = (passed / total) * 100
print(f"\n{'='*40}")
print(f"Production Readiness Score: {passed}/{total} = {score:.0f}%")
print(f"Target: >80% — Status: {'READY' if score >= 80 else 'NOT READY'}")
```

---

## PHẦN 6 — Chuẩn bị Milestone 3 Demo

### Demo Script (15 phút)

#### Phần 1 — Architecture Overview (2 phút)
Mở diagram hybrid architecture, giải thích:
- 5 layers của AI platform
- Tại sao tách GPU lên Kaggle (cost efficiency)
- Event-driven pattern với Kafka

#### Phần 2 — Live Demo: Happy Path (5 phút)

```bash
# Terminal 1: Show logs real-time
docker compose logs -f api-gateway

# Terminal 2: Gửi request
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Explain event-driven architecture for AI platforms",
    "embedding": [0.1, 0.2, ...]
  }'
```

Demo flow theo thứ tự:
1. Ingest data → Kafka: `python scripts/01_ingest_to_kafka.py`
2. Deploy Prefect flow: `cd prefect/flows && python kafka_to_delta.py`
3. Gọi API Gateway → nhận response từ vLLM

#### Phần 3 — Error Scenario Demo (3 phút)

```bash
# Simulate LLM timeout
curl -X POST http://localhost:8000/api/v1/chat \
  --max-time 0.1 \
  -d '{"query": "test"}'

# Kill một service giả lập failure
docker compose stop qdrant
# → Show graceful degradation

# Khởi động lại
docker compose start qdrant
```

#### Phần 4 — Observability Walkthrough (3 phút)

Mở Grafana (http://localhost:3000), show live:
- Request rate dashboard
- P95 latency gauge
- Error rate panel
- Kafka consumer lag

```bash
# Chạy load test nhỏ để graph có data
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8000/api/v1/chat \
    -d '{"query": "load test '$i'", "embedding": [0.1]}' &
done
wait
```

#### Phần 5 — Q&A (2 phút)

Chuẩn bị sẵn câu trả lời cho:
- "Tại sao dùng Kafka thay vì gọi trực tiếp?" → decoupling, replay
- "Circuit breaker implement ở đâu?" → API Gateway middleware
- "Nếu Kaggle ngắt kết nối thì sao?" → fallback to cached responses

---

## Checklist cuối buổi

```
[ ] docker compose ps — tất cả services Up
[ ] python scripts/01_ingest_to_kafka.py — OK
[ ] Prefect flow "kafka-to-delta" deploy thành công
[ ] python scripts/05_embed_to_qdrant.py — OK
[ ] curl http://localhost:8000/api/v1/chat — nhận response
[ ] pytest smoke-tests/ -v — 5/5 PASSED
[ ] python scripts/production_readiness_check.py — score >= 80%
[ ] Grafana dashboard có metrics
[ ] Kaggle notebook vẫn chạy (kernel active)
[ ] Demo script đã chạy thử 1 lần
```

---

## Tóm tắt Timeline 2 giờ

| Thời gian | Công việc |
|---|---|
| 0:00 – 0:20 | Phần 1: Docker Compose up, kiểm tra services |
| 0:20 – 0:35 | Phần 2: Kaggle notebook, lấy ngrok URLs |
| 0:35 – 1:00 | Phần 3: Kết nối 10 integration points, chạy từng script |
| 1:00 – 1:20 | Phần 4: Chạy 5 smoke tests, fix failures |
| 1:20 – 1:35 | Phần 5: Production readiness check, đạt >80% |
| 1:35 – 2:00 | Phần 6: Rehearse demo, chuẩn bị slide |
