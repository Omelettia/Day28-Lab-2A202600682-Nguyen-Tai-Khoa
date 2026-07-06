# Lab #28 — Full Platform Integration Sprint

AI platform tích hợp full-stack **chạy hoàn toàn local** (không cần GPU cloud/Kaggle)
dùng Prefect, Kafka, Qdrant, Prometheus, Grafana, và Ollama cho LLM serving.

## Kiến trúc

```
Docker Compose (local):
  Kafka → Prefect → Delta Lake → Feast (Redis)
  ↓                ↓
  Qdrant         API Gateway (FastAPI)
  ↓                ↓            ↓
  Prometheus ← Grafana     Ollama (LLM, GPU) — OpenAI-compatible
  ↓
  LangSmith tracing

Embeddings: fastembed (local, CPU) — 384-dim BAAI/bge-small-en-v1.5
```

## Yêu cầu

- Docker (đang chạy)
- Python 3.10+ (cho các script chạy trên host)
- GPU NVIDIA + nvidia-container-toolkit (khuyến nghị, để Ollama chạy nhanh) —
  vẫn chạy được trên CPU nếu không có GPU, chỉ chậm hơn
- (Tùy chọn) LangSmith API key cho tracing

## Quick Start

### 1. Khởi động Local Stack

```bash
cd lab28
docker compose up -d
docker compose ps  # Kiểm tra tất cả services Up
```

**Services:**
- Prefect UI: http://localhost:4200
- Grafana: http://localhost:3000 (admin/admin)
- Qdrant: http://localhost:6333/dashboard
- Prometheus: http://localhost:9090
- API Gateway: http://localhost:8000

### 2. Setup LLM (Ollama, local)

Ollama đã nằm sẵn trong `docker-compose.yml` (tự dùng GPU nếu máy có). Sau khi stack Up,
pull model một lần:

```bash
# ~2GB, vừa với GPU 6GB
docker compose exec ollama ollama pull qwen2.5:3b

# Kiểm tra endpoint OpenAI-compatible
curl http://localhost:11434/v1/models
```

API Gateway tự trỏ tới Ollama qua `VLLM_URL=http://ollama:11434/v1` (mặc định trong
compose) — không cần ngrok/tunnel, không cần tài khoản cloud.

**Không có GPU?** Ollama vẫn chạy trên CPU — dùng model nhỏ hơn cho nhanh:

```bash
docker compose exec ollama ollama pull qwen2.5:1.5b
# rồi đặt MODEL_NAME=qwen2.5:1.5b trong .env
```

**Muốn dùng API hosted thay vì local?** Xem `.env.example`: đặt `VLLM_NGROK_URL`,
`MODEL_NAME`, `LLM_API_KEY` (ví dụ Groq) — gateway hỗ trợ mọi endpoint OpenAI-compatible.

### 3. Cập nhật Environment Variables

```bash
# Copy và chỉnh sửa file .env
cp .env.example .env
# Để trống VLLM_NGROK_URL để dùng Ollama local (mặc định).
# Thay LANGCHAIN_API_KEY với key của bạn (cho tracing).
# (Không cần EMBED_NGROK_URL nữa — embeddings chạy local bằng fastembed.)

# docker compose tự đọc file .env. Với các script chạy trên host,
# load env vào shell trước khi chạy:
set -a; source .env; set +a
```

> Sau khi sửa `.env`, chạy lại `docker compose up -d` để api-gateway nhận
> `VLLM_URL` và các biến LangSmith mới.

### 4. Deploy Prefect Flows

Deploy chạy **bên trong** worker container để đường dẫn code và `PREFECT_API_URL`
khớp nhau (worker đã được mount code tại `/opt/prefect/flows`):

```bash
docker compose exec prefect-worker python /opt/prefect/flows/kafka_to_delta.py
```

Kỳ vọng: `Deployed 'kafka-to-delta' to work pool 'lab28-worker'`. Deployment sẽ
hiện trong Prefect UI (http://localhost:4200) với lịch chạy mỗi 5 phút.

### 5. Ingest Data & Embeddings

```bash
# Cài dependencies cho các script chạy trên host (một lần).
# Khuyến nghị dùng venv:
#   python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# (Trên hệ "externally-managed" — Debian/Ubuntu mới — nếu không dùng venv:
#   pip install --user --break-system-packages -r requirements.txt )

# Integration 1: đẩy sample data vào Kafka
python scripts/01_ingest_to_kafka.py

# Integration 5: embed local (fastembed) và lưu vectors vào Qdrant
python scripts/05_embed_to_qdrant.py
```

### 6. Chạy Smoke Tests

```bash
pytest smoke-tests/ -v
```

Kỳ vọng: 5/5 tests passing

### 7. Production Readiness Check

```bash
python scripts/production_readiness_check.py
```

Kỳ vọng: Score >80%

## Scripts

| Script | Mô tả |
|--------|-------|
| `scripts/01_ingest_to_kafka.py` | Ingest sample data vào Kafka |
| `scripts/03_delta_to_feast.py` | Load từ Delta Lake và push features vào Feast (Redis) |
| `scripts/05_embed_to_qdrant.py` | Embed data và lưu vectors vào Qdrant |
| `scripts/09_verify_observability.py` | Kiểm tra Prometheus metrics và LangSmith traces |
| `scripts/production_readiness_check.py` | Production readiness checklist |

## API Gateway

**Health Check:**
```bash
curl http://localhost:8000/health
```

**Chat Endpoint:**
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is platform engineering?",
    "embedding": [0.1, 0.2, ...]
  }'
```

## Monitoring

- **Grafana Dashboard:** http://localhost:3000
- **Prometheus:** http://localhost:9090
- **Prefect UI:** http://localhost:4200

## Troubleshooting

**Services không start:**
```bash
docker compose logs <service_name>
docker compose down -v
docker compose up -d
```

**Prefect worker không connect:**
```bash
# Check Prefect UI: http://localhost:4200
# Đảm bảo worker đang chạy:
docker compose logs prefect-worker
```

**Kafka consumer lag:**
```bash
# Kiểm tra topic
docker exec lab28-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

## Nộp Bài

Xem `SUBMISSION.md` ở thư mục gốc project.
