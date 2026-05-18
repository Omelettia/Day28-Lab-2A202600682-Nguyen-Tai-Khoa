# Lab #28 — Full Platform Integration Sprint

AI platform với kiến trúc hybrid (Local + Kaggle GPU) sử dụng Prefect, Kafka, Qdrant, Prometheus, Grafana.

## Kiến trúc

```
Local (Docker Compose):
  Kafka → Prefect → Delta Lake → Feast (Redis)
  ↓                ↓
  Qdrant         API Gateway (FastAPI)
  ↓                ↓
  Prometheus ← Grafana
  ↓
  LangSmith tracing

Kaggle (GPU T4/P100):
  vLLM serving
  Embedding service
  MLflow tracking
```

## Yêu cầu

- Docker Desktop đang chạy
- Python 3.10+
- Tài khoản Kaggle với GPU đã bật
- **Tunnel service** (chọn 1 trong 2):
  - `ngrok` đã cài và token configured
  - HOẶC `cloudflared` đã cài (`brew install cloudflare/cloudflare/cloudflared`)

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

### 2. Setup Kaggle GPU

Tạo Kaggle Notebook với GPU T4 x2, chạy:

**Option A: Dùng ngrok**

```python
# Cell 1: Install dependencies
!pip install -q vllm fastapi uvicorn pyngrok mlflow sentence-transformers

# Cell 2: Setup ngrok
from pyngrok import ngrok
ngrok.set_auth_token("YOUR_NGROK_TOKEN")  # lấy tại ngrok.com

# Cell 3: Start vLLM server
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
time.sleep(60)
print("vLLM server started")

# Cell 4: Create ngrok tunnel
tunnel = ngrok.connect(8001, "http")
print(f"vLLM URL: {tunnel.public_url}")
```

**Option B: Dùng cloudflared**

```python
# Cell 1: Install dependencies
!pip install -q vllm fastapi uvicorn cloudflared mlflow sentence-transformers

# Cell 2: Start vLLM server
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
time.sleep(60)
print("vLLM server started")

# Cell 3: Create cloudflare tunnel
import subprocess
tunnel = subprocess.run(["cloudflared", "tunnel", "--url", "http://localhost:8001"], capture_output=True, text=True)
print(tunnel.stdout)  # URL sẽ hiển thị
```

### 3. Cập nhật Environment Variables

```bash
# Copy và chỉnh sửa file .env
cp .env.example .env
# Thay VLLM_NGROK_URL với URL từ Kaggle (ngrok hoặc cloudflared)
# Thay EMBED_NGROK_URL nếu có embedding service
# Thay LANGCHAIN_API_KEY với key của bạn
```

### 4. Deploy Prefect Flows

```bash
cd prefect/flows
pip install -r requirements.txt
python kafka_to_delta.py
```

### 5. Ingest Data vào Kafka

```bash
cd ../..
python scripts/01_ingest_to_kafka.py
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
