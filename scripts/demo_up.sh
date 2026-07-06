#!/usr/bin/env bash
# One-command bring-up: start the full local stack and run every integration
# end-to-end, so the smoke tests and readiness check pass immediately after.
#
#   bash scripts/demo_up.sh
#
# Then capture screenshots and run:
#   pytest smoke-tests/ -v
#   python scripts/production_readiness_check.py
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [ -f .env ] && source .env; set +a
MODEL="${MODEL_NAME:-qwen2.5:3b}"

echo "==> Starting stack (docker compose up -d)..."
docker compose up -d

echo "==> Waiting for core endpoints (gateway / qdrant / grafana)..."
until curl -sf http://localhost:6333/healthz >/dev/null 2>&1 \
   && curl -sf http://localhost:8000/health  >/dev/null 2>&1 \
   && curl -sf http://localhost:3000/api/health >/dev/null 2>&1; do sleep 3; done

echo "==> Ensuring Ollama model '$MODEL' is present (kept in the ollama_data volume)..."
docker compose exec -T ollama ollama pull "$MODEL"

echo "==> Waiting for Prefect worker deps (installs on first start)..."
until docker exec lab28-prefect-worker-1 python -c "import kafka,pandas,pyarrow" 2>/dev/null; do sleep 5; done

echo "==> Integration 1: ingest sample data to Kafka"
python scripts/01_ingest_to_kafka.py

echo "==> Integration 5: embed locally (fastembed) and store vectors in Qdrant"
python scripts/05_embed_to_qdrant.py

echo "==> Integration 2: deploy the flow, then run it once to produce Delta parquet"
docker compose exec -T prefect-worker python /opt/prefect/flows/kafka_to_delta.py
docker compose exec -T -w /opt/prefect/flows prefect-worker \
  python -c "from kafka_to_delta import kafka_to_delta_flow; kafka_to_delta_flow()"

echo "==> Integration 3+4: Delta Lake -> Feast (Redis)"
python scripts/03_delta_to_feast.py

echo "==> Warming the LLM so the first real request is fast"
curl -s -o /dev/null -X POST http://localhost:8000/api/v1/chat \
  -H 'Content-Type: application/json' -d '{"query":"warmup","embedding":[0.1]}' || true

echo ""
echo "==> Ready. Now run:"
echo "     pytest smoke-tests/ -v                        # expect 5/5"
echo "     python scripts/production_readiness_check.py  # expect 100%"
echo "   Dashboards: Prefect :4200  Grafana :3000 (admin/admin)  Qdrant :6333  Prometheus :9090"
