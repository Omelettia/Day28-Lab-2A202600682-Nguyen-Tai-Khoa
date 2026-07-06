# Lab #28 — Trả Lời 5 Câu Hỏi Nộp Bài

Các câu trả lời dưới đây bám sát kiến trúc thực tế trong repo này
(`docker-compose.yml`, `api-gateway/main.py`, `prefect/flows/kafka_to_delta.py`,
`monitoring/prometheus.yml`).

---

## 1. Trade-offs trong thiết kế: cân bằng performance, reliability, maintainability

**Quyết định kiến trúc lớn nhất là serving LLM bằng Ollama chạy local (GPU trên máy)
thay vì kiến trúc hybrid với GPU cloud/Kaggle.**

- **Performance ↔ Cost/Complexity:** Chọn model nhỏ (`qwen2.5:3b`) chạy trên GPU local
  6GB qua Ollama (endpoint OpenAI-compatible). Đánh đổi: chất lượng/độ dài context thấp
  hơn model 7B–70B, nhưng bỏ được hoàn toàn tunnel (ngrok/cloudflared), tài khoản cloud,
  và điểm chết mạng. SLO smoke test đặt ở **10 s** (`smoke-tests/test_e2e.py`) thay vì 2 s
  để bao được cả cold-start/CPU fallback — phản ánh thực tế, không phải con số lý tưởng.
  Gateway để `MODEL_NAME`/`VLLM_URL`/`LLM_API_KEY` là biến môi trường nên có thể đổi sang
  API hosted (Groq, OpenRouter...) mà không sửa code.
- **Reliability ↔ Simplicity:** API Gateway (`api-gateway/main.py`) tách vector search
  và LLM inference thành 2 hàm riêng, mỗi hàm có timeout và fallback độc lập. Đánh đổi:
  code phức tạp hơn một chút, nhưng một thành phần chết không kéo sập cả request.
- **Maintainability:** Toàn bộ hạ tầng khai báo trong một `docker-compose.yml` (GitOps —
  config nằm trong git, tái tạo bằng `docker compose up -d`). Version được pin trong
  `requirements.txt` để build tái lập được. Đánh đổi: pin version cần bảo trì khi nâng cấp,
  nhưng tránh được "works on my machine".

**Cân bằng tổng thể:** ưu tiên *reliability* (fallback ở mọi integration point) và
*maintainability* (khai báo, pin version) hơn là tối ưu *latency* tuyệt đối, vì đây là
platform tích hợp — độ ổn định end-to-end quan trọng hơn vài trăm ms.

---

## 2. Xử lý ngắt kết nối tới LLM service & cơ chế fallback

> Bài gốc gợi ý kiến trúc hybrid (Local + Kaggle). Mình chọn phương án **fully-local
> (Ollama)** để loại bỏ điểm yếu lớn nhất của hybrid — tunnel/kernel Kaggle hay chết và
> đổi URL. Dù local, LLM vẫn là một *service riêng* (`ollama`) nên nguyên tắc cô lập lỗi
> và fallback vẫn áp dụng y hệt như khi gọi một GPU ở xa.

1. **Ranh giới lỗi rõ ràng:** Gateway chỉ gọi LLM qua HTTP tại một điểm duy nhất —
   `VLLM_URL` (mặc định `http://ollama:11434/v1`). Đổi sang API hosted chỉ là đổi biến
   môi trường, không đổi code.
2. **Fallback ở API Gateway:** `llm_inference()` được bọc trong `try/except`. Khi Ollama
   chưa pull model, đang cold-start, hoặc timeout (30 s), gateway trả về **HTTP 200** với
   câu trả lời fallback và cờ `"degraded": true` thay vì 500 — đã kiểm chứng thực tế
   (LLM unreachable → `{"model":"fallback","degraded":true}`).
3. **Timeout có giới hạn:** 30 s cho LLM, 5 s cho Qdrant → một backend treo không giữ kết
   nối vô hạn, thread pool của gateway không cạn.
4. **Khôi phục:** Ollama là service trong compose với volume `ollama_data` giữ model đã
   pull, nên `docker compose restart ollama` phục hồi nhanh mà không mất model. Không phụ
   thuộc tài khoản/tunnel bên ngoài.

Hạn chế còn lại (hướng cải thiện): fallback hiện là câu trả lời tĩnh; production nên thêm
cache câu trả lời gần nhất hoặc một model dự phòng nhỏ hơn.

---

## 3. Event-driven architecture với Kafka giúp decouple components

Kafka (topic `data.raw`) nằm giữa **producer** (`scripts/01_ingest_to_kafka.py`) và
**consumer** (`prefect/flows/kafka_to_delta.py`). Lợi ích decouple:

- **Decouple theo thời gian:** producer ghi vào Kafka rồi thoát ngay; nó không cần
  Prefect/Delta Lake đang chạy. Consumer xử lý sau, theo lịch (`cron */5 * * * *`).
- **Decouple theo nhịp độ (back-pressure):** nếu pipeline xử lý chậm, message tồn trong
  Kafka chứ không làm nghẽn nguồn ingest. `consumer_timeout_ms=5000` cho phép batch.
- **Decouple theo consumer:** nhiều consumer group có thể đọc cùng `data.raw` độc lập
  (ví dụ một nhánh đẩy vào Delta Lake, một nhánh khác đẩy embedding) mà không sửa producer.
- **Replay:** `auto_offset_reset="earliest"` cho phép đọc lại toàn bộ lịch sử để backfill
  hay debug — điều không làm được nếu gọi hàm trực tiếp.

> **Chi tiết cấu hình quan trọng:** Kafka dùng **dual listener** — host truy cập qua
> `localhost:9092` (EXTERNAL), còn các container (Prefect worker) qua `kafka:29092`
> (INTERNAL). Nếu chỉ advertise `localhost:9092`, worker sau bước bootstrap sẽ cố kết nối
> `localhost` *bên trong container của chính nó* và thất bại — pipeline âm thầm consume 0
> record. Đây là lỗi đã được sửa trong `docker-compose.yml`.

---

## 4. Observability: logs, metrics, traces

Ba trụ cột observability, tất cả xem được qua UI:

- **Metrics (Prometheus + Grafana):** `prometheus-fastapi-instrumentator` expose
  `/metrics` trên API Gateway (request count, latency histogram, status code).
  Prometheus (`monitoring/prometheus.yml`) scrape `api-gateway:8000` mỗi 15 s và tự
  giám sát chính nó. Grafana (:3000) vẽ request rate, P95 latency, error rate.
  *Lưu ý trung thực:* Kafka và Prefect **không** expose `/metrics` mặc định nên đã bỏ
  khỏi scrape config để tránh target "down" giả — muốn giám sát chúng cần thêm
  kafka-exporter / JMX exporter sidecar.
- **Traces (LangSmith):** hàm `llm_inference()` được gắn decorator `@traceable`. Khi
  `LANGCHAIN_TRACING_V2=true` và `LANGCHAIN_API_KEY` được set (truyền vào container qua
  `docker-compose.yml`), mỗi lần gọi LLM sinh một run trong project `lab28-platform` —
  xem được input/output/latency trên LangSmith. `scripts/09_verify_observability.py`
  kiểm tra cả Prometheus lẫn LangSmith có dữ liệu.
- **Logs:** log tập trung qua `docker compose logs -f <service>` (stdout/stderr của từng
  service). Prefect UI (:4200) hiển thị log theo từng flow run và task.

Coverage: request-level (metrics), inference-level (traces), pipeline-level (Prefect run
logs) — đủ để truy vết một request từ gateway → LLM và một message từ Kafka → Delta Lake.

---

## 5. Nếu Qdrant hoặc Kafka crash: graceful degradation

Hệ thống được thiết kế để **suy giảm chức năng chứ không sập toàn bộ**:

**Qdrant crash (vector store):**
- `vector_search()` trong gateway bọc `try/except`, timeout 5 s. Qdrant chết → trả về
  context rỗng (`[]`) và request **vẫn tiếp tục** tới LLM, chỉ mất phần retrieval.
- Response ghi `"context_docs": 0` để client/observability biết đang chạy suy giảm.
- Đã kiểm chứng: chạy gateway với Qdrant unreachable → HTTP 200, không 500.

**Kafka crash (event bus):**
- Producer (`01_ingest_to_kafka.py`) sẽ báo lỗi khi gửi — dữ liệu chưa mất, có thể gửi
  lại sau khi Kafka phục hồi (không có ghi nhận sai vào downstream).
- Consumer (Prefect flow) đơn giản là consume 0 record trong lần chạy đó
  (`consumer_timeout_ms=5000`) và kết thúc sạch; lần chạy theo lịch kế tiếp sẽ bắt kịp nhờ
  `auto_offset_reset="earliest"` (message vẫn nằm trong Kafka sau khi khôi phục).
- Vì Kafka nằm giữa ingest và xử lý, Kafka sập **không** ảnh hưởng đường phục vụ
  (serving path) — API Gateway vẫn trả lời request bình thường.

**LLM (Ollama) crash / chưa pull model:** đã mô tả ở câu 2 — fallback response +
`"degraded": true`, không 500.

**Nguyên tắc chung:** mỗi integration point có timeout + `try/except`, và các thành phần
được phân tách qua ranh giới rõ (Kafka giữa ingest↔xử lý; biến môi trường `VLLM_URL` giữa
gateway↔LLM service) nên lỗi một chỗ được cô lập, không lan ra toàn hệ thống.

**Demo minh hoạ (khớp `LAB28.md` Phần 3):**
```bash
docker compose stop qdrant          # gateway vẫn trả lời, context_docs=0
docker compose start qdrant         # tự phục hồi retrieval
```
