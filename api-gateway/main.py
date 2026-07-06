# api-gateway/main.py
import os
import time

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field
from prometheus_fastapi_instrumentator import Instrumentator

# Integration 10: LangSmith tracing. `@traceable` sends a run to LangSmith when
# LANGCHAIN_TRACING_V2/LANGCHAIN_API_KEY are set, and is a no-op otherwise.
try:
    from langsmith import traceable
except Exception:  # pragma: no cover - defensive: keep gateway up if langsmith missing
    def traceable(*d_args, **d_kwargs):
        if d_args and callable(d_args[0]):
            return d_args[0]
        return lambda fn: fn

app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)  # Integration 9: Prometheus /metrics

VLLM_URL = os.environ.get("VLLM_URL", "http://ollama:11434/v1").rstrip("/")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333").rstrip("/")
MODEL_NAME = os.environ.get("MODEL_NAME", "qwen2.5:3b")
# Optional bearer token for hosted OpenAI-compatible APIs (Groq, OpenRouter, ...).
# Left unset for local Ollama, which needs no auth.
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
# Cap generation so latency is bounded and predictable (keeps the P95 SLO on a
# small local GPU); tune per model/hardware via LLM_MAX_TOKENS.
LLM_MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", "160"))


class ChatRequest(BaseModel):
    """Pydantic validation → missing `query` returns 422, not a 500."""
    query: str
    embedding: list[float] = Field(default_factory=lambda: [0.0] * 384)


async def vector_search(embedding: list[float]) -> list:
    """Integration 8a: retrieve context from Qdrant.

    Graceful degradation: if Qdrant is unreachable, fall back to no context
    instead of failing the whole request.
    """
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                f"{QDRANT_URL}/collections/documents/points/search",
                json={"vector": embedding, "limit": 3},
            )
            resp.raise_for_status()
            return resp.json().get("result", [])
    except Exception:
        return []


@traceable(name="llm_inference", run_type="llm")
async def llm_inference(prompt: str) -> dict:
    """Integration 8b + 10: call the OpenAI-compatible LLM (traced to LangSmith).

    Works with local Ollama or any hosted provider (Groq, OpenRouter, ...).
    """
    headers = {"Authorization": f"Bearer {LLM_API_KEY}"} if LLM_API_KEY else {}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{VLLM_URL}/chat/completions",
            headers=headers,
            json={
                "model": MODEL_NAME,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": LLM_MAX_TOKENS,
            },
        )
        resp.raise_for_status()
        return resp.json()


@app.post("/api/v1/chat")
async def chat(req: ChatRequest):
    start = time.time()

    context = await vector_search(req.embedding)
    prompt = f"Context: {context}\n\nQuery: {req.query}"

    try:
        result = await llm_inference(prompt)
        answer = result["choices"][0]["message"]["content"]
        model = result.get("model", MODEL_NAME)
        degraded = False
    except Exception:
        # Graceful degradation: LLM/tunnel down → safe fallback, never a 500.
        answer = (
            "The language model is temporarily unavailable. "
            "Your request was received; please retry shortly."
        )
        model = "fallback"
        degraded = True

    latency = round((time.time() - start) * 1000, 2)
    return {
        "answer": answer,
        "latency_ms": latency,
        "model": model,
        "degraded": degraded,
        "context_docs": len(context),
    }


@app.get("/health")
def health():
    return {"status": "ok"}
