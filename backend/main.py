import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

from dotenv import load_dotenv

# Load .env from this file's directory before anything else
load_dotenv(Path(__file__).parent / ".env")

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from db.kuzu_client import dedupe_pr_reference_edges, get_connection, init_kuzu, init_schema
from db.chroma_client import init_chroma, get_collection
from db.sqlite_client import init_sqlite, init_tables, scrub_sensitive_session_configs
from state.session import SessionManager

logger = logging.getLogger(__name__)

state: dict = {}

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    state["kuzu_db"] = init_kuzu()
    kuzu_conn = get_connection(state["kuzu_db"])
    init_schema(kuzu_conn)
    deduped_refs = dedupe_pr_reference_edges(kuzu_conn)
    if deduped_refs:
        logger.info("Kuzu migration: deduplicated %d REFERENCES edges", deduped_refs)
    state["kuzu_conn"] = kuzu_conn

    state["chroma"] = init_chroma()
    state["chroma_col"] = get_collection("gitlore-main")

    sqlite_conn = init_sqlite()
    init_tables(sqlite_conn)
    scrubbed_configs = scrub_sensitive_session_configs(sqlite_conn)
    if scrubbed_configs:
        logger.info("SQLite migration: scrubbed %d session configs", scrubbed_configs)
    state["sqlite"] = sqlite_conn
    state["session_mgr"] = SessionManager(sqlite_conn)
    state["progress_queue"] = asyncio.Queue()
    state["ingest_task"] = None

    # Lazy-init heavy retrieval components on first use
    state["embedder"] = None
    state["reranker"] = None
    state["query_contexts"] = {}

    yield

    if "sqlite" in state:
        state["sqlite"].close()


app = FastAPI(title="Gitlore API", lifespan=lifespan)


def _cors_allow_origins() -> list[str]:
    frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:5173")
    origins = [o.strip() for o in frontend_url.split(",") if o.strip()]
    return origins or ["http://localhost:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allow_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_embedder():
    if state["embedder"] is None:
        from ingestion.embedder import Embedder
        state["embedder"] = Embedder(state["chroma_col"])
    return state["embedder"]


def _get_reranker():
    if state["reranker"] is None:
        from retrieval.reranker import Reranker
        state["reranker"] = Reranker()
    return state["reranker"]


def _parse_ingest_date(
    raw_value: str | None,
    field_name: str,
    default: datetime,
    *,
    end_of_day_if_date_only: bool = False,
) -> datetime:
    if raw_value is None or not raw_value.strip():
        return default

    value = raw_value.strip()
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{field_name} must be an ISO-8601 date/datetime "
                "(for example: 2026-04-10 or 2026-04-10T15:30:00+00:00)"
            ),
        ) from exc

    if end_of_day_if_date_only and "T" not in value:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _sanitize_config_for_storage(config: dict) -> dict:
    sanitized = dict(config)
    sanitized.pop("github_token", None)
    return sanitized


def _store_query_context(query_id: str, context_items: list[dict]) -> None:
    contexts: dict[str, dict[str, dict]] = state.setdefault("query_contexts", {})
    contexts[query_id] = {
        item["source_id"]: item
        for item in context_items
        if "source_id" in item
    }

    max_contexts = 100
    while len(contexts) > max_contexts:
        oldest_key = next(iter(contexts))
        contexts.pop(oldest_key, None)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "kuzu": state.get("kuzu_conn") is not None,
        "chroma": state.get("chroma") is not None,
        "sqlite": state.get("sqlite") is not None,
    }


# ---------------------------------------------------------------------------
# Ingestion routes
# ---------------------------------------------------------------------------

class IngestRequest(BaseModel):
    repo_url: str
    start_date: str | None = None
    end_date: str | None = None
    github_token: str | None = None
    subdir: str | None = None


async def _run_ingestion(config: dict, session_id: str) -> None:
    from ingestion.orchestrator import IngestionOrchestrator
    session_mgr: SessionManager = state["session_mgr"]
    queue: asyncio.Queue = state["progress_queue"]
    orch = IngestionOrchestrator(
        kuzu_conn=state["kuzu_conn"],
        chroma_collection=state["chroma_col"],
        sqlite_conn=state["sqlite"],
        session_manager=session_mgr,
    )
    try:
        await orch.run({**config, "_session_id": session_id}, queue)
        session_mgr.update_status(session_id, "complete")
    except asyncio.CancelledError:
        session_mgr.update_status(session_id, "cancelled")
    except Exception:
        session_mgr.update_status(session_id, "error")
    finally:
        state["ingest_task"] = None


@app.post("/ingest/start")
async def ingest_start(req: IngestRequest):
    session_mgr: SessionManager = state["session_mgr"]

    repo_url = req.repo_url.strip()
    if not repo_url:
        raise HTTPException(status_code=400, detail="repo_url must not be empty")

    current = session_mgr.get_current_session()
    if current and current["status"] == "running":
        raise HTTPException(
            status_code=409,
            detail=f"Ingestion already running (session {current['session_id']}). POST /ingest/cancel first.",
        )

    start_dt = _parse_ingest_date(
        req.start_date,
        "start_date",
        datetime(1970, 1, 1, tzinfo=timezone.utc),
    )
    end_dt = _parse_ingest_date(
        req.end_date,
        "end_date",
        datetime.now(tz=timezone.utc),
        end_of_day_if_date_only=True,
    )
    if start_dt > end_dt:
        raise HTTPException(
            status_code=400,
            detail="start_date must be earlier than or equal to end_date",
        )

    config = req.model_dump()
    config["repo_url"] = repo_url
    config["start_date"] = start_dt.isoformat()
    config["end_date"] = end_dt.isoformat()

    session_id = session_mgr.create_session(
        repo_url,
        _sanitize_config_for_storage(config),
    )
    q: asyncio.Queue = state["progress_queue"]
    while not q.empty():
        q.get_nowait()
    task = asyncio.create_task(_run_ingestion(config, session_id))
    state["ingest_task"] = task
    return {"session_id": session_id, "status": "started"}


@app.get("/ingest/progress")
async def ingest_progress():
    async def _stream() -> AsyncGenerator[str, None]:
        q: asyncio.Queue = state["progress_queue"]
        yield ": keep-alive\n\n"
        while True:
            try:
                event: dict = await asyncio.wait_for(q.get(), timeout=30.0)
            except asyncio.TimeoutError:
                yield ": keep-alive\n\n"
                continue
            yield f"data: {json.dumps(event)}\n\n"
            if event.get("stage") in ("complete", "error"):
                break

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/ingest/status")
def ingest_status():
    session_mgr: SessionManager = state["session_mgr"]
    current = session_mgr.get_current_session()
    if current is None:
        return {"status": "idle", "session": None}
    return {"status": current["status"], "session": current}


@app.post("/ingest/cancel")
async def ingest_cancel():
    session_mgr: SessionManager = state["session_mgr"]
    cancelled = session_mgr.cancel_current()
    task = state.get("ingest_task")
    if task and not task.done():
        task.cancel()
    return {"cancelled": cancelled}


# ---------------------------------------------------------------------------
# Query routes
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str


@app.post("/query")
async def query_endpoint(req: QueryRequest, request: Request):
    question = req.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="question must not be empty")

    groq_key = os.environ.get("GROQ_API_KEY", "")

    async def _stream() -> AsyncGenerator[str, None]:
        loop = asyncio.get_event_loop()
        # Yield an empty comment immediately so the client sees HTTP 200 / headers
        yield ": start\n\n"

        try:
            if not groq_key:
                yield f"data: {json.dumps({'type': 'error', 'message': 'GROQ_API_KEY not set — set it in your environment and restart the server'})}\n\n"
                return

            # -- Classify -------------------------------------------------------
            from retrieval.classifier import QueryClassifier
            classification = QueryClassifier().classify(question)
            entities = classification["entities"]
            keywords = classification["keywords"]
            q_type = classification["type"]

            # -- Retrieve (all three paths in parallel) -------------------------
            from retrieval.graph_retriever import GraphRetriever
            from retrieval.vector_retriever import VectorRetriever
            from retrieval.bm25_retriever import BM25Retriever
            from state.bm25_index import BM25Index

            graph_ret = GraphRetriever(state["kuzu_conn"])
            embedder = _get_embedder()
            vector_ret = VectorRetriever(embedder)

            bm25_idx = BM25Index(state["sqlite"])
            bm25_idx.load()
            bm25_ret = BM25Retriever(bm25_idx)

            graph_results, vector_results, bm25_results = await asyncio.gather(
                loop.run_in_executor(None, graph_ret.retrieve, entities, q_type, 15),
                loop.run_in_executor(None, vector_ret.retrieve, question, 20),
                loop.run_in_executor(None, bm25_ret.retrieve, keywords + entities, 10),
            )
            all_candidates = graph_results + vector_results + bm25_results

            # -- Rerank ---------------------------------------------------------
            reranker = _get_reranker()
            context_items = await loop.run_in_executor(
                None, reranker.rerank, question, all_candidates, 8
            )
            query_id = str(uuid.uuid4())
            _store_query_context(query_id, context_items)

            # -- Build prompt ---------------------------------------------------
            from llm.prompt_builder import PromptBuilder
            messages = PromptBuilder().build(question, context_items)

            # -- Stream LLM -----------------------------------------------------
            from llm.groq_client import GroqClient
            from llm.citation_extractor import CitationExtractor

            full_answer: list[str] = []
            client_disconnected = False
            async for token in GroqClient(api_key=groq_key).stream(messages):
                if await request.is_disconnected():
                    client_disconnected = True
                    logger.info("Query stream client disconnected before completion")
                    break
                full_answer.append(token)
                yield f"data: {json.dumps({'type': 'token', 'text': token})}\n\n"

            if client_disconnected:
                return

            # -- Citations + persist --------------------------------------------
            answer_text = "".join(full_answer)
            result = CitationExtractor().extract(answer_text, context_items)

            try:
                session = state["session_mgr"].get_current_session()
                sid = session["session_id"] if session else "no-session"
                state["sqlite"].execute(
                    "INSERT INTO chat_history (session_id, role, content) VALUES (?, ?, ?)",
                    (sid, "user", question),
                )
                state["sqlite"].execute(
                    "INSERT INTO chat_history (session_id, role, content) VALUES (?, ?, ?)",
                    (sid, "assistant", answer_text),
                )
                state["sqlite"].commit()
            except Exception as exc:
                logger.warning("Failed to persist chat history: %s", exc)

            yield f"data: {json.dumps({'type': 'done', 'query_id': query_id, 'citations': result['citations']})}\n\n"

        except Exception as exc:
            logger.exception("Query pipeline error: %s", exc)
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/query/history")
def query_history():
    conn = state["sqlite"]
    rows = conn.execute(
        "SELECT id, session_id, role, content, created_at "
        "FROM chat_history ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    return [
        {
            "id": r[0],
            "session_id": r[1],
            "role": r[2],
            "content": r[3],
            "timestamp": r[4],
            "created_at": r[4],
        }
        for r in rows
    ]


@app.get("/query/sources/{source_id}")
def query_source(
    source_id: str,
    query_id: str = Query(..., min_length=1),
):
    contexts = state.get("query_contexts", {})
    ctx = contexts.get(query_id)
    if ctx is None:
        raise HTTPException(
            status_code=404,
            detail=f"Query context '{query_id}' not found",
        )

    item = ctx.get(source_id)
    if item is None:
        raise HTTPException(
            status_code=404,
            detail=f"Source '{source_id}' not found in query context '{query_id}'",
        )
    return {"source_id": source_id, "type": item.get("type"), "content": item.get("content"), "metadata": item.get("metadata")}
