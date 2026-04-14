# Gitlore Backend

FastAPI server powering the Gitlore RAG pipeline.

## Setup

### 1. Install dependencies

```bash
cd backend
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

Copy and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```
GROQ_API_KEY=gsk_...         # Required: Groq API key for LLM streaming
GITHUB_TOKEN=ghp_...         # Optional: GitHub PAT for PR/issue ingestion (60 req/hr without)
FRONTEND_URL=http://localhost:5173  # Optional: CORS origin(s), comma-separated
```

### 3. Run the server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The server starts at `http://localhost:8000`. Data files are written to `./data/` (created automatically).

---

## API Endpoints

### Health

| Method | Path      | Description                                      |
|--------|-----------|--------------------------------------------------|
| GET    | `/health` | Returns `{"status":"ok"}` and DB connection flags |

### Ingestion

| Method | Path               | Description                                                         |
|--------|--------------------|---------------------------------------------------------------------|
| POST   | `/ingest/start`    | Start ingestion. Body: `{repo_url, start_date?, end_date?, github_token?, subdir?}` |
| GET    | `/ingest/progress` | Server-Sent Events stream of ingestion progress events              |
| GET    | `/ingest/status`   | Current session status (`idle` / `running` / `complete` / `error`) |
| POST   | `/ingest/cancel`   | Cancel a running ingestion task                                     |

### Query

| Method | Path                          | Description                                              |
|--------|-------------------------------|----------------------------------------------------------|
| POST   | `/query`                      | Stream RAG answer. Body: `{question: string}`            |
| GET    | `/query/history`              | Last 20 chat messages across all sessions                |
| GET    | `/query/sources/{source_id}`  | Full content of a cited source. Query param: `query_id`  |

### Notes

- `/ingest/progress` and `/query` both return `text/event-stream` responses — connect with `EventSource` or `fetch` in streaming mode.
- `GROQ_API_KEY` must be set or `/query` will return an error event instead of an answer.
- Database files are stored under `backend/data/` and are excluded from version control.
