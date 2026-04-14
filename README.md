# Gitlore

> The accumulated knowledge (lore) of a git repository.

Gitlore lets you point at any GitHub repository, ask questions in natural language, and get grounded answers with citations — sourced from actual code, commit history, PR discussions, and documentation. Every engineering team has the same problem: a large codebase that lives mostly in people's heads. `git blame` shows *what* changed but not *why*. Gitlore answers the questions no other tool does: *"Why was the auth system rewritten?"*, *"When did this module stop using mutex locking?"*, *"How does a message flow from producer to consumer?"*

---

## Architecture

The core innovation is a **Temporal Knowledge Graph (TKG)**. Unlike standard RAG (embed → store → retrieve), every edge in the graph carries a timestamp. This means answers respect time — "this function was refactored in March 2024 because of a security audit" is a retrievable fact, not a hallucination.

```
GitHub / local repo
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│  Ingestion Pipeline (7 stages)                               │
│  clone → parse code → parse commits → fetch PRs →           │
│  build TKG → embed → build BM25                             │
└───────────────┬─────────────────┬───────────────────────────┘
                │                 │
       ┌────────▼──────┐  ┌───────▼────────┐
       │  Kuzu (TKG)   │  │   ChromaDB     │  + BM25 (SQLite)
       │  knowledge    │  │   semantic     │
       │  graph        │  │   vectors      │
       └────────┬──────┘  └───────┬────────┘
                │                 │
                └────────┬────────┘
                         ▼
              Hybrid Retriever (parallel)
              + Cross-encoder Reranker (top-8)
                         │
                         ▼
           Groq — llama-3.3-70b-versatile (streaming)
                         │
                         ▼
                  React chat UI
```

**Why hybrid retrieval beats plain vector RAG for codebases:**

| Path | Role | Query types |
|---|---|---|
| TKG traversal | Structural + temporal | why, when, what-changed |
| BM25 | Exact symbol and keyword matching | function names, file names, error strings |
| Vector (ChromaDB) | Semantic / conceptual | explain, describe, overview |
| Cross-encoder reranker | Merges all three, rescores, returns top-8 | All |

---

## Tech Stack

| Layer | Technology |
|---|---|
| API server | FastAPI + Uvicorn |
| Graph DB | Kuzu (embedded, no server) |
| Vector DB | ChromaDB (embedded, no server) |
| Full-text search | BM25Okapi via rank-bm25, serialized to SQLite |
| Embeddings | sentence-transformers / all-MiniLM-L6-v2 (local, CPU) |
| Reranker | cross-encoder / ms-marco-MiniLM-L-6-v2 |
| LLM | Groq API — llama-3.3-70b-versatile |
| Code parsing | tree-sitter (Python, Go, JS, TS, Java, Rust, C, C++) |
| Git history | GitPython |
| GitHub PRs | PyGithub |
| Frontend | React 18 + Vite + Tailwind CSS |

**Key principle:** Fully local. Kuzu, ChromaDB, and SQLite all run embedded inside the FastAPI process. Clone the repo and it works — no Docker, no infrastructure setup required.

---

## Setup

### Prerequisites

- Python 3.11+
- Node.js 18+
- [Groq API key](https://console.groq.com/) — free tier is sufficient
- GitHub personal access token — optional; needed only for PR/issue ingestion

### Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Edit .env:
#   GROQ_API_KEY=gsk_...          (required)
#   GITHUB_TOKEN=ghp_...          (optional — skips PR ingestion if absent)
#   FRONTEND_URL=http://localhost:5173   (optional — CORS origin in production)

uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The server starts at `http://localhost:8000`. The `backend/data/` directory is created automatically and holds all database files.

### Frontend

```bash
cd frontend
npm install

# Optional: override the API URL
cp .env.example .env
# Edit .env:
#   VITE_API_BASE_URL=http://localhost:8000

npm run dev        # development — http://localhost:5173
npm run build      # production build → dist/
npm run preview    # preview production build locally
```

---

## Usage

### 1. Ingest a repository

Open `http://localhost:5173`. On the **Setup** screen:

- Paste a GitHub repo URL — e.g. `https://github.com/etcd-io/etcd`
- Set a **date range** to control ingestion scope (large repos like etcd have 15,000+ commits; a focused range ingests faster and keeps the graph tight)
- Optionally enter your **GitHub token** (enables PR and issue ingestion, which powers "why" answers)
- Click **Start Ingestion**

The **Ingestion Progress** screen streams live updates through 7 stages via SSE:

```
cloning → parsing code → parsing commits → fetching PRs →
building knowledge graph → embedding → building BM25 index
```

### 2. Ask questions

Once ingestion completes, the UI moves to the **Chat** screen. Example questions:

| Question | Primary retrieval path |
|---|---|
| *Why did etcd move from its v2 HTTP API to gRPC in v3?* | TKG → commits → PR discussions |
| *When did the WAL rotation logic change?* | TKG → commit timestamps |
| *What does the BrokerService do?* | Vector + BM25 → code chunks |
| *How does a message flow from producer to consumer?* | TKG + Vector combined |
| *Explain what the raft package is responsible for* | Vector → code + doc chunks |

Answers stream token-by-token. Each answer includes **citation chips** — click one to expand the raw source context (commit message, PR discussion, function body, or file chunk) that grounded the answer.

---

## API Endpoints

### Health
| Method | Path | Description |
|---|---|---|
| GET | `/health` | Returns `{"status":"ok"}` and DB connection flags |

### Ingestion
| Method | Path | Description |
|---|---|---|
| POST | `/ingest/start` | Start ingestion. Body: `{repo_url, start_date?, end_date?, github_token?, subdir?}` |
| GET | `/ingest/progress` | SSE stream of `{stage, message, percent}` events |
| GET | `/ingest/status` | Current session status: `idle / running / complete / error` |
| POST | `/ingest/cancel` | Cancel a running ingestion task |

### Query
| Method | Path | Description |
|---|---|---|
| POST | `/query` | Stream RAG answer. Body: `{question: string}`. Returns SSE: `{type:"token",text}` then `{type:"done",citations}` |
| GET | `/query/history` | Last 20 chat messages across all sessions |
| GET | `/query/sources/{source_id}` | Full content of a cited source. Query param: `query_id` |

Both `/ingest/progress` and `/query` use `text/event-stream` — connect with `EventSource` or `fetch` in streaming mode.

---

## Project Structure

```
gitlore/
├── backend/
│   ├── main.py                   # FastAPI app, all routes, lifespan startup
│   ├── requirements.txt
│   ├── .env                      # secrets — never commit (see .env.example)
│   ├── db/
│   │   ├── kuzu_client.py        # Kuzu init, schema, connection
│   │   ├── chroma_client.py      # ChromaDB persistent client
│   │   └── sqlite_client.py      # SQLite: sessions, chat history, BM25 index
│   ├── ingestion/
│   │   ├── orchestrator.py       # 7-stage async pipeline with SSE progress
│   │   ├── git_parser.py         # GitPython: clone, walk commits, parse file tree
│   │   ├── code_parser.py        # tree-sitter AST: functions, classes, imports
│   │   ├── github_parser.py      # PyGithub: PRs, issues, rate-limit handling
│   │   ├── embedder.py           # Chunking (512t/64t overlap) + MiniLM embeddings
│   │   └── graph_builder.py      # Kuzu MERGE inserts: nodes + timestamped edges
│   ├── retrieval/
│   │   ├── classifier.py         # Rule-based query type + entity extraction
│   │   ├── graph_retriever.py    # Multi-hop TKG traversal (1.0/0.7/0.5 scoring)
│   │   ├── vector_retriever.py   # ChromaDB semantic search
│   │   ├── bm25_retriever.py     # BM25Okapi keyword search
│   │   └── reranker.py           # Cross-encoder dedup + top-k rerank
│   ├── llm/
│   │   ├── groq_client.py        # AsyncGroq streaming wrapper
│   │   ├── prompt_builder.py     # System prompt + context blocks + question
│   │   └── citation_extractor.py # [source_id] → metadata mapping
│   ├── state/
│   │   ├── session.py            # UUID sessions: create/update/cancel
│   │   └── bm25_index.py         # BM25 pickle serialization to SQLite
│   └── data/                     # runtime data — never commit
│       ├── chroma/               # vector embeddings
│       ├── kuzu                  # knowledge graph
│       └── gitlore.db            # sessions + chat history + BM25 index
└── frontend/
    ├── src/
    │   ├── App.jsx               # view router: setup → ingestion → chat
    │   ├── config/api.js         # API base URL from VITE_API_BASE_URL
    │   └── views/
    │       ├── SetupView.jsx     # repo URL, date range, token, subdir form
    │       ├── IngestionView.jsx # 8-stage timeline + live SSE log stream
    │       └── ChatView.jsx      # streaming chat, citation chips, history sidebar
    ├── package.json
    └── vite.config.js
```

---

## TKG Schema

### Nodes
| Node | Properties |
|---|---|
| `File` | `path, language, last_modified` |
| `Function` | `id, name, signature, docstring, file_path, start_line` |
| `Commit` | `hash, short_hash, message, author_name, author_email, timestamp, branch` |
| `PR` | `number, title, description, merged_at, author` |
| `Concept` | `name, description` |

### Edges
| Edge | From → To | Temporal |
|---|---|---|
| `CONTAINS` | File → Function | No |
| `CALLS` | Function → Function | No |
| `MODIFIES` | Commit → File | Yes — commit timestamp |
| `INTRODUCES` | Commit → Function | Yes — when function first appeared |
| `REFERENCES` | PR → Commit | Yes — PR merge timestamp |
| `TAGGED` | File / Function → Concept | No |

To answer *"when did function X change"*: traverse `MODIFIES` edges from Function → Commit, order by timestamp. One hop.
To answer *"why was X changed"*: from those Commits, follow `REFERENCES` to PRs, read PR descriptions. Two hops.

---

## Demo

The recommended demo repo is **[etcd-io/etcd](https://github.com/etcd-io/etcd)** — a Go distributed key-value store with rich PR history going back to 2013, including the famous v2 → v3 architectural migration from HTTP/JSON to gRPC.

**Recommended scope:** date range 2015–2018, Raft subsystem subdirectory.

**Demo question:** *"Why did etcd move from its v2 HTTP API to gRPC in v3?"*

The answer lives entirely in PR discussions and commit messages from 2016–2017. Gitlore retrieves the real architectural decision with citations, demonstrating exactly what the TKG + hybrid RAG pipeline is built for.
