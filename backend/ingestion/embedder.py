import logging
import os
from pathlib import Path
from typing import Any

import numpy as np
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

_MODEL_NAME = "all-MiniLM-L6-v2"


def _words_to_tokens(n_words: int) -> float:
    """Approximate token count from word count (1 token ≈ 0.75 words)."""
    return n_words / 0.75


def _tokens_to_words(n_tokens: int) -> int:
    """Approximate word count from token count (1 token ≈ 0.75 words)."""
    return max(1, int(n_tokens * 0.75))


class Embedder:
    def __init__(self, chroma_collection):
        """
        Parameters
        ----------
        chroma_collection : chromadb.Collection
            A pre-created (or fetched) ChromaDB collection.
        """
        logger.info("Loading sentence-transformer model %s …", _MODEL_NAME)
        self._model = SentenceTransformer(_MODEL_NAME)
        self._col = chroma_collection

    # ------------------------------------------------------------------
    # Text chunking
    # ------------------------------------------------------------------

    def chunk_text(
        self,
        text: str,
        chunk_size: int = 512,
        overlap: int = 64,
    ) -> list[str]:
        """Split *text* into overlapping chunks measured in approximate tokens.

        Uses word-based tokenisation: 1 token ≈ 0.75 words.

        Parameters
        ----------
        chunk_size : int
            Target chunk size in tokens.
        overlap : int
            Overlap between consecutive chunks in tokens.
        """
        if not text or not text.strip():
            return []

        words = text.split()
        if not words:
            return []

        max_words = _tokens_to_words(chunk_size)
        step_words = max(1, _tokens_to_words(chunk_size - overlap))

        chunks: list[str] = []
        start = 0
        while start < len(words):
            end = min(start + max_words, len(words))
            chunks.append(" ".join(words[start:end]))
            if end == len(words):
                break
            start += step_words

        return chunks

    # ------------------------------------------------------------------
    # Embedding helpers
    # ------------------------------------------------------------------

    def _upsert_batch(
        self,
        ids: list[str],
        texts: list[str],
        metadatas: list[dict],
    ) -> None:
        """Embed *texts* and upsert into ChromaDB."""
        if not ids:
            return
        vecs = self._model.encode(texts, show_progress_bar=False)
        self._col.upsert(
            ids=ids,
            documents=texts,
            embeddings=vecs.tolist(),
            metadatas=metadatas,
        )

    @staticmethod
    def _safe_meta(meta: dict) -> dict:
        """ChromaDB metadata values must be str, int, float, or bool."""
        out: dict = {}
        for k, v in meta.items():
            if isinstance(v, (str, int, float, bool)):
                out[k] = v
            elif v is None:
                out[k] = ""
            else:
                out[k] = str(v)
        return out

    # ------------------------------------------------------------------
    # Public embed methods
    # ------------------------------------------------------------------

    def embed_files(self, files: list[dict], repo_path: str = "") -> int:
        """Chunk and embed file contents.

        Each dict must have at least ``path`` and optionally ``content``
        (raw source text).  If ``content`` is absent the file is read
        from disk; paths are resolved relative to *repo_path* if provided.

        Returns total number of chunks embedded.
        """
        total = 0
        for f in files:
            path = f.get("path", "")
            content: str | None = f.get("content")

            if content is None:
                # Try to read from disk
                candidates = [
                    Path(path),
                    Path(repo_path) / path if repo_path else None,
                ]
                for cand in candidates:
                    if cand and cand.exists():
                        try:
                            content = cand.read_text(encoding="utf-8", errors="replace")
                        except OSError:
                            pass
                        break

            if not content:
                logger.debug("Skipping %s — no content available", path)
                continue

            chunks = self.chunk_text(content)
            if not chunks:
                continue

            ids, texts, metas = [], [], []
            for idx, chunk in enumerate(chunks):
                ids.append(f"{path}::{idx}")
                texts.append(chunk)
                metas.append(
                    self._safe_meta({
                        "source_type": "code",
                        "file_path": path,
                        "language": f.get("language", "unknown"),
                        "chunk_index": idx,
                    })
                )

            self._upsert_batch(ids, texts, metas)
            total += len(chunks)

        logger.info("embed_files: %d chunks from %d files", total, len(files))
        return total

    def embed_commits(self, commits: list[dict]) -> int:
        """Embed one document per commit (message + diff_stat summary).

        Returns total embedded.
        """
        ids, texts, metas = [], [], []
        for c in commits:
            h = c.get("hash", "")
            if not h:
                continue
            stat = c.get("diff_stat", {})
            text = (
                f"{c.get('message', '').strip()} "
                f"[+{stat.get('insertions', 0)} -{stat.get('deletions', 0)}]"
            ).strip()
            if not text:
                continue
            ids.append(f"commit::{h}")
            texts.append(text)
            metas.append(
                self._safe_meta({
                    "source_type": "commit",
                    "hash": h,
                    "author_name": c.get("author_name", ""),
                    "timestamp": c.get("timestamp", ""),
                })
            )

        self._upsert_batch(ids, texts, metas)
        logger.info("embed_commits: %d documents", len(ids))
        return len(ids)

    def embed_prs(self, prs: list[dict]) -> int:
        """Embed PR title+body and each review comment as separate documents.

        Returns total embedded.
        """
        total = 0
        for pr in prs:
            num = pr.get("number")
            if num is None:
                continue

            # Title + body as one doc
            title_body = f"{pr.get('title', '')} {pr.get('body', '')}".strip()
            if title_body:
                self._upsert_batch(
                    [f"pr::{num}"],
                    [title_body],
                    [self._safe_meta({
                        "source_type": "pr",
                        "pr_number": num,
                        "author": pr.get("author_login", ""),
                    })],
                )
                total += 1

            # Review comments
            for i, rc in enumerate(pr.get("review_comments", [])):
                body = rc.get("body", "").strip()
                if not body:
                    continue
                self._upsert_batch(
                    [f"pr::{num}::rc::{i}"],
                    [body],
                    [self._safe_meta({
                        "source_type": "pr",
                        "pr_number": num,
                        "author": rc.get("author", "") or "",
                    })],
                )
                total += 1

        logger.info("embed_prs: %d documents from %d PRs", total, len(prs))
        return total

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def query(self, query_text: str, n_results: int = 20) -> list[dict]:
        """Embed *query_text* and return top *n_results* from ChromaDB.

        Each result: {id, text, metadata, distance}.
        """
        vec = self._model.encode([query_text], show_progress_bar=False)[0].tolist()
        res = self._col.query(
            query_embeddings=[vec],
            n_results=n_results,
            include=["documents", "metadatas", "distances"],
        )
        results: list[dict] = []
        for doc_id, text, meta, dist in zip(
            res["ids"][0],
            res["documents"][0],
            res["metadatas"][0],
            res["distances"][0],
        ):
            results.append({
                "id": doc_id,
                "text": text,
                "metadata": meta,
                "distance": dist,
            })
        return results


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json
    import shutil

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    CHROMA_PATH = "./data/chroma_test"
    shutil.rmtree(CHROMA_PATH, ignore_errors=True)

    import chromadb
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    col = client.get_or_create_collection("gitlore-embedtest")

    embedder = Embedder(col)

    mock_files = [
        {
            "path": "auth/login.py",
            "language": "python",
            "content": """
def authenticate_user(username: str, password: str) -> bool:
    \"\"\"Verify user credentials against the database.\"\"\"
    user = db.find_user(username)
    if user is None:
        return False
    return verify_password(password, user.hashed_password)

def create_token(user_id: int) -> str:
    \"\"\"Generate a JWT authentication token.\"\"\"
    payload = {'sub': user_id, 'exp': time.time() + 3600}
    return jwt.encode(payload, SECRET_KEY)
""",
        },
        {
            "path": "storage/file_handler.py",
            "language": "python",
            "content": """
def read_file(path: str) -> bytes:
    with open(path, 'rb') as f:
        return f.read()

def write_file(path: str, data: bytes) -> None:
    with open(path, 'wb') as f:
        f.write(data)
""",
        },
        {
            "path": "api/routes.py",
            "language": "python",
            "content": """
from fastapi import APIRouter, Depends
router = APIRouter()

@router.post('/login')
def login(credentials: LoginRequest, auth: AuthService = Depends()):
    token = auth.authenticate(credentials.username, credentials.password)
    return {'token': token}

@router.get('/health')
def health_check():
    return {'status': 'ok'}
""",
        },
        {
            "path": "db/models.py",
            "language": "python",
            "content": """
class User:
    id: int
    username: str
    hashed_password: str
    created_at: datetime

class Session:
    token: str
    user_id: int
    expires_at: datetime
""",
        },
        {
            "path": "utils/crypto.py",
            "language": "python",
            "content": """
import hashlib, hmac, secrets

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()

def verify_password(password: str, hashed: str) -> bool:
    return hmac.compare_digest(hash_password(password), hashed)
""",
        },
    ]

    print("Embedding 5 mock code files...")
    n = embedder.embed_files(mock_files)
    print(f"Embedded {n} chunks total\n")

    print('Querying: "authentication function"')
    results = embedder.query("authentication function", n_results=3)
    for i, r in enumerate(results, 1):
        preview = r["text"][:120].replace("\n", " ")
        print(f"  [{i}] dist={r['distance']:.4f} | {r['metadata'].get('file_path')} | {preview}")

    del col, client
    shutil.rmtree(CHROMA_PATH, ignore_errors=True)
