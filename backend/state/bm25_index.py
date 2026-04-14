import json
import logging
import pickle
import re
import sqlite3
from datetime import datetime, timezone

import numpy as np
from rank_bm25 import BM25Okapi

logger = logging.getLogger(__name__)

_TOKENISE_RE = re.compile(r"[^a-zA-Z0-9]+")

_DDL = """
CREATE TABLE IF NOT EXISTS bm25_index (
    id         INTEGER PRIMARY KEY,
    index_blob BLOB    NOT NULL,
    docs_json  TEXT    NOT NULL,
    created_at TEXT    NOT NULL
)
"""


def _tokenise(text: str) -> list[str]:
    """Lowercase and split on non-alphanumeric characters, drop empty tokens."""
    return [t for t in _TOKENISE_RE.split(text.lower()) if t]


class BM25Index:
    def __init__(self, sqlite_conn: sqlite3.Connection):
        self._conn = sqlite_conn
        self._bm25: BM25Okapi | None = None
        self._docs: list[dict] = []
        self._conn.execute(_DDL)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self, documents: list[dict]) -> None:
        """Build (or rebuild) the BM25 index from *documents*.

        Parameters
        ----------
        documents : list[dict]
            Each dict must have ``id`` (str), ``text`` (str), and
            ``metadata`` (dict).  Intended content: function names, class
            names, file names, commit messages — *not* full source code.
        """
        if not documents:
            logger.warning("BM25Index.build called with empty document list — skipping.")
            return

        self._docs = documents
        tokenised = [_tokenise(d["text"]) for d in documents]

        # BM25Okapi IDF is 0 for terms that appear in ≥50 % of the corpus
        # (because log((N - df + 0.5) / (df + 0.5)) ≤ 0 when df ≥ N/2).
        # This is expected behaviour; results degrade only for very small
        # corpora (< 3 docs) where every term appears in exactly one doc.
        self._bm25 = BM25Okapi(tokenised)

        blob = pickle.dumps(self._bm25)
        docs_json = json.dumps(documents)
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        self._conn.execute("DELETE FROM bm25_index")
        self._conn.execute(
            "INSERT INTO bm25_index (id, index_blob, docs_json, created_at) VALUES (1, ?, ?, ?)",
            (blob, docs_json, now),
        )
        self._conn.commit()
        logger.info("BM25Index built: %d documents indexed.", len(documents))

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> bool:
        """Load the index from SQLite.  Returns True if found, False if not."""
        row = self._conn.execute(
            "SELECT index_blob, docs_json FROM bm25_index WHERE id = 1"
        ).fetchone()
        if row is None:
            logger.debug("BM25Index: no persisted index found.")
            return False
        try:
            self._bm25 = pickle.loads(row[0])
            self._docs = json.loads(row[1])
            logger.info("BM25Index loaded: %d documents.", len(self._docs))
            return True
        except Exception as exc:
            logger.error("BM25Index: failed to load persisted index: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def query(self, query_text: str, n_results: int = 10) -> list[dict]:
        """Return top *n_results* documents ranked by BM25 score.

        Each result: {id, text, metadata, score}.
        """
        if self._bm25 is None or not self._docs:
            logger.warning("BM25Index.query called before build/load — returning empty.")
            return []

        tokens = _tokenise(query_text)
        if not tokens:
            return []

        scores: np.ndarray = self._bm25.get_scores(tokens)
        k = min(n_results, len(self._docs))

        # argsort descending, take top k
        top_indices = np.argsort(scores)[::-1][:k]

        results: list[dict] = []
        for idx in top_indices:
            doc = self._docs[idx]
            results.append({
                "id": doc["id"],
                "text": doc["text"],
                "metadata": doc.get("metadata", {}),
                "score": float(scores[idx]),
            })
        return results


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sqlite3

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = sqlite3.connect(":memory:")
    idx = BM25Index(conn)

    docs = [
        {"id": "fn::authenticate_user", "text": "authenticate_user auth login password verify", "metadata": {"type": "function", "file": "auth/login.py"}},
        {"id": "fn::create_token",      "text": "create_token jwt token generate", "metadata": {"type": "function", "file": "auth/login.py"}},
        {"id": "fn::read_file",         "text": "read_file open bytes disk storage", "metadata": {"type": "function", "file": "storage/file_handler.py"}},
        {"id": "fn::write_file",        "text": "write_file save bytes disk storage", "metadata": {"type": "function", "file": "storage/file_handler.py"}},
        {"id": "fn::hash_password",     "text": "hash_password pbkdf2 sha256 salt crypto", "metadata": {"type": "function", "file": "utils/crypto.py"}},
        {"id": "cl::User",              "text": "User model database username password", "metadata": {"type": "class", "file": "db/models.py"}},
        {"id": "commit::abc123",        "text": "add authentication middleware login security", "metadata": {"type": "commit"}},
    ]

    idx.build(docs)

    # Persist + reload round-trip
    loaded = BM25Index(conn)
    ok = loaded.load()
    print(f"Load succeeded: {ok}\n")

    results = loaded.query("authentication login", n_results=3)
    print("Top 3 for 'authentication login':")
    for r in results:
        print(f"  score={r['score']:.4f}  id={r['id']:<35}  text={r['text']}")
