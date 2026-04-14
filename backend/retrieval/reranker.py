import hashlib
import logging

from sentence_transformers import CrossEncoder

logger = logging.getLogger(__name__)

_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


def _content_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def _make_source_id(item: dict) -> str:
    meta = item.get("metadata") or {}
    # source_type from ChromaDB embeddings; type from BM25/graph metadata
    src = meta.get("source_type") or meta.get("type") or item.get("type") or "unknown"
    key = (
        meta.get("hash")
        or (str(meta["pr_number"]) if meta.get("pr_number") is not None else None)
        or meta.get("file_path")
        or meta.get("file")       # BM25 index uses "file" not "file_path"
        or meta.get("name")
        or item.get("content", "")[:32].replace(" ", "_").replace("/", "_")
        or "unknown"
    )
    # Sanitise: keep only safe characters
    import re
    key = re.sub(r"[^a-zA-Z0-9._/-]", "_", str(key))
    return f"{src}_{key}"


class Reranker:
    def __init__(self):
        logger.info("Loading cross-encoder model %s …", _MODEL)
        self._model = CrossEncoder(_MODEL)

    def rerank(
        self,
        query: str,
        candidates: list[dict],
        top_k: int = 8,
    ) -> list[dict]:
        """Deduplicate, cross-encode, and return the top *top_k* candidates.

        Adds a ``source_id`` field to each result.
        """
        if not candidates:
            return []

        # Deduplicate by content hash
        seen_hashes: set[str] = set()
        unique: list[dict] = []
        for item in candidates:
            h = _content_hash(item.get("content", ""))
            if h not in seen_hashes:
                seen_hashes.add(h)
                unique.append(item)

        if not unique:
            return []

        # Cross-encode (batch predict is faster than one-by-one)
        pairs = [(query, item.get("content", "")) for item in unique]
        try:
            scores = self._model.predict(pairs)
        except Exception as exc:
            logger.error("Cross-encoder predict failed: %s", exc)
            # Fall back to original order
            scores = list(range(len(unique), 0, -1))

        ranked = sorted(
            zip(scores, unique),
            key=lambda x: x[0],
            reverse=True,
        )

        results: list[dict] = []
        for score, item in ranked[:top_k]:
            enriched = dict(item)
            enriched["rerank_score"] = float(score)
            enriched["source_id"] = _make_source_id(item)
            results.append(enriched)

        return results
