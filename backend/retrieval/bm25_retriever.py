import logging

logger = logging.getLogger(__name__)


def _infer_type(meta: dict) -> str:
    t = meta.get("type", "")
    if t in ("function", "class", "file"):
        return t
    if t == "commit":
        return "commit"
    return "unknown"


class BM25Retriever:
    def __init__(self, bm25_index):
        self._index = bm25_index

    def retrieve(self, keywords: list[str], n_results: int = 10) -> list[dict]:
        """Query the BM25 index with *keywords*.

        Returns list of {type, content, metadata, score} where score is
        normalised to [0, 1] using score / (score + 1).
        """
        if not keywords:
            return []

        query_str = " ".join(keywords)
        try:
            raw = self._index.query(query_str, n_results=n_results)
        except Exception as exc:
            logger.error("BM25 retrieval failed: %s", exc)
            return []

        results: list[dict] = []
        for item in raw:
            raw_score = item.get("score", 0.0)
            meta = item.get("metadata") or {}
            results.append({
                "type": _infer_type(meta),
                "content": item.get("text", ""),
                "metadata": meta,
                "score": _score_to_relevance(raw_score),
            })
        return results


def _score_to_relevance(raw_score: float) -> float:
    # Normalise BM25 score from [0, inf) to [0, 1).
    return raw_score / (raw_score + 1.0) if raw_score > 0 else 0.0
