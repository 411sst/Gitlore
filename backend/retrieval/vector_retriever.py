import logging

logger = logging.getLogger(__name__)

def _infer_type(meta: dict) -> str:
    src = meta.get("source_type", "")
    if src == "code":
        return "file"
    if src in ("commit", "pr"):
        return src
    return "unknown"


class VectorRetriever:
    def __init__(self, embedder):
        self._embedder = embedder

    def retrieve(self, query: str, n_results: int = 20) -> list[dict]:
        """Embed *query* and return top *n_results* from ChromaDB.

        Returns list of {type, content, metadata, score}.
        """
        try:
            raw = self._embedder.query(query, n_results=n_results)
        except Exception as exc:
            logger.error("Vector retrieval failed: %s", exc)
            return []

        results: list[dict] = []
        for item in raw:
            meta = item.get("metadata") or {}
            results.append({
                "type": _infer_type(meta),
                "content": item.get("text", ""),
                "metadata": meta,
                "score": _distance_to_score(item.get("distance")),
            })
        return results


def _distance_to_score(d):
    # ChromaDB distances are L2 squared (range 0-4 for unit vectors).
    # Map distance to relevance in [0, 1] with higher-is-better semantics.
    return 1.0 / (1.0 + d) if d is not None else 0.0
