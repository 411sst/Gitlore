import re

_CITATION_RE = re.compile(r"\[([^\[\]]+)\]")


class CitationExtractor:
    def extract(self, answer: str, context_items: list[dict]) -> dict:
        """Find all [source_id] patterns in *answer* and resolve them to metadata.

        Parameters
        ----------
        answer : str
            The full LLM answer text.
        context_items : list[dict]
            The reranked context items passed to the LLM, each containing a
            ``source_id`` field.

        Returns
        -------
        dict
            ``{answer: str, citations: [{source_id, type, metadata}]}``
        """
        # Build lookup from source_id → item
        source_map: dict[str, dict] = {
            item["source_id"]: item
            for item in context_items
            if "source_id" in item
        }

        cited_ids: list[str] = []
        seen: set[str] = set()
        for m in _CITATION_RE.finditer(answer):
            sid = m.group(1).strip()
            if sid in source_map and sid not in seen:
                cited_ids.append(sid)
                seen.add(sid)

        citations = [
            {
                "source_id": sid,
                "type": source_map[sid].get("type", "unknown"),
                "metadata": source_map[sid].get("metadata", {}),
            }
            for sid in cited_ids
        ]

        return {"answer": answer, "citations": citations}
