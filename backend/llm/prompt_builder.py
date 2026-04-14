_SYSTEM_PROMPT = (
    "You are a codebase assistant. Answer only from the provided context. "
    "Cite sources using their source_id in square brackets like [commit_abc123]. "
    "Do not speculate beyond what is in the context. "
    "If the context does not contain enough information to answer, say so clearly."
)


class PromptBuilder:
    def build(self, query: str, context_items: list[dict]) -> list[dict]:
        """Build a messages list ready for the Groq chat completions API.

        Parameters
        ----------
        query : str
            The user's question.
        context_items : list[dict]
            Reranked context items, each with ``source_id``, ``type``, and
            ``content`` fields.

        Returns
        -------
        list[dict]
            ``[{"role": "system", "content": ...}, {"role": "user", "content": ...}]``
        """
        context_blocks: list[str] = []
        for item in context_items:
            sid = item.get("source_id", "unknown")
            item_type = item.get("type", "unknown")
            content = item.get("content", "").strip()
            if not content:
                continue
            context_blocks.append(
                f"[{sid}] ({item_type})\n{content}"
            )

        context_str = "\n\n---\n\n".join(context_blocks)
        user_message = (
            f"Context:\n\n{context_str}\n\n"
            f"---\n\nQuestion: {query}"
        )

        return [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]
