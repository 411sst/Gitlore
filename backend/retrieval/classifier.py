import re

_STOPWORDS = frozenset({
    "a", "an", "the", "is", "in", "on", "at", "to", "for", "of", "and",
    "or", "but", "not", "with", "this", "that", "it", "be", "are", "was",
    "were", "has", "have", "had", "do", "does", "did", "will", "would",
    "can", "could", "should", "may", "might", "what", "when", "where",
    "who", "which", "how", "why", "me", "my", "i", "you", "your", "we",
    "our", "they", "their", "by", "from", "about", "into", "through",
    "during", "before", "after", "above", "between", "each", "get",
    "its", "also", "there", "than", "then", "so", "any", "tell", "show",
    "give", "find", "list", "all",
    # query verbs that appear capitalised at sentence start
    "explain", "describe", "what", "when", "why", "how", "where",
})

# Symbol patterns — order matters (most specific first)
_SYMBOL_RE = re.compile(
    r"(?:"
    r"[A-Z]{2,}(?:_[A-Z0-9]+)*|"          # UPPER_CASE / SCREAMING_SNAKE
    r"[A-Za-z][a-z]+(?:[A-Z][a-zA-Z0-9]*)+|"  # CamelCase (upper or lower start)
    r"[a-z]+[A-Z][a-zA-Z0-9]*|"           # lowerCamelCase (e.g. watchServer)
    r"[a-z]+(?:_[a-z0-9]+){1,}|"          # snake_case (2+ segments)
    r"[a-zA-Z][a-zA-Z0-9]*[0-9]+[a-zA-Z0-9]*|"  # alphanumeric tokens (clientv3, etcd3)
    r"\w+\.[a-zA-Z]{1,5}"                  # file.ext or path.go
    r")"
)


class QueryClassifier:
    """Rule-based query classifier.

    Returns:
        {
            type:     "why" | "when" | "what" | "how" | "explain",
            entities: list of likely symbol / file names,
            keywords: list of meaningful non-symbol words,
        }
    """

    def classify(self, query: str) -> dict:
        lower = query.lower()

        # Determine query type
        if any(w in lower for w in ("why", "reason", "because", "purpose")):
            q_type = "why"
        elif any(w in lower for w in ("when", "introduced", "removed", "changed", "added", "deprecated")):
            q_type = "when"
        elif "how" in lower:
            q_type = "how"
        elif any(w in lower for w in ("explain", "describe", "overview")):
            q_type = "explain"
        else:
            q_type = "what"

        # Extract entity tokens: CamelCase/snake_case/UPPER/file.ext  +
        # plain capitalized words (likely proper names / identifiers)
        entities: list[str] = []
        seen: set[str] = set()

        for m in _SYMBOL_RE.finditer(query):
            tok = m.group()
            if tok.lower() not in _STOPWORDS and tok.lower() not in seen:
                entities.append(tok)
                seen.add(tok.lower())

        # Also capture standalone capitalized words not caught by regex
        for word in re.findall(r"\b[A-Z][a-zA-Z0-9]+\b", query):
            if word.lower() not in seen and word.lower() not in _STOPWORDS:
                entities.append(word)
                seen.add(word.lower())

        # Keywords: meaningful words not in stopwords, not already an entity
        keywords: list[str] = []
        for word in re.findall(r"\b[a-zA-Z]{3,}\b", lower):
            if word not in _STOPWORDS and word not in seen:
                keywords.append(word)
                seen.add(word)

        return {"type": q_type, "entities": entities, "keywords": keywords}
