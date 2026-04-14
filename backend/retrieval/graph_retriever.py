import logging

import kuzu

logger = logging.getLogger(__name__)


class GraphRetriever:
    def __init__(self, kuzu_conn: kuzu.Connection):
        self._conn = kuzu_conn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def retrieve(
        self,
        entities: list[str],
        query_type: str,
        limit: int = 15,
    ) -> list[dict]:
        """Traverse the knowledge graph and return ranked context items.

        Score convention:
            1.0 — direct match on a node
            0.7 — one hop from matched node
            0.5 — two hops from matched node
        """
        if not entities:
            return []

        results: list[dict] = []
        seen_ids: set[str] = set()

        for entity in entities[:5]:  # cap to avoid overly broad searches
            results.extend(self._query_entity(entity, query_type, seen_ids))

        # Deduplicate and sort
        if query_type == "when":
            results.sort(key=lambda r: (r["metadata"].get("timestamp", ""), r["score"]), reverse=True)
        elif query_type == "why":
            # PRs first (discussion context), then commits
            def _why_key(r):
                type_rank = 0 if r["type"] == "pr" else (1 if r["type"] == "commit" else 2)
                return (type_rank, -r["score"])
            results.sort(key=_why_key)
        else:
            results.sort(key=lambda r: r["score"], reverse=True)

        return results[:limit]

    # ------------------------------------------------------------------
    # Internal query methods
    # ------------------------------------------------------------------

    def _query_entity(
        self, entity: str, query_type: str, seen_ids: set[str]
    ) -> list[dict]:
        results: list[dict] = []

        # --- File nodes (direct match = 1.0) ---
        for row in self._run(
            "MATCH (f:File) WHERE f.path CONTAINS $e "
            "RETURN f.path, f.language LIMIT 5",
            {"e": entity},
        ):
            fid = f"file::{row[0]}"
            if fid not in seen_ids:
                seen_ids.add(fid)
                results.append({
                    "type": "file",
                    "content": f"File: {row[0]} (language: {row[1]})",
                    "metadata": {"source_type": "file", "file_path": row[0], "language": row[1]},
                    "score": 1.0,
                })

        # --- Function nodes (direct match = 1.0) ---
        for row in self._run(
            "MATCH (fn:Function) WHERE fn.name CONTAINS $e "
            "RETURN fn.id, fn.name, fn.signature, fn.file_path, fn.start_line LIMIT 5",
            {"e": entity},
        ):
            fid = f"function::{row[0]}"
            if fid not in seen_ids:
                seen_ids.add(fid)
                results.append({
                    "type": "function",
                    "content": (
                        f"Function: {row[1]}\n"
                        f"Signature: {row[2]}\n"
                        f"File: {row[3]} line {row[4]}"
                    ),
                    "metadata": {
                        "source_type": "function",
                        "name": row[1],
                        "file_path": row[3],
                        "start_line": row[4],
                    },
                    "score": 1.0,
                })

        # --- Commits that modified matching files (one hop = 0.7) ---
        for row in self._run(
            "MATCH (c:Commit)-[r:MODIFIES]->(f:File) "
            "WHERE f.path CONTAINS $e "
            "RETURN c.hash, c.short_hash, c.message, c.author_name, r.timestamp "
            "ORDER BY r.timestamp DESC LIMIT 8",
            {"e": entity},
        ):
            cid = f"commit::{row[0]}"
            if cid not in seen_ids:
                seen_ids.add(cid)
                ts = row[4].isoformat() if row[4] else ""
                results.append({
                    "type": "commit",
                    "content": (
                        f"Commit: {row[1]}\n"
                        f"Message: {row[2]}\n"
                        f"Author: {row[3]}\n"
                        f"Date: {ts}"
                    ),
                    "metadata": {
                        "source_type": "commit",
                        "hash": row[0],
                        "short_hash": row[1],
                        "author_name": row[3],
                        "timestamp": ts,
                    },
                    "score": 0.7,
                })

        # --- Commits that modified files containing matching functions (one hop via fn = 0.7) ---
        for row in self._run(
            "MATCH (c:Commit)-[:MODIFIES]->(f:File)<-[:CONTAINS]-(fn:Function) "
            "WHERE fn.name CONTAINS $e "
            "RETURN c.hash, c.short_hash, c.message, c.author_name "
            "ORDER BY c.timestamp DESC LIMIT 5",
            {"e": entity},
        ):
            cid = f"commit::{row[0]}"
            if cid not in seen_ids:
                seen_ids.add(cid)
                results.append({
                    "type": "commit",
                    "content": (
                        f"Commit: {row[1]}\n"
                        f"Message: {row[2]}\n"
                        f"Author: {row[3]}"
                    ),
                    "metadata": {
                        "source_type": "commit",
                        "hash": row[0],
                        "short_hash": row[1],
                        "author_name": row[3],
                        "timestamp": "",
                    },
                    "score": 0.7,
                })

        # --- PRs that reference commits that touched matching entities (two hops = 0.5) ---
        for row in self._run(
            "MATCH (p:PR)-[:REFERENCES]->(c:Commit)-[:MODIFIES]->(f:File) "
            "WHERE f.path CONTAINS $e "
            "RETURN p.number, p.title, p.description, p.author, p.merged_at LIMIT 5",
            {"e": entity},
        ):
            pid = f"pr::{row[0]}"
            if pid not in seen_ids:
                seen_ids.add(pid)
                ts = row[4].isoformat() if row[4] else ""
                results.append({
                    "type": "pr",
                    "content": (
                        f"PR #{row[0]}: {row[1]}\n"
                        f"Author: {row[3]}\n"
                        f"Merged: {ts}\n"
                        f"Description: {(row[2] or '')[:500]}"
                    ),
                    "metadata": {
                        "source_type": "pr",
                        "pr_number": row[0],
                        "title": row[1],
                        "author": row[3],
                        "merged_at": ts,
                    },
                    "score": 0.5,
                })

        return results

    def _run(self, cypher: str, params: dict) -> list[list]:
        try:
            res = self._conn.execute(cypher, params)
            rows = []
            while res.has_next():
                rows.append(res.get_next())
            return rows
        except Exception as exc:
            logger.warning("Graph query failed: %s | %s", cypher[:80], exc)
            return []
