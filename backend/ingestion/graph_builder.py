import logging
from datetime import datetime, timezone

import kuzu

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_kuzu_ts(ts_str: str | None) -> str | None:
    """Normalise an ISO-8601 timestamp string to the format Kuzu's
    ``timestamp()`` function accepts: ``YYYY-MM-DD HH:MM:SS`` (UTC).

    Returns ``None`` if the input is falsy so the caller can skip the column.
    """
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _fn_id(file_path: str, name: str, start_line: int) -> str:
    return f"{file_path}::{name}::{start_line}"


# ---------------------------------------------------------------------------
# GraphBuilder
# ---------------------------------------------------------------------------

class GraphBuilder:
    def __init__(self, kuzu_conn: kuzu.Connection):
        self._conn = kuzu_conn

    # ------------------------------------------------------------------
    # Main build entry point
    # ------------------------------------------------------------------

    def build_from_parsed(
        self,
        commits: list[dict],
        files: list[dict],
        code_entities: list[dict],
    ) -> dict:
        """Insert nodes and edges derived from parsed repo data.

        Parameters
        ----------
        commits:       output of GitParser.parse_commits()
        files:         output of GitParser.parse_file_tree()
        code_entities: output of CodeParser.parse_all()

        Returns a summary dict with insertion counts.
        """
        files_inserted = self._insert_files(files)
        functions_inserted, contains_edges = self._insert_functions(code_entities)
        commits_inserted = self._insert_commits(commits)
        modifies_edges = self._insert_modifies(commits)
        introduces_edges = self._insert_introduces(commits, code_entities)

        edges_created = contains_edges + modifies_edges + introduces_edges
        logger.info(
            "Graph build complete: %d files, %d functions, %d commits, %d edges",
            files_inserted, functions_inserted, commits_inserted, edges_created,
        )
        return {
            "files_inserted": files_inserted,
            "functions_inserted": functions_inserted,
            "commits_inserted": commits_inserted,
            "edges_created": edges_created,
        }

    # ------------------------------------------------------------------
    # Node inserters
    # ------------------------------------------------------------------

    def _insert_files(self, files: list[dict]) -> int:
        count = 0
        for f in files:
            ts = _to_kuzu_ts(f.get("last_modified"))
            if ts:
                self._conn.execute(
                    "MERGE (n:File {path: $path}) "
                    "ON CREATE SET n.language = $lang, n.last_modified = timestamp($ts)",
                    {"path": f["path"], "lang": f.get("language", "unknown"), "ts": ts},
                )
            else:
                self._conn.execute(
                    "MERGE (n:File {path: $path}) "
                    "ON CREATE SET n.language = $lang",
                    {"path": f["path"], "lang": f.get("language", "unknown")},
                )
            count += 1
        return count

    def _insert_functions(self, code_entities: list[dict]) -> tuple[int, int]:
        fn_count = 0
        edge_count = 0
        for entity in code_entities:
            file_path = entity["path"]
            for fn in entity.get("functions", []):
                fn_id = _fn_id(file_path, fn["name"], fn.get("start_line", 0))
                sig = f"{fn['name']}({', '.join(fn.get('parameters', []))})"
                self._conn.execute(
                    "MERGE (f:Function {id: $id}) "
                    "ON CREATE SET f.name = $name, f.signature = $sig, "
                    "f.docstring = $doc, f.file_path = $fp, f.start_line = $sl",
                    {
                        "id": fn_id,
                        "name": fn["name"],
                        "sig": sig,
                        "doc": fn.get("docstring"),
                        "fp": file_path,
                        "sl": fn.get("start_line", 0),
                    },
                )
                fn_count += 1

                # CONTAINS edge: File → Function
                # Only create if the File node exists
                self._conn.execute(
                    "MATCH (fi:File {path: $fp}), (fn:Function {id: $fid}) "
                    "MERGE (fi)-[:CONTAINS]->(fn)",
                    {"fp": file_path, "fid": fn_id},
                )
                edge_count += 1

        return fn_count, edge_count

    def _insert_commits(self, commits: list[dict]) -> int:
        count = 0
        for c in commits:
            ts = _to_kuzu_ts(c.get("timestamp"))
            if ts:
                self._conn.execute(
                    "MERGE (n:Commit {hash: $hash}) "
                    "ON CREATE SET n.short_hash = $sh, n.message = $msg, "
                    "n.author_name = $an, n.author_email = $ae, "
                    "n.timestamp = timestamp($ts), n.branch = $branch",
                    {
                        "hash": c["hash"],
                        "sh": c.get("short_hash", c["hash"][:8]),
                        "msg": c.get("message", ""),
                        "an": c.get("author_name", ""),
                        "ae": c.get("author_email", ""),
                        "ts": ts,
                        "branch": c.get("branch", "main"),
                    },
                )
                count += 1
            else:
                logger.warning("Skipping commit %s: missing/invalid timestamp", c.get("hash", "?"))
        return count

    # ------------------------------------------------------------------
    # Edge inserters
    # ------------------------------------------------------------------

    def _insert_modifies(self, commits: list[dict]) -> int:
        edge_count = 0
        for c in commits:
            ts = _to_kuzu_ts(c.get("timestamp"))
            if not ts:
                continue
            for file_path in c.get("changed_files", []):
                # Ensure a File stub exists even if it wasn't in the tree
                # (deleted files, renames, etc.)
                self._conn.execute(
                    "MERGE (n:File {path: $path})",
                    {"path": file_path},
                )
                # MERGE prevents duplicates on re-ingestion; properties are set
                # via ON CREATE because MODIFIES has no natural unique key beyond
                # (commit, file) — re-running will not update the timestamp.
                self._conn.execute(
                    "MATCH (cm:Commit {hash: $hash}), (fi:File {path: $path}) "
                    "MERGE (cm)-[r:MODIFIES]->(fi) "
                    "ON CREATE SET r.timestamp = timestamp($ts)",
                    {"hash": c["hash"], "path": file_path, "ts": ts},
                )
                edge_count += 1
        return edge_count

    def _insert_introduces(
        self, commits: list[dict], code_entities: list[dict]
    ) -> int:
        """Create INTRODUCES edges approximating the first commit to touch a file
        that contains each function.

        Strategy: iterate commits oldest-first; for each changed file, look up
        functions in that file from code_entities; create an INTRODUCES edge
        only if the function has no existing INTRODUCES edge yet.

        Uses an in-memory set to track introduced function IDs so we only
        issue one Kuzu query per new edge (not one per function per commit).
        """
        # Load already-introduced functions in a single query (idempotent restarts)
        already_introduced: set[str] = set()
        res = self._conn.execute(
            "MATCH (:Commit)-[:INTRODUCES]->(fn:Function) RETURN fn.id"
        )
        while res.has_next():
            already_introduced.add(res.get_next()[0])

        # Build lookup: file_path → list of function IDs
        file_to_fns: dict[str, list[str]] = {}
        for entity in code_entities:
            fp = entity["path"]
            ids = [
                _fn_id(fp, fn["name"], fn.get("start_line", 0))
                for fn in entity.get("functions", [])
            ]
            if ids:
                file_to_fns[fp] = ids

        edge_count = 0
        # Sort oldest-first so the first commit we process wins
        sorted_commits = sorted(
            commits,
            key=lambda c: c.get("timestamp", ""),
        )

        for c in sorted_commits:
            ts = _to_kuzu_ts(c.get("timestamp"))
            if not ts:
                continue
            for file_path in c.get("changed_files", []):
                for fn_id in file_to_fns.get(file_path, []):
                    if fn_id in already_introduced:
                        continue
                    self._conn.execute(
                        "MATCH (cm:Commit {hash: $hash}), (fn:Function {id: $fid}) "
                        "CREATE (cm)-[:INTRODUCES {timestamp: timestamp($ts)}]->(fn)",
                        {"hash": c["hash"], "fid": fn_id, "ts": ts},
                    )
                    already_introduced.add(fn_id)
                    edge_count += 1

        return edge_count

    # ------------------------------------------------------------------
    # PR graph builder
    # ------------------------------------------------------------------

    def build_pr_nodes(self, prs: list[dict]) -> dict:
        """Insert PR nodes and REFERENCES edges into the graph.

        A REFERENCES edge is created from the PR to each commit SHA listed in
        ``pr['linked_commit_shas']``.  If a commit hash does not yet exist in
        the graph the edge is silently skipped (logged at DEBUG level).

        Returns a summary dict: {prs_inserted, edges_created}.
        """
        prs_inserted = 0
        edges_created = 0

        for pr in prs:
            merged_ts = _to_kuzu_ts(pr.get("merged_at"))
            if merged_ts:
                self._conn.execute(
                    "MERGE (p:PR {number: $num}) "
                    "ON CREATE SET p.title = $title, p.description = $pr_body, "
                    "p.merged_at = timestamp($ts), p.author = $author",
                    {
                        "num": pr["number"],
                        "title": pr.get("title", ""),
                        "pr_body": pr.get("body", ""),
                        "ts": merged_ts,
                        "author": pr.get("author_login", ""),
                    },
                )
            else:
                self._conn.execute(
                    "MERGE (p:PR {number: $num}) "
                    "ON CREATE SET p.title = $title, p.description = $pr_body, p.author = $author",
                    {
                        "num": pr["number"],
                        "title": pr.get("title", ""),
                        "pr_body": pr.get("body", ""),
                        "author": pr.get("author_login", ""),
                    },
                )
            prs_inserted += 1

            # REFERENCES edges to commits
            for sha in pr.get("linked_commit_shas", []):
                # Check the commit exists before creating the edge
                res = self._conn.execute(
                    "MATCH (c:Commit {hash: $hash}) RETURN c.hash LIMIT 1",
                    {"hash": sha},
                )
                if not res.has_next():
                    logger.debug(
                        "PR #%d references unknown commit %s — skipping edge",
                        pr["number"], sha[:12],
                    )
                    continue

                edge_exists = self._conn.execute(
                    "MATCH (p:PR {number: $num})-[:REFERENCES]->(c:Commit {hash: $hash}) "
                    "RETURN 1 LIMIT 1",
                    {"num": pr["number"], "hash": sha},
                ).has_next()

                ref_ts = _to_kuzu_ts(pr.get("merged_at"))
                if ref_ts:
                    self._conn.execute(
                        "MATCH (p:PR {number: $num}), (c:Commit {hash: $hash}) "
                        "MERGE (p)-[r:REFERENCES]->(c) "
                        "ON CREATE SET r.merged_at = timestamp($ts)",
                        {"num": pr["number"], "hash": sha, "ts": ref_ts},
                    )
                else:
                    self._conn.execute(
                        "MATCH (p:PR {number: $num}), (c:Commit {hash: $hash}) "
                        "MERGE (p)-[:REFERENCES]->(c)",
                        {"num": pr["number"], "hash": sha},
                    )
                if not edge_exists:
                    edges_created += 1

        logger.info(
            "PR build complete: %d PRs inserted, %d REFERENCES edges created",
            prs_inserted, edges_created,
        )
        return {"prs_inserted": prs_inserted, "edges_created": edges_created}

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def query_file_history(self, file_path: str) -> list[dict]:
        """Return all commits that modified this file, newest first."""
        res = self._conn.execute(
            "MATCH (c:Commit)-[r:MODIFIES]->(f:File {path: $path}) "
            "RETURN c.hash, c.short_hash, c.message, c.author_name, r.timestamp "
            "ORDER BY r.timestamp DESC",
            {"path": file_path},
        )
        rows = []
        while res.has_next():
            h, sh, msg, author, ts = res.get_next()
            rows.append({
                "hash": h,
                "short_hash": sh,
                "message": msg,
                "author_name": author,
                "timestamp": ts.isoformat() if ts else None,
            })
        return rows

    def query_function_origin(self, function_name: str) -> dict | None:
        """Return the earliest commit that introduced a function with this name."""
        res = self._conn.execute(
            "MATCH (c:Commit)-[r:INTRODUCES]->(fn:Function) "
            "WHERE fn.name = $name "
            "RETURN c.hash, c.short_hash, c.message, c.author_name, r.timestamp "
            "ORDER BY r.timestamp ASC LIMIT 1",
            {"name": function_name},
        )
        if not res.has_next():
            return None
        h, sh, msg, author, ts = res.get_next()
        return {
            "hash": h,
            "short_hash": sh,
            "message": msg,
            "author_name": author,
            "timestamp": ts.isoformat() if ts else None,
        }


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json
    import os
    import shutil

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    import tempfile, uuid
    DB_PATH = os.path.join("./data", f"kuzu_test_{uuid.uuid4().hex[:8]}")
    os.makedirs("./data", exist_ok=True)

    db = kuzu.Database(DB_PATH)
    conn = kuzu.Connection(db)

    # Init schema
    from db.kuzu_client import init_schema
    init_schema(conn)

    # ------------------------------------------------------------------
    # Mock data
    # ------------------------------------------------------------------
    mock_files = [
        {"path": "server/main.go", "language": "go", "last_modified": "2016-06-01T10:00:00"},
        {"path": "server/handler.go", "language": "go", "last_modified": "2016-08-15T12:00:00"},
        {"path": "client/client.go", "language": "go", "last_modified": "2016-03-20T09:00:00"},
    ]

    mock_code_entities = [
        {
            "path": "server/main.go",
            "language": "go",
            "functions": [
                {"name": "main", "start_line": 10, "end_line": 20, "parameters": [], "docstring": None},
                {"name": "startServer", "start_line": 25, "end_line": 40, "parameters": ["addr string"], "docstring": None},
            ],
        },
        {
            "path": "server/handler.go",
            "language": "go",
            "functions": [
                {"name": "handleRequest", "start_line": 5, "end_line": 30, "parameters": ["w http.ResponseWriter", "r *http.Request"], "docstring": "Handles incoming HTTP requests."},
            ],
        },
        {
            "path": "client/client.go",
            "language": "go",
            "functions": [
                {"name": "NewClient", "start_line": 8, "end_line": 20, "parameters": ["addr string"], "docstring": "Creates a new client."},
                {"name": "Close", "start_line": 22, "end_line": 28, "parameters": ["c *Client"], "docstring": None},
            ],
        },
    ]

    mock_commits = [
        {
            "hash": "aaa000001",
            "short_hash": "aaa0000",
            "message": "Initial server scaffold",
            "author_name": "Alice",
            "author_email": "alice@example.com",
            "timestamp": "2016-01-15T08:00:00",
            "changed_files": ["server/main.go", "server/handler.go"],
        },
        {
            "hash": "bbb000002",
            "short_hash": "bbb0000",
            "message": "Add client package",
            "author_name": "Bob",
            "author_email": "bob@example.com",
            "timestamp": "2016-03-20T09:00:00",
            "changed_files": ["client/client.go"],
        },
        {
            "hash": "ccc000003",
            "short_hash": "ccc0000",
            "message": "Fix handler edge case",
            "author_name": "Alice",
            "author_email": "alice@example.com",
            "timestamp": "2016-08-15T12:00:00",
            "changed_files": ["server/handler.go"],
        },
        {
            "hash": "ddd000004",
            "short_hash": "ddd0000",
            "message": "Refactor server main",
            "author_name": "Carol",
            "author_email": "carol@example.com",
            "timestamp": "2016-11-01T14:00:00",
            "changed_files": ["server/main.go", "client/client.go"],
        },
    ]

    # ------------------------------------------------------------------
    # Build graph
    # ------------------------------------------------------------------
    builder = GraphBuilder(conn)
    summary = builder.build_from_parsed(mock_commits, mock_files, mock_code_entities)
    print("\nBuild summary:", json.dumps(summary, indent=2))

    # ------------------------------------------------------------------
    # Query: history of server/handler.go
    # ------------------------------------------------------------------
    print("\nHistory of server/handler.go:")
    history = builder.query_file_history("server/handler.go")
    for row in history:
        print(json.dumps(row, indent=2))

    # ------------------------------------------------------------------
    # Query: origin of handleRequest
    # ------------------------------------------------------------------
    print("\nOrigin of handleRequest:")
    origin = builder.query_function_origin("handleRequest")
    print(json.dumps(origin, indent=2))

    # Explicitly release Kuzu handles before attempting to delete on Windows
    del builder, conn, db
    shutil.rmtree(DB_PATH, ignore_errors=True)
