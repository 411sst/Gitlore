import kuzu
import os
import logging

logger = logging.getLogger(__name__)

DB_PATH = "./data/kuzu"

# ---------------------------------------------------------------------------
# Node and relationship DDL
# ---------------------------------------------------------------------------

_NODE_TABLES = [
    """CREATE NODE TABLE IF NOT EXISTS File(
        path          STRING PRIMARY KEY,
        language      STRING,
        last_modified TIMESTAMP
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Function(
        id         STRING PRIMARY KEY,
        name       STRING,
        signature  STRING,
        docstring  STRING,
        file_path  STRING,
        start_line INT64
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Commit(
        hash         STRING PRIMARY KEY,
        short_hash   STRING,
        message      STRING,
        author_name  STRING,
        author_email STRING,
        timestamp    TIMESTAMP,
        branch       STRING
    )""",
    """CREATE NODE TABLE IF NOT EXISTS PR(
        number      INT64 PRIMARY KEY,
        title       STRING,
        description STRING,
        merged_at   TIMESTAMP,
        author      STRING
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Concept(
        name        STRING PRIMARY KEY,
        description STRING
    )""",
]

_REL_TABLES = [
    "CREATE REL TABLE IF NOT EXISTS CONTAINS(FROM File TO Function)",
    "CREATE REL TABLE IF NOT EXISTS CALLS(FROM Function TO Function)",
    "CREATE REL TABLE IF NOT EXISTS MODIFIES(FROM Commit TO File, timestamp TIMESTAMP)",
    "CREATE REL TABLE IF NOT EXISTS INTRODUCES(FROM Commit TO Function, timestamp TIMESTAMP)",
    "CREATE REL TABLE IF NOT EXISTS REFERENCES(FROM PR TO Commit, merged_at TIMESTAMP)",
    "CREATE REL TABLE IF NOT EXISTS TAGGED(FROM File TO Concept)",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_kuzu() -> kuzu.Database:
    os.makedirs("./data", exist_ok=True)
    return kuzu.Database(DB_PATH)


def get_connection(db: kuzu.Database) -> kuzu.Connection:
    return kuzu.Connection(db)


def init_schema(conn: kuzu.Connection) -> None:
    """Create all node and relationship tables (idempotent — uses IF NOT EXISTS)."""
    for ddl in _NODE_TABLES:
        conn.execute(ddl)
    for ddl in _REL_TABLES:
        conn.execute(ddl)
    logger.info("Kuzu schema initialised.")


def dedupe_pr_reference_edges(conn: kuzu.Connection) -> int:
    """Collapse duplicate PR->Commit REFERENCES edges to one edge per pair.

    Returns the number of duplicate edges removed.
    """
    try:
        res = conn.execute(
            "MATCH (p:PR)-[r:REFERENCES]->(c:Commit) "
            "WITH p.number AS pr_number, c.hash AS commit_hash, COUNT(r) AS edge_count "
            "WHERE edge_count > 1 "
            "RETURN pr_number, commit_hash, edge_count"
        )
    except Exception as exc:
        logger.warning("Could not scan REFERENCES duplicates: %s", exc)
        return 0

    duplicate_pairs: list[tuple[int, str, int]] = []
    while res.has_next():
        pr_number, commit_hash, edge_count = res.get_next()
        duplicate_pairs.append((pr_number, commit_hash, edge_count))

    removed = 0
    for pr_number, commit_hash, edge_count in duplicate_pairs:
        conn.execute(
            "MATCH (p:PR {number: $num})-[r:REFERENCES]->(c:Commit {hash: $hash}) "
            "DELETE r",
            {"num": pr_number, "hash": commit_hash},
        )
        conn.execute(
            "MATCH (p:PR {number: $num}), (c:Commit {hash: $hash}) "
            "MERGE (p)-[r:REFERENCES]->(c) "
            "ON CREATE SET r.merged_at = p.merged_at",
            {"num": pr_number, "hash": commit_hash},
        )
        removed += max(0, edge_count - 1)

    if removed:
        logger.info("Deduplicated %d PR REFERENCES edges", removed)
    return removed
