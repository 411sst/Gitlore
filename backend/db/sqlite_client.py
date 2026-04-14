import sqlite3
import os
import json
import logging

logger = logging.getLogger(__name__)

DB_PATH = "./data/gitlore.db"

def init_sqlite() -> sqlite3.Connection:
    os.makedirs("./data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id          TEXT PRIMARY KEY,
            repo_url    TEXT NOT NULL,
            config_json TEXT,
            status      TEXT NOT NULL DEFAULT 'pending',
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS chat_history (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role       TEXT NOT NULL,
            content    TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        );

        CREATE TABLE IF NOT EXISTS ingestion_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            stage      TEXT NOT NULL,
            message    TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        );
    """)
    conn.commit()


def scrub_sensitive_session_configs(conn: sqlite3.Connection) -> int:
    """Remove sensitive keys from persisted session config blobs."""
    rows = conn.execute(
        "SELECT id, config_json FROM sessions "
        "WHERE config_json IS NOT NULL AND config_json != ''"
    ).fetchall()

    updated = 0
    for row in rows:
        session_id = row["id"] if isinstance(row, sqlite3.Row) else row[0]
        raw_cfg = row["config_json"] if isinstance(row, sqlite3.Row) else row[1]
        try:
            cfg = json.loads(raw_cfg)
        except Exception:
            continue
        if not isinstance(cfg, dict):
            continue

        if "github_token" not in cfg:
            continue

        cfg.pop("github_token", None)
        conn.execute(
            "UPDATE sessions SET config_json = ?, updated_at = datetime('now') WHERE id = ?",
            (json.dumps(cfg), session_id),
        )
        updated += 1

    if updated:
        conn.commit()
        logger.info("Scrubbed sensitive keys from %d sessions", updated)
    return updated
