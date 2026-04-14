import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Literal

logger = logging.getLogger(__name__)

Status = Literal["pending", "running", "complete", "error", "cancelled"]


class SessionManager:
    def __init__(self, sqlite_conn: sqlite3.Connection):
        self._conn = sqlite_conn
        self._cancel_flag: bool = False

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create_session(self, repo_url: str, config: dict) -> str:
        """Insert a new session row and return its UUID string."""
        session_id = str(uuid.uuid4())
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self._conn.execute(
            "INSERT INTO sessions (id, repo_url, config_json, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'running', ?, ?)",
            (session_id, repo_url, json.dumps(config), now, now),
        )
        self._conn.commit()
        self._cancel_flag = False
        logger.info("Session created: %s  repo=%s", session_id, repo_url)
        return session_id

    def update_status(self, session_id: str, status: Status) -> None:
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self._conn.execute(
            "UPDATE sessions SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, session_id),
        )
        self._conn.commit()

    def log_event(self, session_id: str, stage: str, message: str) -> None:
        self._conn.execute(
            "INSERT INTO ingestion_log (session_id, stage, message) VALUES (?, ?, ?)",
            (session_id, stage, message),
        )
        self._conn.commit()

    def get_current_session(self) -> dict | None:
        row = self._conn.execute(
            "SELECT id, repo_url, config_json, status, created_at, updated_at "
            "FROM sessions ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return {
            "session_id": row[0],
            "repo_url": row[1],
            "config": json.loads(row[2]) if row[2] else {},
            "status": row[3],
            "created_at": row[4],
            "updated_at": row[5],
        }

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    def cancel_current(self) -> bool:
        """Set the cancellation flag.  Returns True if a session is running."""
        session = self.get_current_session()
        if session and session["status"] == "running":
            self._cancel_flag = True
            logger.info("Cancellation requested for session %s", session["session_id"])
            return True
        return False

    def is_cancelled(self) -> bool:
        return self._cancel_flag

    def clear_cancel(self) -> None:
        self._cancel_flag = False
