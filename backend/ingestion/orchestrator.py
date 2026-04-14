"""IngestionOrchestrator — coordinates all ingestion stages and streams
progress updates to an asyncio.Queue consumed by the SSE endpoint."""

import asyncio
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import kuzu

logger = logging.getLogger(__name__)

# Temporary clone base dir (resolved at import time so it works on Windows too)
_CLONE_BASE = os.path.join(tempfile.gettempdir(), "gitlore_clones")


def _iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class IngestionOrchestrator:
    def __init__(self, kuzu_conn, chroma_collection, sqlite_conn, session_manager=None):
        self._kuzu_conn = kuzu_conn
        self._chroma_col = chroma_collection
        self._sqlite_conn = sqlite_conn
        self._session_mgr = session_manager

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _emit(
        self,
        queue: asyncio.Queue,
        stage: str,
        message: str,
        percent: int,
        session_id: str | None = None,
    ) -> None:
        event = {"stage": stage, "message": message, "percent": percent}
        await queue.put(event)
        if self._session_mgr and session_id:
            self._session_mgr.log_event(session_id, stage, message)
        logger.info("[%s] %s (%d%%)", stage, message, percent)

    def _check_cancel(self) -> None:
        if self._session_mgr and self._session_mgr.is_cancelled():
            raise asyncio.CancelledError("Ingestion cancelled by user")

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    async def run(self, config: dict, progress_queue: asyncio.Queue) -> dict:
        """Execute all ingestion stages, pushing progress events to *progress_queue*.

        Raises CancelledError (caught in the background task wrapper) if
        session_manager.cancel_current() is called between stages.
        """
        repo_url: str = config["repo_url"]
        start_date: datetime = _iso(config["start_date"])
        end_date: datetime = _iso(config["end_date"])
        github_token: str | None = config.get("github_token")
        subdir: str | None = config.get("subdir")
        session_id: str | None = config.get("_session_id")

        summary: dict = {}

        try:
            # ----------------------------------------------------------------
            # Stage 1 — Clone
            # ----------------------------------------------------------------
            await self._emit(progress_queue, "cloning", f"Cloning {repo_url} …", 5, session_id)
            self._check_cancel()

            from ingestion.git_parser import GitParser

            os.makedirs(_CLONE_BASE, exist_ok=True)
            # Derive a stable directory name from the URL
            repo_slug = repo_url.rstrip("/").split("/")[-1].replace(".git", "")
            clone_dir = os.path.join(_CLONE_BASE, repo_slug)

            loop = asyncio.get_event_loop()
            clone_dir = await loop.run_in_executor(
                None, GitParser.clone, repo_url, clone_dir
            )
            await self._emit(progress_queue, "cloning", f"Clone ready at {clone_dir}", 12, session_id)
            summary["clone_dir"] = clone_dir

            # ----------------------------------------------------------------
            # Stage 2 — Parse code
            # ----------------------------------------------------------------
            self._check_cancel()
            await self._emit(progress_queue, "parsing_code", "Scanning file tree …", 15, session_id)

            from ingestion.code_parser import CodeParser

            git_parser = GitParser(clone_dir, start_date, end_date)
            file_tree = await loop.run_in_executor(None, git_parser.parse_file_tree)

            # Apply subdir filter if requested
            if subdir:
                file_tree = [f for f in file_tree if f["path"].startswith(subdir)]

            await self._emit(
                progress_queue, "parsing_code",
                f"Parsing {len(file_tree)} files …", 20, session_id,
            )
            self._check_cancel()

            # Only parse languages we support
            supported_exts = {".py", ".go", ".js", ".jsx", ".ts", ".tsx",
                              ".java", ".rs", ".c", ".h", ".cpp", ".cc", ".hpp"}
            parseable = [
                os.path.join(clone_dir, f["path"])
                for f in file_tree
                if Path(f["path"]).suffix.lower() in supported_exts
            ]

            code_parser = CodeParser(clone_dir)
            code_entities = await loop.run_in_executor(
                None, code_parser.parse_all, parseable
            )
            # Normalise paths back to repo-relative for graph consistency
            for e in code_entities:
                e["path"] = os.path.relpath(e["path"], clone_dir).replace("\\", "/")

            await self._emit(
                progress_queue, "parsing_code",
                f"Parsed {len(code_entities)} files, "
                f"{sum(len(e['functions']) for e in code_entities)} functions",
                35, session_id,
            )
            summary["files_parsed"] = len(code_entities)
            summary["functions_found"] = sum(len(e["functions"]) for e in code_entities)

            # ----------------------------------------------------------------
            # Stage 3 — Parse commits
            # ----------------------------------------------------------------
            self._check_cancel()
            await self._emit(
                progress_queue, "parsing_commits",
                f"Walking commits from {start_date.date()} to {end_date.date()} …",
                37, session_id,
            )

            commits = await loop.run_in_executor(None, git_parser.parse_commits)
            await self._emit(
                progress_queue, "parsing_commits",
                f"Found {len(commits)} commits in range", 45, session_id,
            )
            summary["commits"] = len(commits)

            # ----------------------------------------------------------------
            # Stage 4 — Fetch PRs (optional, best-effort)
            # ----------------------------------------------------------------
            self._check_cancel()
            prs: list[dict] = []
            await self._emit(
                progress_queue, "fetching_prs",
                "Fetching GitHub PRs …" if github_token else
                "Skipping PR fetch (no token provided)", 47, session_id,
            )

            if github_token:
                try:
                    from ingestion.github_parser import GitHubParser
                    gh = GitHubParser(
                        repo_url=repo_url,
                        token=github_token,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    prs = await loop.run_in_executor(None, gh.fetch_prs)
                    await self._emit(
                        progress_queue, "fetching_prs",
                        f"Fetched {len(prs)} PRs", 52, session_id,
                    )
                except Exception as exc:
                    await self._emit(
                        progress_queue, "fetching_prs",
                        f"PR fetch failed (non-fatal): {exc}", 52, session_id,
                    )
            summary["prs"] = len(prs)

            # ----------------------------------------------------------------
            # Stage 5 — Build knowledge graph
            # ----------------------------------------------------------------
            self._check_cancel()
            await self._emit(
                progress_queue, "building_graph",
                "Building Kuzu knowledge graph …", 53, session_id,
            )

            from ingestion.graph_builder import GraphBuilder

            builder = GraphBuilder(self._kuzu_conn)
            graph_summary = await loop.run_in_executor(
                None,
                builder.build_from_parsed,
                commits,
                file_tree,
                code_entities,
            )
            await self._emit(
                progress_queue, "building_graph",
                f"Graph: {graph_summary['files_inserted']} files, "
                f"{graph_summary['functions_inserted']} functions, "
                f"{graph_summary['commits_inserted']} commits, "
                f"{graph_summary['edges_created']} edges",
                65, session_id,
            )
            summary.update(graph_summary)

            if prs:
                self._check_cancel()
                pr_summary = await loop.run_in_executor(
                    None, builder.build_pr_nodes, prs
                )
                summary["prs_in_graph"] = pr_summary["prs_inserted"]

            # ----------------------------------------------------------------
            # Stage 6 — Embed
            # ----------------------------------------------------------------
            self._check_cancel()
            await self._emit(
                progress_queue, "embedding",
                "Embedding files, commits, and PRs …", 67, session_id,
            )

            from ingestion.embedder import Embedder

            embedder = Embedder(self._chroma_col)

            # Enrich file_tree with content paths for the embedder
            enriched_files = [
                {**f, "path": os.path.join(clone_dir, f["path"]).replace("\\", "/")}
                for f in file_tree
            ]
            n_file_chunks = await loop.run_in_executor(
                None, embedder.embed_files, enriched_files, clone_dir
            )
            self._check_cancel()
            n_commits_emb = await loop.run_in_executor(
                None, embedder.embed_commits, commits
            )
            n_pr_emb = 0
            if prs:
                n_pr_emb = await loop.run_in_executor(
                    None, embedder.embed_prs, prs
                )

            await self._emit(
                progress_queue, "embedding",
                f"Embedded {n_file_chunks} file chunks, {n_commits_emb} commits, "
                f"{n_pr_emb} PR documents",
                85, session_id,
            )
            summary["chunks_embedded"] = n_file_chunks + n_commits_emb + n_pr_emb

            # ----------------------------------------------------------------
            # Stage 7 — Build BM25 index
            # ----------------------------------------------------------------
            self._check_cancel()
            await self._emit(
                progress_queue, "building_bm25",
                "Building BM25 symbol index …", 88, session_id,
            )

            from state.bm25_index import BM25Index

            bm25_docs: list[dict] = []

            # Function names + signatures
            for entity in code_entities:
                fp = entity["path"]
                for fn in entity.get("functions", []):
                    text = f"{fn['name']} {fn.get('signature', '')} {fp}"
                    bm25_docs.append({
                        "id": f"fn::{fp}::{fn['name']}::{fn.get('start_line', 0)}",
                        "text": text,
                        "metadata": {"type": "function", "file": fp},
                    })
                for cls in entity.get("classes", []):
                    bm25_docs.append({
                        "id": f"cls::{fp}::{cls['name']}",
                        "text": f"{cls['name']} {fp}",
                        "metadata": {"type": "class", "file": fp},
                    })
                # File name itself
                bm25_docs.append({
                    "id": f"file::{fp}",
                    "text": fp.replace("/", " ").replace("_", " ").replace(".", " "),
                    "metadata": {"type": "file", "file": fp},
                })

            # Commit messages
            for c in commits:
                bm25_docs.append({
                    "id": f"commit::{c['hash']}",
                    "text": c.get("message", ""),
                    "metadata": {"type": "commit", "hash": c["hash"]},
                })

            bm25 = BM25Index(self._sqlite_conn)
            await loop.run_in_executor(None, bm25.build, bm25_docs)

            await self._emit(
                progress_queue, "building_bm25",
                f"BM25 index built: {len(bm25_docs)} symbols", 95, session_id,
            )
            summary["bm25_docs"] = len(bm25_docs)

            # ----------------------------------------------------------------
            # Done
            # ----------------------------------------------------------------
            await self._emit(
                progress_queue, "complete",
                "Ingestion complete", 100, session_id,
            )
            summary["status"] = "complete"
            return summary

        except asyncio.CancelledError:
            await self._emit(
                progress_queue, "error", "Ingestion cancelled", 0, session_id
            )
            raise
        except Exception as exc:
            logger.exception("Ingestion failed: %s", exc)
            await self._emit(
                progress_queue, "error", str(exc), 0, session_id
            )
            raise
