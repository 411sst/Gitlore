import os
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import git

logger = logging.getLogger(__name__)

# Extension → language name
_EXT_LANG = {
    ".py": "python",
    ".go": "go",
    ".ts": "typescript",
    ".tsx": "tsx",
    ".js": "javascript",
    ".jsx": "jsx",
    ".java": "java",
    ".rs": "rust",
    ".c": "c",
    ".h": "c",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".cxx": "cpp",
    ".hpp": "cpp",
    ".rb": "ruby",
    ".cs": "csharp",
    ".swift": "swift",
    ".kt": "kotlin",
    ".scala": "scala",
    ".sh": "shell",
    ".md": "markdown",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".toml": "toml",
    ".xml": "xml",
    ".html": "html",
    ".css": "css",
}


def _lang_from_path(path: str) -> str:
    return _EXT_LANG.get(Path(path).suffix.lower(), "unknown")


def _ensure_aware(dt: datetime) -> datetime:
    """Return timezone-aware datetime (assume UTC if naive)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


class GitParser:
    def __init__(self, repo_path: str, start_date: datetime, end_date: datetime):
        self.repo_path = repo_path
        self.start_date = _ensure_aware(start_date)
        self.end_date = _ensure_aware(end_date)
        self._repo: git.Repo | None = None

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @staticmethod
    def clone(repo_url: str, target_dir: str) -> str:
        """Shallow-clone without file blobs (faster for history analysis).

        Returns the target directory path.
        """
        if os.path.exists(target_dir):
            try:
                logger.info("Refreshing existing clone at %s", target_dir)
                repo = git.Repo(target_dir)
                repo.git.fetch("origin", "--prune", "--filter=blob:none")

                remote_head = repo.git.rev_parse("--abbrev-ref", "origin/HEAD").strip()
                if "/" not in remote_head:
                    raise RuntimeError(f"Unexpected origin/HEAD format: {remote_head}")

                branch = remote_head.split("/", 1)[1]
                repo.git.checkout("-B", branch, remote_head)
                repo.git.reset("--hard", remote_head)
                repo.git.clean("-fd")
                return target_dir
            except Exception as exc:
                logger.warning(
                    "Failed to refresh existing clone at %s (%s). Re-cloning.",
                    target_dir,
                    exc,
                )
                shutil.rmtree(target_dir, ignore_errors=True)

        logger.info("Cloning %s → %s", repo_url, target_dir)
        git.Repo.clone_from(
            repo_url,
            target_dir,
            multi_options=["--filter=blob:none"],
        )
        logger.info("Clone complete.")
        return target_dir

    # ------------------------------------------------------------------
    # Core parsing
    # ------------------------------------------------------------------

    # Sentinel that marks the start of each commit record in git log output.
    # Chosen to be highly unlikely to appear in commit messages or file paths.
    _COMMIT_SENTINEL = "<<<GITLORE_COMMIT>>>"

    def parse_commits(self) -> list[dict]:
        """Return all commits whose author date falls within [start_date, end_date].

        Uses a single ``git log`` invocation with ``--numstat`` so no blob
        objects are fetched (safe for partial/blobless clones).
        """
        repo = git.Repo(self.repo_path)

        # Each commit header is emitted as one line: SENTINEL|hash|short|author|email|isodate
        # Followed by: blank line, list of changed files (one per line), blank line.
        # %s (subject) is the first line of the commit message — safe as a single line.
        fmt = f"{self._COMMIT_SENTINEL}|%H|%h|%s|%aN|%aE|%aI"

        raw = repo.git.log(
            f"--format={fmt}",
            "--numstat",
            "--diff-filter=ACDMRT",
            f"--after={self.start_date.isoformat()}",
            f"--before={self.end_date.isoformat()}",
        )

        results: list[dict] = []
        current: dict | None = None

        for line in raw.splitlines():
            if line.startswith(self._COMMIT_SENTINEL):
                if current is not None:
                    results.append(current)
                parts = line.split("|", 6)
                # parts: [SENTINEL, hash, short, subject, author, email, isodate]
                if len(parts) < 7:
                    current = None
                    continue
                _, h, sh, msg, author, email, ts_str = parts
                current = {
                    "hash": h,
                    "short_hash": sh,
                    "message": msg,
                    "author_name": author,
                    "author_email": email,
                    "timestamp": ts_str,
                    "changed_files": [],
                    "diff_stat": {"insertions": 0, "deletions": 0},
                }
            elif current is not None and line.strip():
                parts = line.split("\t", 2)
                if len(parts) == 3:
                    ins_raw, del_raw, path = parts
                    path = path.strip()
                    if path and path not in current["changed_files"]:
                        current["changed_files"].append(path)

                    if ins_raw.isdigit():
                        current["diff_stat"]["insertions"] += int(ins_raw)
                    if del_raw.isdigit():
                        current["diff_stat"]["deletions"] += int(del_raw)
                else:
                    path = line.strip()
                    if path and path not in current["changed_files"]:
                        current["changed_files"].append(path)

        if current is not None:
            results.append(current)

        return results

    def parse_file_tree(self) -> list[dict]:
        """Return all files tracked in the repo HEAD with language and size."""
        repo = git.Repo(self.repo_path)
        results = []

        for blob in repo.head.commit.tree.traverse():
            if blob.type != "blob":
                continue
            try:
                size = int(getattr(blob, "size", 0) or 0)
            except Exception:
                size = 0

            results.append(
                {
                    "path": blob.path,
                    "language": _lang_from_path(blob.path),
                    "size_bytes": size,
                }
            )

        return results


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json
    from datetime import timezone

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    REPO_URL = "https://github.com/etcd-io/etcd"
    TARGET_DIR = "/tmp/etcd_test"

    cloned = GitParser.clone(REPO_URL, TARGET_DIR)

    start = datetime(2016, 1, 1, tzinfo=timezone.utc)
    end = datetime(2017, 1, 1, tzinfo=timezone.utc)

    parser = GitParser(cloned, start, end)
    print("Parsing commits…")
    commits = parser.parse_commits()
    print(f"Total commits in range: {len(commits)}")
    print("\nFirst 5 commits:")
    for c in commits[:5]:
        print(json.dumps(c, indent=2))
