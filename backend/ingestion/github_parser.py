import logging
import re
import time
from datetime import datetime, timezone

from github import Github, Auth, GithubException, RateLimitExceededException

logger = logging.getLogger(__name__)

# SHA-1 pattern used to extract linked commit hashes from PR body text
_SHA_RE = re.compile(r"\b([0-9a-f]{40})\b", re.IGNORECASE)


def _ensure_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_owner_repo(repo_url: str) -> tuple[str, str]:
    """Return (owner, repo) from a GitHub URL or 'owner/repo' string."""
    url = repo_url.strip().rstrip("/")
    # Strip .git suffix if present
    if url.endswith(".git"):
        url = url[:-4]
    # https://github.com/owner/repo  or  http://github.com/owner/repo
    m = re.search(r"github\.com[:/]([^/]+)/([^/]+)$", url)
    if m:
        return m.group(1), m.group(2)
    # owner/repo
    parts = url.split("/")
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Cannot parse owner/repo from: {repo_url!r}")


class GitHubParser:
    def __init__(
        self,
        repo_url: str,
        token: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ):
        self.owner, self.repo_name = _parse_owner_repo(repo_url)
        self.token = token
        self.start_date = _ensure_aware(start_date)
        self.end_date = _ensure_aware(end_date)

        if token:
            self._gh = Github(auth=Auth.Token(token))
        else:
            logger.warning(
                "No GitHub token provided — unauthenticated access is limited to "
                "60 requests/hour. Provide a token via GITHUB_TOKEN for full access."
            )
            self._gh = Github()

        self._repo = self._gh.get_repo(f"{self.owner}/{self.repo_name}")

    # ------------------------------------------------------------------
    # Rate limit helpers
    # ------------------------------------------------------------------

    def _reset_wait(self) -> None:
        """Sleep until the rate-limit window resets (+ 5 s buffer)."""
        try:
            reset_at = self._gh.get_rate_limit().resources.core.reset
            reset_at = _ensure_aware(reset_at)
            now = datetime.now(tz=timezone.utc)
            wait = max(0.0, (reset_at - now).total_seconds()) + 5
            logger.info("Rate limit hit — sleeping %.0f s until %s", wait, reset_at)
            time.sleep(wait)
        except Exception:
            logger.warning("Could not determine reset time; sleeping 60 s")
            time.sleep(60)

    def _call_with_retry(self, fn, *args, **kwargs):
        """Call *fn* with automatic retry on RateLimitExceededException.

        If no token is set and the rate limit is hit, log and re-raise so the
        caller can return partial results instead of hanging.
        """
        while True:
            try:
                return fn(*args, **kwargs)
            except RateLimitExceededException:
                if not self.token:
                    raise
                self._reset_wait()

    # ------------------------------------------------------------------
    # PR fetching
    # ------------------------------------------------------------------

    def fetch_prs(self) -> list[dict]:
        """Fetch closed/merged PRs within the configured date range."""
        results: list[dict] = []
        try:
            # Sort newest-first; stop early when updated_at < start_date
            pulls = self._repo.get_pulls(
                state="closed",
                sort="updated",
                direction="desc",
            )
            for pr in pulls:
                # Safe early-exit: nothing older can have been merged in range
                updated = _ensure_aware(pr.updated_at)
                if self.start_date and updated and updated < self.start_date:
                    break

                merged_at = _ensure_aware(pr.merged_at)
                if merged_at is None:
                    continue  # not merged, just closed
                if self.end_date and merged_at > self.end_date:
                    continue
                if self.start_date and merged_at < self.start_date:
                    continue

                results.append(self._extract_pr(pr))
        except RateLimitExceededException:
            logger.error(
                "GitHub rate limit exceeded with no token — returning %d PRs fetched so far. "
                "Set GITHUB_TOKEN to continue.",
                len(results),
            )
        except GithubException as exc:
            logger.error("GitHub API error while fetching PRs: %s", exc)

        return results

    def _extract_pr(self, pr) -> dict:
        # Collect commit SHAs: merge commit + any 40-char hex in the body
        sha_set: set[str] = set()
        if pr.merge_commit_sha:
            sha_set.add(pr.merge_commit_sha)
        if pr.body:
            sha_set.update(_SHA_RE.findall(pr.body))

        # Review comments (one API call per PR — skipped gracefully on error)
        review_comments: list[dict] = []
        try:
            for rc in self._call_with_retry(pr.get_review_comments):
                review_comments.append({
                    "author": rc.user.login if rc.user else None,
                    "body": rc.body,
                    "created_at": rc.created_at.isoformat() if rc.created_at else None,
                })
        except RateLimitExceededException:
            logger.warning(
                "Rate limit hit while fetching review comments for PR #%d — skipping",
                pr.number,
            )
        except GithubException as exc:
            logger.debug("Could not fetch review comments for PR #%d: %s", pr.number, exc)

        return {
            "number": pr.number,
            "title": pr.title,
            "body": pr.body or "",
            "merged_at": pr.merged_at.isoformat() if pr.merged_at else None,
            "author_login": pr.user.login if pr.user else None,
            "linked_commit_shas": sorted(sha_set),
            "review_comments": review_comments,
            "labels": [lbl.name for lbl in pr.labels],
        }

    # ------------------------------------------------------------------
    # Issue fetching
    # ------------------------------------------------------------------

    def fetch_issues(self) -> list[dict]:
        """Fetch closed issues within the configured date range."""
        results: list[dict] = []
        try:
            kwargs: dict = {"state": "closed", "sort": "updated", "direction": "desc"}
            if self.start_date:
                kwargs["since"] = self.start_date

            issues = self._repo.get_issues(**kwargs)
            for issue in issues:
                # Skip pull requests (GitHub issues endpoint includes PRs)
                if issue.pull_request:
                    continue

                closed_at = _ensure_aware(issue.closed_at)
                if closed_at is None:
                    continue
                if self.end_date and closed_at > self.end_date:
                    continue
                if self.start_date and closed_at < self.start_date:
                    # updated_at >= closed_at so once we're below start we can stop
                    updated = _ensure_aware(issue.updated_at)
                    if updated and updated < self.start_date:
                        break
                    continue

                results.append({
                    "number": issue.number,
                    "title": issue.title,
                    "body": issue.body or "",
                    "closed_at": issue.closed_at.isoformat() if issue.closed_at else None,
                    "author_login": issue.user.login if issue.user else None,
                    "labels": [lbl.name for lbl in issue.labels],
                })
        except RateLimitExceededException:
            logger.error(
                "GitHub rate limit exceeded with no token — returning %d issues fetched so far.",
                len(results),
            )
        except GithubException as exc:
            logger.error("GitHub API error while fetching issues: %s", exc)

        return results


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from datetime import timezone

    start = datetime(2016, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2016, 6, 1, tzinfo=timezone.utc)

    # Use retry=0 so PyGithub raises RateLimitExceededException immediately
    # instead of sleeping for thousands of seconds internally.
    no_retry_gh = Github(retry=0)
    try:
        repo = no_retry_gh.get_repo("etcd-io/etcd")
    except RateLimitExceededException:
        print("Rate limit already exhausted -- re-run after the reset window or set GITHUB_TOKEN.")
        raise SystemExit(0)

    print("Fetching PRs from etcd-io/etcd (2016-01-01 to 2016-06-01, unauthenticated)...")
    print("Note: unauthenticated limit is 60 req/hr -- printing first 3 in range then exiting.\n")

    # Fetch oldest-first so the first hits are likely in the 2016 window.
    # Use created+asc: etcd's earliest PRs are from 2013; page through until
    # we reach our window, stopping after 3 matches OR on rate-limit.
    count = 0
    try:
        pulls = repo.get_pulls(state="closed", sort="created", direction="asc")
        for pr in pulls:
            created = _ensure_aware(pr.created_at)
            # Stop scanning once we're past the end window
            if created and created > end:
                break

            merged_at = _ensure_aware(pr.merged_at)
            if merged_at is None:
                continue
            if merged_at < start or merged_at > end:
                continue

            # Fetch review comments (may exhaust rate limit here)
            review_comments: list[dict] = []
            try:
                for rc in pr.get_review_comments():
                    review_comments.append({
                        "author": rc.user.login if rc.user else None,
                        "body": rc.body,
                        "created_at": rc.created_at.isoformat() if rc.created_at else None,
                    })
            except RateLimitExceededException:
                raise  # propagate to outer handler
            except GithubException:
                pass  # skip comments if unavailable

            sha_set: set[str] = set()
            if pr.merge_commit_sha:
                sha_set.add(pr.merge_commit_sha)
            if pr.body:
                sha_set.update(_SHA_RE.findall(pr.body))

            print(
                f"PR #{pr.number:<5} | {pr.title[:55]:<55} | "
                f"{len(review_comments)} review comment(s)"
            )
            count += 1
            if count >= 3:
                break

    except RateLimitExceededException:
        print(f"\nRate limit hit after {count} PR(s) -- expected without a token.")

    print(f"\nDone. Printed {count} PR(s).")
