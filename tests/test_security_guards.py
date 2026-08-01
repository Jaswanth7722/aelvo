"""Regression tests for security guards added from REPORT_ANALYZE.md.

Covers:
- CRITICAL #3: tools/git_tools.py branch-name validation (injection guard)
- CRITICAL #4: tools/code_tools.py pytest -k test_filter sanitization
- HIGH #7:    core/scraping/web_scraping.py URL scheme allowlist (SSRF)
"""

import pytest

from tools.git_tools import generate_pr_description
from tools.code_tools import run_tests
from core.scraping.web_scraping import (
    _url_scheme_allowed,
    execute_heavy_crawl,
    execute_light_scrape,
)


# ============================================================================
# CRITICAL #3 — git branch-name validation
# ============================================================================


def test_generate_pr_description_rejects_malicious_base_branch():
    """A crafted base branch (shell metachars / git option prefix) is rejected."""
    result = generate_pr_description("main;rm -rf /", "feature/x", ".")
    assert result["status"] == "error"
    assert "Invalid base branch name" in result["logs"]


def test_generate_pr_description_rejects_malicious_head_branch():
    """A crafted head branch (git option injection) is rejected."""
    result = generate_pr_description("main", "--upload-pack=evil", ".")
    assert result["status"] == "error"
    assert "Invalid head branch name" in result["logs"]


def test_generate_pr_description_rejects_none_branches():
    """None branch names are rejected by the 'or \"\"' guard."""
    result = generate_pr_description(None, "feature/x", ".")
    assert result["status"] == "error"
    assert "Invalid base branch name" in result["logs"]


def test_generate_pr_description_accepts_valid_branches(monkeypatch):
    """Valid branch names pass validation and reach the git command layer."""
    called: list[list[str]] = []

    def fake_run_git_cmd(args, cwd):
        called.append(args)
        # log returns 2 fake commits; diff returns one file
        if args[0] == "log":
            return 0, "abc123 feat: one\nbcd456 feat: two\n", ""
        if args[0] == "diff":
            return 0, "src/foo.py\n", ""
        return 0, "", ""

    monkeypatch.setattr("tools.git_tools._run_git_cmd", fake_run_git_cmd)
    result = generate_pr_description("main", "feature/x", ".")

    assert result["status"] == "success"
    # The refspec must be interpolated exactly as provided (no injection layer).
    assert ["log", "main..feature/x", "--oneline"] in called


# ============================================================================
# CRITICAL #4 — pytest -k test_filter sanitization
# ============================================================================


def test_run_tests_rejects_malicious_test_filter(monkeypatch):
    """Expression-injection filters are rejected before any subprocess runs."""
    def boom(*args, **kwargs):
        raise AssertionError("_run must not be called for a rejected filter")

    monkeypatch.setattr("tools.code_tools._run", boom)
    result = run_tests(
        "test_foo.py", "python", "__import__('os').system('id')", "."
    )
    assert result["status"] == "error"
    assert "Invalid test_filter" in result["logs"]


def test_run_tests_rejects_shell_metacharacter_filter(monkeypatch):
    """Semicolons / parens in filters are rejected, not passed to pytest."""
    def boom(*args, **kwargs):
        raise AssertionError("_run must not be called for a rejected filter")

    monkeypatch.setattr("tools.code_tools._run", boom)
    result = run_tests("test_foo.py", "python", "test_a; rm -rf /", ".")
    assert result["status"] == "error"
    assert "Invalid test_filter" in result["logs"]


def test_run_tests_accepts_valid_test_filter(monkeypatch):
    """Legitimate -k expressions (words, spaces, or/and/not) are forwarded."""
    captured: dict = {}

    def fake_run(cmd, cwd, timeout, env=None):
        captured["cmd"] = cmd
        return {"returncode": 0, "stdout": "2 passed", "stderr": ""}

    monkeypatch.setattr("tools.code_tools._run", fake_run)
    result = run_tests("test_foo.py", "python", "test_a or test_b", ".")

    assert result["status"] == "success"
    assert "-k" in captured["cmd"]
    assert captured["cmd"][-1] == "test_a or test_b"


# ============================================================================
# HIGH #7 — web-scraping URL scheme allowlist (SSRF)
# ============================================================================


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/path",
        "https://example.com/path?q=1",
        "HTTP://example.com/UPPER",
    ],
)
def test_url_scheme_allowed_accepts_http_https(url):
    assert _url_scheme_allowed(url) is True


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "javascript:alert(1)",
        "ftp://example.com/file",
        "data:text/html,<script>alert(1)</script>",
        "//relative-without-scheme",
    ],
)
def test_url_scheme_allowed_rejects_non_http(url):
    assert _url_scheme_allowed(url) is False


def test_light_scrape_blocks_file_scheme():
    """execute_light_scrape refuses file:// URLs before any network access."""
    result = execute_light_scrape("file:///etc/passwd")
    assert result["status"] == "error"
    assert "Blocked URL scheme" in result["logs"]


def test_heavy_crawl_blocks_file_scheme():
    """execute_heavy_crawl refuses file:// URLs before spawning a crawler."""
    result = execute_heavy_crawl("file:///etc/passwd", kernel=None)
    assert result["status"] == "error"
    assert "Blocked URL scheme" in result["logs"]
