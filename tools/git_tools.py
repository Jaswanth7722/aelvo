# git_tools.py - Git Repository Intelligence Tools for AELVO OMEGA

import subprocess
import re
from pathlib import Path
from typing import Dict, Any, List, Tuple

def _run_git_cmd(args: List[str], cwd: str) -> Tuple[int, str, str]:
    """Runs a Git shell command safely in the specified working directory."""
    try:
        res = subprocess.run(
            ["git"] + args,
            capture_output=True,
            text=True,
            cwd=cwd,
            timeout=10
        )
        return res.returncode, res.stdout.strip(), res.stderr.strip()
    except Exception as e:
        return -1, "", str(e)

def get_git_state(workspace_root: str) -> Dict[str, Any]:
    """Inspects the local git repository scope to compile files changed, recent logs, and stashes."""
    if not (Path(workspace_root) / ".git").exists():
        return {
            "status": "error",
            "logs": "Workspace is not a git repository.",
            "executed": {}
        }
    
    code, branch, _ = _run_git_cmd(["rev-parse", "--abbrev-ref", "HEAD"], workspace_root)
    if code != 0:
        return {"status": "error", "logs": "Failed to get current branch. Repository might have no commits.", "executed": {}}

    _, status_raw, _ = _run_git_cmd(["status", "--porcelain"], workspace_root)
    
    staged = []
    unstaged = []
    untracked = []
    
    for line in status_raw.splitlines():
        if len(line) < 4: continue
        x, y = line[0], line[1]
        file_path = line[3:].strip()
        
        # Staged files (X is staged, Y is unstaged)
        if x in ("A", "M", "D", "R"):
            staged.append(file_path)
        # Unstaged files (Y is unstaged)
        if y in ("M", "D"):
            unstaged.append(file_path)
        # Untracked files
        if x == "?" and y == "?":
            untracked.append(file_path)

    # 5 most recent commits
    _, log_raw, _ = _run_git_cmd(["log", "-n", "5", "--oneline"], workspace_root)
    commits = log_raw.splitlines() if log_raw else []

    # Stash count
    _, stash_raw, _ = _run_git_cmd(["stash", "list"], workspace_root)
    stash_count = len(stash_raw.splitlines()) if stash_raw else 0

    return {
        "status": "success",
        "logs": f"Fetched repository state on branch '{branch}'.",
        "executed": {"branch": branch, "staged_count": len(staged)},
        "data": {
            "branch": branch,
            "staged_files": staged,
            "unstaged_files": unstaged,
            "untracked_files": untracked,
            "recent_commits": commits,
            "stash_count": stash_count
        }
    }

def generate_commit_message(workspace_root: str) -> Dict[str, Any]:
    """Analyzes working git directory diffs to formulate clean conventional commits."""
    state = get_git_state(workspace_root)
    if state["status"] == "error":
        return state

    staged_files = state["data"]["staged_files"]
    if not staged_files:
        return {
            "status": "error",
            "logs": "No files are staged for commit. Use 'git add' first.",
            "executed": {}
        }

    # Run git diff --staged
    code, diff_raw, _ = _run_git_cmd(["diff", "--staged"], workspace_root)
    if code != 0 or not diff_raw:
        return {"status": "error", "logs": "Failed to retrieve staged diff.", "executed": {}}

    # Parse diff and categorize changes
    # Conventional commit types: feat, fix, docs, refactor, test, chore, perf, ci
    change_type = "chore"
    scope = ""
    
    file_ex = [Path(f).suffix.lower() for f in staged_files]
    file_names = [Path(f).name.lower() for f in staged_files]

    # Heuristic classifications
    if any(name.startswith("test") or "test" in name for name in file_names):
        change_type = "test"
    elif any(ext in (".md", ".txt") or "docs" in name for ext, name in zip(file_ex, file_names)):
        change_type = "docs"
    elif any(name in ("package.json", "cargo.toml", "go.mod", "requirements.txt", ".gitignore") for name in file_names):
        change_type = "chore"
    elif any("fix" in line.lower() or "error" in line.lower() or "bug" in line.lower() for line in diff_raw.splitlines()[:50]):
        change_type = "fix"
    elif any(ext in (".py", ".ts", ".tsx", ".rs", ".go") for ext in file_ex):
        # Default code file edits
        change_type = "feat"
        if len(staged_files) == 1:
            scope = Path(staged_files[0]).stem

    # Construct description based on additions/deletions in diff
    additions = re.findall(r"^\+([^+].*)$", diff_raw, re.MULTILINE)
    deletions = re.findall(r"^-([^-].*)$", diff_raw, re.MULTILINE)
    
    # Analyze added symbols (classes, functions)
    added_functions = []
    for line in additions:
        match = re.search(r"\b(?:def|class|function|fn)\s+(\w+)", line)
        if match:
            added_functions.append(match.group(1))

    summary = f"updated {len(staged_files)} file(s)"
    if added_functions:
        summary = f"add {', '.join(added_functions[:2])}"
    elif len(staged_files) == 1:
        summary = f"update {Path(staged_files[0]).name}"

    # Build final Conventional Commit Message
    scope_str = f"({scope})" if scope else ""
    subject = f"{change_type}{scope_str}: {summary}"
    if len(subject) > 72:
        subject = subject[:69] + "..."

    body_lines = [
        f"Modified files: {', '.join(staged_files)}",
        f"Total changes: +{len(additions)} additions, -{len(deletions)} deletions."
    ]
    if added_functions:
        body_lines.append(f"Newly introduced classes/functions: {', '.join(added_functions)}")

    commit_message = f"{subject}\n\n" + "\n".join(body_lines)

    return {
        "status": "success",
        "logs": "Conventional commit message generated successfully.",
        "executed": {"staged_files": len(staged_files)},
        "data": {
            "subject": subject,
            "body": "\n".join(body_lines),
            "full_message": commit_message
        }
    }

def detect_merge_conflicts(workspace_root: str) -> Dict[str, Any]:
    """Scans the repository workspace for unresolved git conflict tokens."""
    state = get_git_state(workspace_root)
    if state["status"] == "error":
        return state

    conflict_files = []
    conflict_markers = [
        re.compile(r"^<<<<<<< "),
        re.compile(r"^=======$"),
        re.compile(r"^>>>>>>> ")
    ]

    # Walk files in git state that are modified/unstaged/staged
    all_modified = state["data"]["unstaged_files"] + state["data"]["untracked_files"] + state["data"].get("staged_files", [])
    for rel_path in all_modified:
        abs_p = Path(workspace_root) / rel_path
        if not abs_p.is_file(): continue
        
        try:
            with open(abs_p, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
                if "<<<<<<<" in content and "=======" in content and ">>>>>>>" in content:
                    # Parse specific line offsets
                    lines = content.splitlines()
                    markers_found = []
                    for idx, line in enumerate(lines, 1):
                        if line.startswith("<<<<<<<") or line.startswith("=======") or line.startswith(">>>>>>>"):
                            markers_found.append({"line": idx, "content": line})
                    
                    conflict_files.append({
                        "file": rel_path,
                        "markers": markers_found
                    })
        except Exception as _ex: print("Silenced exception: %s", _ex)

    return {
        "status": "success" if not conflict_files else "error",
        "logs": f"Scan completed. Found {len(conflict_files)} conflict file(s).",
        "executed": {"files_scanned": len(all_modified)},
        "data": conflict_files
    }

def generate_pr_description(base_branch: str, head_branch: str, workspace_root: str) -> Dict[str, Any]:
    """Compares head_branch to base_branch and formats a standard pull request template in Markdown."""
    # Find list of commits between branches
    code, commits_raw, _ = _run_git_cmd(["log", f"{base_branch}..{head_branch}", "--oneline"], workspace_root)
    if code != 0:
        return {"status": "error", "logs": f"Failed to compare {base_branch} and {head_branch}.", "executed": {}}

    commits_list = commits_raw.splitlines() if commits_raw else []

    # Get list of files changed
    _, files_raw, _ = _run_git_cmd(["diff", f"{base_branch}..{head_branch}", "--name-only"], workspace_root)
    files_changed = files_raw.splitlines() if files_raw else []

    pr_template = f"""# Pull Request Description

## Overview
Comparing `{head_branch}` into `{base_branch}`. 
This PR introduces the following core updates based on {len(commits_list)} commit(s):

### Key Changes
{chr(10).join([f'- {c}' for c in commits_list[:10]])}
{'- ...and more commits' if len(commits_list) > 10 else ''}

### Affected Files
{chr(10).join([f'- `{f}`' for f in files_changed])}

## Verification Plan
### Automated Verification
- [ ] Linter validation run successfully.
- [ ] Automated unit test suites passing.

### Manual Verification
- Verified operations inside REPL environment.
"""

    return {
        "status": "success",
        "logs": "Pull Request description synthesized successfully.",
        "executed": {"commits_compared": len(commits_list), "files_changed": len(files_changed)},
        "data": {"pr_markdown": pr_template}
    }
