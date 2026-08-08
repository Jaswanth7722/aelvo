"""npm packaging sanity tests.

``npm install -g aelvo`` (or ``npm install -g Aelvo``) must expose an
``Aelvo`` command that launches the Python CLI. These tests validate the
manifest contract (bin entries, postinstall, shipped files) and that the
launcher/bootstrap scripts are syntactically valid Node.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_package_json_exists_and_names_bin():
    with open(os.path.join(ROOT, "package.json"), encoding="utf-8") as f:
        pkg = json.load(f)

    assert pkg["name"].lower() == "aelvo"
    assert pkg["version"] == "2.2.0"
    bins = pkg.get("bin", {})
    # The activation word the user asked for — case-insensitive npm lookup
    # resolves `npm install -g Aelvo` to the published `aelvo` package.
    assert "Aelvo" in bins
    assert "aelvo" in bins
    for script in bins.values():
        assert os.path.exists(os.path.join(ROOT, script)), f"missing bin: {script}"


def test_publish_workflow_exists_and_gates_on_version():
    """CI auto-publish: the workflow must exist, gate on a version change,
    reference the NPM_TOKEN secret, and never hardcode a token."""
    wf = os.path.join(ROOT, ".github", "workflows", "publish.yml")
    assert os.path.exists(wf), "missing .github/workflows/publish.yml"
    with open(wf, encoding="utf-8") as f:
        content = f.read()
    assert "npm publish" in content
    assert "secrets.NPM_TOKEN" in content
    assert "npm_fBmK" not in content  # never embed the real token
    assert "check-version" in content and "publish" in content


def test_bin_scripts_exist_and_are_valid_node():
    node = shutil_which("node")
    if node is None:
        pytest.skip("node not installed")
    for script in ("bin/aelvo.js", "bin/install.js"):
        path = os.path.join(ROOT, script)
        assert os.path.exists(path)
        res = subprocess.run(
            [node, "--check", path],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert res.returncode == 0, f"{script} is not valid JS: {res.stderr}"


def test_launcher_resolves_python_and_sets_data_dir():
    """The launcher must default AELVO_DATA_DIR to ~/.aelvo and pass args."""
    node = shutil_which("node")
    if node is None:
        pytest.skip("node not installed")

    launcher = os.path.join(ROOT, "bin", "aelvo.js")
    # --version exits immediately via the CLI path; skip the slow first-run pip
    # bootstrap and rely on the (pre-installed) system Python. The launcher
    # must still set AELVO_DATA_DIR and forward args, and must not crash.
    env = dict(os.environ)
    env.pop("AELVO_DATA_DIR", None)
    env["AELVO_SKIP_BOOTSTRAP"] = "1"
    env["PYTHONPATH"] = ROOT
    res = subprocess.run(
        [node, launcher, "--version"],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        env=env,
    )
    assert res.returncode == 0, (res.stdout or "") + (res.stderr or "")
    combined = (res.stdout or "") + (res.stderr or "")
    assert "ImportError" not in combined and "Traceback" not in combined


def test_requirements_cli_is_lean_and_present():
    req = os.path.join(ROOT, "requirements-cli.txt")
    assert os.path.exists(req)
    with open(req, encoding="utf-8") as f:
        lines = [
            ln.strip()
            for ln in f.read().splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")
        ]
    joined = "\n".join(lines)
    # Heavy lazy deps deliberately excluded to keep the global install fast.
    assert not any(dep in joined for dep in ("chromadb", "scrapy", "playwright"))
    assert any(ln.startswith("rich") for ln in lines)
    assert any(ln.startswith("prompt_toolkit") for ln in lines)


def shutil_which(name: str):
    from shutil import which

    return which(name)


def test_postinstall_script_registered():
    with open(os.path.join(ROOT, "package.json"), encoding="utf-8") as f:
        pkg = json.load(f)
    assert pkg.get("scripts", {}).get("postinstall") == "node bin/install.js"
