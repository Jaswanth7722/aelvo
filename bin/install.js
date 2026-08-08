#!/usr/bin/env node
/**
 * install.js — postinstall / first-run bootstrap for the Aelvo npm package.
 *
 * Creates a Python virtual environment inside the package (.venv) and
 * installs the CLI requirements from requirements-cli.txt. This is what makes
 * `npm install -g Aelvo` a genuine one-liner: after install, `Aelvo` just
 * works (no manual pip install step).
 *
 * The venv is created with --system-site-packages off so the package is fully
 * self-contained; the CLI-only requirements keep the install lean (the heavy
 * web/scraping extras are lazy-imported with graceful fallbacks).
 */
"use strict";

const { spawnSync } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");

const PKG_ROOT = path.join(__dirname, "..");
const REQS = path.join(PKG_ROOT, "requirements-cli.txt");

function log(msg) {
  process.stdout.write(`[Aelvo] ${msg}\n`);
}

function findPython() {
  const candidates = ["python3", "python", "py"];
  for (const name of candidates) {
    try {
      const res = spawnSync(name, ["--version"], { stdio: "pipe" });
      if (res.status === 0) return name;
    } catch (_err) {
      /* try next */
    }
  }
  return null;
}

function main() {
  if (!fs.existsSync(REQS)) {
    log(`requirements-cli.txt not found (${REQS}); skipping dependency install.`);
    return;
  }

  const python = findPython();
  if (!python) {
    log("Python not found on PATH. `Aelvo` needs Python 3.10+ — install it, then run `npm rebuild aelvo`.");
    return;
  }

  const venvDir = path.join(PKG_ROOT, ".venv");
  const venvPython = path.join(venvDir, "Scripts", "python.exe");
  const venvPythonPosix = path.join(venvDir, "bin", "python");

  if (fs.existsSync(venvPython) || fs.existsSync(venvPythonPosix)) {
    log("venv already exists — syncing requirements.");
  } else {
    log("Creating Python virtual environment...");
    const created = spawnSync(python, ["-m", "venv", venvDir], {
      stdio: "inherit",
      cwd: PKG_ROOT,
    });
    if (created.status !== 0) {
      log("venv creation failed; falling back to system Python.");
      return;
    }
  }

  const venvPy = fs.existsSync(venvPython) ? venvPython : venvPythonPosix;
  log("Installing CLI requirements (first install may take a couple of minutes)...");
  const pip = spawnSync(venvPy, ["-m", "pip", "install", "--disable-pip-version-check", "-r", REQS], {
    stdio: "inherit",
    cwd: PKG_ROOT,
    env: {
      ...process.env,
      PIP_NO_INPUT: "1",
    },
  });
  if (pip.status !== 0) {
    log("pip install failed — check the output above. The CLI will attempt system Python as a fallback.");
    return;
  }
  log("Aelvo ready. Run `Aelvo` from any folder, or `Aelvo <folder>` to open one.");
}

main();
