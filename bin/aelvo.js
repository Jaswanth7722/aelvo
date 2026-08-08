#!/usr/bin/env node
/**
 * aelvo.js — `Aelvo` command launcher.
 *
 * `npm install -g aelvo` (or `npm install -g Aelvo`) puts this on PATH.
 * Running `Aelvo` opens the *current working directory* as the working
 * folder — like claude/codex/codebuff, the command is the activation and it
 * works from any folder. `Aelvo <folder>` opens any folder.
 *
 * The heavy lifting lives in the Python CLI (`python -m cli`); this wrapper:
 *   1. Locates the Python interpreter (venv from postinstall, else system).
 *   2. Bootstraps the venv on first run if it is missing.
 *   3. Sets AELVO_DATA_DIR (~/.aelvo by default) so global state stays out of
 *      the node_modules tree.
 *   4. Spawns the CLI with cwd = the user's current folder and forwards args.
 */
"use strict";

const { spawn } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");

const PKG_ROOT = path.join(__dirname, "..");

function log(msg) {
  process.stderr.write(`[Aelvo] ${msg}\n`);
}

/** Resolve the python interpreter (venv first, then system python). */
function resolvePython() {
  const candidates = [
    path.join(PKG_ROOT, ".venv", "Scripts", "python.exe"), // Windows venv
    path.join(PKG_ROOT, ".venv", "bin", "python"), // POSIX venv
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  // Fall back to a system interpreter; the CLI prints a clear error if deps
  // are missing rather than crashing opaquely.
  return process.env.AELVO_PYTHON || "python";
}

/** Bootstrap the venv + pip install CLI requirements (postinstall also runs this). */
function ensureVenv() {
  const venvPython = path.join(PKG_ROOT, ".venv", "Scripts", "python.exe");
  const venvPythonPosix = path.join(PKG_ROOT, ".venv", "bin", "python");
  if (fs.existsSync(venvPython) || fs.existsSync(venvPythonPosix)) {
    return true;
  }
  // Power users / CI can skip the (slow) first-run pip bootstrap and rely on
  // a pre-installed system Python environment.
  if (process.env.AELVO_SKIP_BOOTSTRAP === "1") {
    log("AELVO_SKIP_BOOTSTRAP=1 — using system Python without pip install.");
    return true;
  }
  log("First run — creating Python environment (this takes a couple of minutes)...");
  const installer = path.join(PKG_ROOT, "bin", "install.js");
  if (!fs.existsSync(installer)) {
    log("No installer found; using system Python.");
    return true;
  }
  try {
    const { spawnSync } = require("child_process");
    const res = spawnSync(process.execPath, [installer], {
      stdio: "inherit",
      cwd: PKG_ROOT,
    });
    return res.status === 0;
  } catch (err) {
    log(`Environment bootstrap failed: ${err.message}`);
    return true; // let the CLI try system python and surface a clear error
  }
}

/** Map the user's request to (cwd, argv) for the Python CLI. */
function main() {
  const args = process.argv.slice(2);

  if (!ensureVenv()) {
    process.exit(1);
  }

  const python = resolvePython();
  // Keep global state (vault, global memory, logs, cache) out of node_modules.
  const env = { ...process.env };
  if (!env.AELVO_DATA_DIR) {
    env.AELVO_DATA_DIR = path.join(os.homedir(), ".aelvo");
  }
  // Make the package importable regardless of cwd (python -m cli).
  env.PYTHONPATH = PKG_ROOT + (env.PYTHONPATH ? path.delimiter + env.PYTHONPATH : "");

  // cwd stays the user's shell working directory. The CLI resolves a folder
  // argument against that cwd itself (os.getcwd()), so `Aelvo ./proj` opens
  // `./proj` relative to where the user typed the command — chdir-ing here
  // would double-resolve relative paths into `proj/proj`.
  const child = spawn(python, ["-m", "cli", ...args], {
    cwd: process.cwd(),
    env,
    stdio: "inherit",
  });
  child.on("error", (err) => {
    log(`Failed to start Python: ${err.message}`);
    log("Install Python 3.10+ and ensure it is on PATH, then run `npm rebuild aelvo`.");
    process.exit(1);
  });
  child.on("exit", (code, signal) => {
    if (signal) {
      process.kill(process.pid, signal);
    } else {
      process.exit(code == null ? 0 : code);
    }
  });
}

main();
