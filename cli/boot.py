"""
boot.py — Lean backend bootstrap for the dedicated terminal CLI.

``cli/__main__.py`` (``Aelvo``) boots *only* the components the terminal
REPL actually uses — the kernel, jailed filesystem, hybrid memory engine,
tool registry, LLM agent, orchestrator, provider runtime and the runtime
status CLI — and skips the heavy optional subsystems the full web boot drags
in (MCP platform discovery, long-horizon planning, repo-intelligence scans,
cognitive engine). The prompt appears in a couple of seconds instead of after
the whole platform boots.

The working-folder/path globals live on ``main`` (the same module the web
boot uses) so the folder switcher, tool wrappers and system-prompt paths stay
in sync between the two entry points.

Folder semantics: ``Aelvo`` opens the **current working directory**; ``Aelvo
<folder>`` (or ``-w <folder>``) opens any folder. Per-folder state (memory
DB, anchor, backups) lives in a hidden ``<folder>/.aelvo/`` directory so the
user's project tree stays clean and each folder gets isolated memory.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger("aelvo.cli.boot")


async def boot_backend(*, project: str = "", workspace_dir: str = "") -> dict:
    """Build the backend components the CLI needs; returns ``run_cli`` kwargs.

    Folder semantics (no more named-workspace registry):
      * ``workspace_dir`` empty → open the **current working directory**.
      * ``workspace_dir`` set → open that folder (``Aelvo <folder>`` / ``-w``).

    Per-folder state lives in ``<folder>/.aelvo/`` (memory.db, anchor.md,
    backups) so any folder can be opened and the user's tree stays clean.

    Args:
        project: Accepted for web-boot compatibility; ignored by the CLI
            (folders replace the named-project registry).
        workspace_dir: Open a specific folder; defaults to ``os.getcwd()``.
    """
    import main as _main

    # ── folder resolution ────────────────────────────────────────────────
    if workspace_dir:
        target = os.path.abspath(os.path.expanduser(workspace_dir))
    else:
        target = os.getcwd()
    os.makedirs(target, exist_ok=True)
    if not os.path.isdir(target):
        raise NotADirectoryError(f"Not a folder: {target}")
    ws_name = os.path.basename(os.path.normpath(target)) or "default"
    _main._ws_name = ws_name
    _main.WORKSPACE_PATH = target
    _main.DB_PATH, _main.ANCHOR_PATH, _main.BACKUP_DIR = _main._folder_state_paths(
        target
    )

    # Keep the system prompt module in sync with the active folder.
    from core.system_prompt import configure_paths

    configure_paths(
        db_path=_main.DB_PATH,
        anchor_path=_main.ANCHOR_PATH,
        workspace_path=_main.WORKSPACE_PATH,
    )

    # Scaffold hidden .aelvo/ state + anchor (fresh folders otherwise crash
    # with "FATAL: Anchor file missing" inside AelvoKernel).
    _main._scaffold_folder_state(target, ws_name)

    # ── provider detection (env → encrypted vault) ────────────────────────
    # Use the same registry as cli/providers.py (/provider command) so the
    # detected provider at boot and any runtime /provider switch stay
    # consistent. (main.py's web boot uses core.registry's older registry.)
    from core.registry.models import MODEL_REGISTRY
    from core.startup import detect_provider

    try:
        provider_name, provider_config, api_key, model = detect_provider(
            MODEL_REGISTRY
        )
    except Exception as exc:
        log.debug("Provider detection failed: %s", exc)
        provider_name = provider_config = api_key = model = None

    # ── core components ───────────────────────────────────────────────────
    from core.execution import AelvoKernel
    from core.filesystem import AelvoFileSystem
    from core.governance import MemoryEngine

    aelvo_kernel = AelvoKernel(
        db_path=_main.DB_PATH,
        anchor_path=_main.ANCHOR_PATH,
        backup_dir=_main.BACKUP_DIR,
    )
    log.debug("AelvoKernel initialized (commands, audit trail, state)")

    fs = AelvoFileSystem(base_path=_main.WORKSPACE_PATH, kernel=aelvo_kernel)
    log.debug("AelvoFileSystem jailed to: %s", _main.WORKSPACE_PATH)

    memory_engine = MemoryEngine(
        db_path=_main.DB_PATH,
        anchor_path=_main.ANCHOR_PATH,
        tool_registry={},  # populated below
        project_name=ws_name,
    )
    log.debug("MemoryEngine initialized (hybrid: SQLite + Vector)")

    # Tool registry — the same wrappers the web boot uses.
    tool_registry = _main.build_tool_registry(fs, aelvo_kernel, memory_engine)

    # Vector RAG integration (concept similarity search).
    from core.rag import MemorySearcher

    searcher = MemorySearcher(chroma_collection=memory_engine.memory_collection)
    tool_registry["search_memory"] = {
        "fn": searcher.search,
        "required_constraints": [],
        "constraints_map": {},
    }

    from tools import build_extended_tool_registry

    tool_registry.update(build_extended_tool_registry(fs, aelvo_kernel, memory_engine))
    memory_engine.tools = tool_registry

    # ── provider runtime + agent ──────────────────────────────────────────
    provider_runtime = None
    try:
        from core.provider_runtime import init_provider_runtime

        provider_runtime = await init_provider_runtime()
        # Mirror the web boot: persist the detected key into the vault.
        if provider_runtime and api_key and provider_name:
            try:
                import time
                import uuid

                from auth.types import Credential, CredentialType

                cred = Credential(
                    id=f"key_{provider_name}_{uuid.uuid4().hex[:8]}",
                    provider=provider_name,
                    credential_type=CredentialType.API_KEY,
                    value=api_key,
                    label=f"{provider_name} API key (from env, CLI boot)",
                    created_at=time.time(),
                    is_valid=True,
                    metadata={"model": model or "", "source": "env_or_cli"},
                )
                provider_runtime.credential_store.store(cred)
            except Exception as exc:
                log.warning("Failed to store credential for %s: %s", provider_name, exc)
        log.debug(
            "Provider runtime initialized (%d providers, %d models)",
            len(provider_runtime.provider_configs),
            len(provider_runtime.model_registry.list_models()),
        )
    except Exception as exc:
        log.warning("Provider runtime init skipped: %s", exc)

    agent = None
    if provider_config is not None:
        agent = _main.AelvoAgent(
            api_key=api_key,
            model=model,
            provider_name=provider_name,
            provider_config=provider_config,
            provider_runtime=provider_runtime,
        )
        _main._ACTIVE_AGENT.set(agent)
        log.info("Using provider: %s | Model: %s", provider_name, model)
    else:
        log.warning(
            "No LLM provider configured — booting with agent=None. "
            "Type /provider in the CLI to configure one."
        )

    # ── orchestrator ──────────────────────────────────────────────────────
    from core.orchestration import Orchestrator

    orchestrator = Orchestrator(
        memory_engine=memory_engine,
        kernel=aelvo_kernel,
        base_path=_main.WORKSPACE_PATH,
        provider_runtime=provider_runtime,
    )
    log.debug("Orchestrator coordinate systems online")

    # ── runtime status CLI (backs /status and #status) ────────────────────
    runtime_cli = None
    try:
        from runtime_next.monitoring.cli import RuntimeCLI
        from runtime_next.monitoring.dashboard import RuntimeDashboard

        runtime_cli = RuntimeCLI(dashboard=RuntimeDashboard())
        log.debug("RuntimeCLI initialized for #status commands")
    except Exception as exc:
        log.warning("RuntimeCLI init skipped: %s", exc)

    return {
        "agent": agent,
        "orchestrator": orchestrator,
        "memory_engine": memory_engine,
        "aelvo_kernel": aelvo_kernel,
        "db_path": _main.DB_PATH,
        "workspace_path": _main.WORKSPACE_PATH,
        "project": ws_name,
        "mcp_cli": None,  # MCP subsystem is web-boot only; the orchestrator
        # accepts None (guarded at every call site).
        "runtime_cli": runtime_cli,
        "provider_runtime": provider_runtime,
        "fs": fs,
        "provider_name": provider_name,
        "model": model,
    }
