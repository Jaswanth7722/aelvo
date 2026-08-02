"""
cli — AELVO Terminal CLI (CodeBuff / Claude Code style).

A lightweight interactive terminal agent that reuses the exact same backend
as the web dashboard (kernel, filesystem, memory, orchestrator, MCP). It adds
zero new dependencies: rendering is built on ``rich`` and the REPL on
``prompt_toolkit``, both already in ``requirements.txt``.

Usage:
    python main.py --cli                  # interactive REPL
    python main.py --cli --ask "prompt"   # one-shot: run a single prompt and exit
"""
