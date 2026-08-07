"""
cli — AELVO Terminal CLI (CodeBuff / Claude Code style).

A lightweight interactive terminal agent that reuses the exact same backend
as the web dashboard (kernel, filesystem, memory, orchestrator). It adds
zero new dependencies: rendering is built on ``rich`` and the REPL on
``prompt_toolkit``, both already in ``requirements.txt``.

Two ways to launch:

    python -m cli                            # dedicated CLI (fast lean boot)
    python -m cli "prompt"                   # dedicated one-shot
    python -m cli -w ./folder --provider x   # dedicated + flags
    aelvo                                    # Windows alias (aelvo.bat)
    python main.py                           # full boot → CLI (default mode)
    python main.py --web                     # web dashboard

The dedicated entry (``cli/__main__.py`` + ``cli/boot.py``) skips the heavy
optional subsystems the web boot runs (MCP discovery, long-horizon planning,
repo scans), so the prompt appears in a couple of seconds.
"""
