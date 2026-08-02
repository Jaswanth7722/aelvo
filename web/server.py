"""web/server.py — AELVO web-only launch server.

Serves the built React dashboard (``web/dist``) over HTTP and runs the
WebSocket event bridge so the browser receives live agent events and can
send user messages back to the orchestrator.

Usage (from main.py):

    from web.server import run_web
    await run_web(
        agent=agent,
        orchestrator=orchestrator,
        db_path=DB_PATH,
        host="127.0.0.1",
        http_port=8000,
        open_browser=True,
    )
"""

from __future__ import annotations

import asyncio
import logging
import webbrowser
from pathlib import Path

from aiohttp import web

from web.web_bridge import WebBridge

log = logging.getLogger("aelvo.web.server")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8000
DEFAULT_WS_PORT = 8765

# The built React app lives here (npm run build output).
_DIST_DIR = Path(__file__).resolve().parent / "dist"


async def _serve_file(request: web.Request) -> web.StreamResponse:
    """Serve static assets from web/dist with SPA fallback to index.html."""
    rel = request.rel_url.path.lstrip("/")
    if not rel:
        rel = "index.html"

    # Prevent path traversal outside the dist directory.
    target = (_DIST_DIR / rel).resolve()
    dist_root = _DIST_DIR.resolve()
    inside = target == dist_root or dist_root in target.parents
    if not inside or not target.exists():
        target = _DIST_DIR / "index.html"

    if not target.exists():
        raise web.HTTPNotFound(text="AELVO web build not found. Run `cd web && npm run build`.")

    content_type = _content_type_for(target.suffix)
    body = target.read_bytes()
    return web.Response(body=body, content_type=content_type)


def _content_type_for(suffix: str) -> str:
    mapping = {
        ".html": "text/html",
        ".js": "application/javascript",
        ".mjs": "application/javascript",
        ".css": "text/css",
        ".json": "application/json",
        ".svg": "image/svg+xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".ico": "image/x-icon",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
        ".ttf": "font/ttf",
        ".map": "application/json",
    }
    return mapping.get(suffix, "application/octet-stream")


async def run_web(
    agent=None,
    orchestrator=None,
    memory_engine=None,
    aelvo_kernel=None,
    db_path: str = "",
    host: str = DEFAULT_HOST,
    http_port: int = DEFAULT_HTTP_PORT,
    ws_port: int = DEFAULT_WS_PORT,
    open_browser: bool = True,
    mcp_cli=None,
    runtime_cli=None,
    provider_runtime=None,
) -> None:
    """Start the web-only AELVO interface and block until interrupted.

    The WebSocket bridge subscribes to the orchestrator's runtime EventBus
    so every runtime event streams to the browser in real time. User chat
    messages arriving over the socket are routed through
    ``orchestrator.execute_turn``.
    """
    # 1. Event bridge — streams runtime events to the browser.
    bridge = WebBridge(host=host, port=ws_port)
    if orchestrator is not None and hasattr(orchestrator, "runtime_bus"):
        bridge.subscribe_to_runtime(orchestrator.runtime_bus)
    # Bind unconditionally (agent may be None when no provider key existed at
    # boot). The bridge keeps provider_runtime/orchestrator/db_path so the
    # Providers page can manage keys AND hot-swap in an agent without a restart.
    bridge.bind_agent(
        agent=agent,
        orchestrator=orchestrator,
        db_path=db_path,
        kernel=aelvo_kernel,
        mcp_cli=mcp_cli,
        runtime_cli=runtime_cli,
        provider_runtime=provider_runtime,
    )
    await bridge.start()

    # 2. HTTP static server for the built dashboard.
    app = web.Application()
    app.router.add_get("/{tail:.*}", _serve_file)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, http_port)
    await site.start()

    url = f"http://{host}:{http_port}"
    log.info("AELVO web dashboard ready at %s", url)
    log.info("WebSocket bridge on ws://%s:%d", host, ws_port)

    if open_browser:
        try:
            webbrowser.open(url)
        except Exception as exc:  # pragma: no cover - headless environments
            log.warning("Could not open browser automatically: %s", exc)

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        log.info("Shutting down AELVO web server...")
        await bridge.stop()
        await runner.cleanup()
