"""
web_bridge.py — AELVO WebSocket Event Bridge

Runs an asyncio WebSocket server that subscribes to the runtime EventBus
and forwards every event as JSON to all connected web clients.

Designed to be launched as a background task from main.py alongside the TUI.

Protocol:
    Server → Client: JSON events.
        { "type": "finding_published", "specialist": "ORACLE",
          "action": "Found 3 vulnerabilities",
          "data": { ... }, "timestamp": 1234567890.0 }

    Client → Server: JSON commands.
        { "type": "ping" } → server responds { "type": "pong" }
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import traceback
from typing import Any, Dict, Optional, Set

import websockets
from websockets.server import WebSocketServerProtocol

from runtime_next.models.events import BaseEvent

log = logging.getLogger("aelvo.web.bridge")


class _SessionRecorder:
    """Minimal session tracker matching the interface the orchestrator uses.

    Mirrors the fields the old REPL's SessionTracker exposed so
    ``orchestrator.execute_turn`` can record tools/files without needing a
    terminal session object.
    """

    def __init__(self):
        self.user_query = ""
        self.tools_used: list = []
        self.files_touched: list = []
        self.final_answer = ""
        self.status = "success"

    def record_tool(self, tool_name: str, args: dict, outcome_status: str):
        self.tools_used.append(tool_name)
        if args.get("path"):
            self.files_touched.append(args["path"])
        if args.get("url"):
            self.files_touched.append(args["url"][:80])
        if outcome_status == "error":
            self.status = "partial"

    def record_answer(self, answer: str):
        self.final_answer = answer[:500]

    def save(self, db_path: str):
        # Persist the condensed session record, mirroring the old SessionTracker.
        import sqlite3

        if not self.user_query:
            return
        try:
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with sqlite3.connect(db_path) as db:
                db.execute(
                    """CREATE TABLE IF NOT EXISTS sessions (
                        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        user_query TEXT,
                        tools_used TEXT,
                        files_touched TEXT,
                        final_answer TEXT,
                        status TEXT DEFAULT 'success'
                    )"""
                )
                db.execute(
                    "INSERT INTO sessions (timestamp, user_query, tools_used, files_touched, final_answer, status)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        timestamp,
                        self.user_query[:200],
                        ", ".join(self.tools_used) if self.tools_used else "respond",
                        ", ".join(self.files_touched) if self.files_touched else "",
                        self.final_answer,
                        self.status,
                    ),
                )
        except Exception as exc:
            log.debug("Session save failed: %s", exc)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765

# Attributes that are part of the BaseEvent metaclass, not event-specific data
_PYDANTIC_SKIP = {
    "model_dump", "model_dump_json", "model_copy", "model_fields",
    "model_fields_set", "model_extra", "model_computed_fields",
    "model_post_init", "model_config", "model_parametrized_name",
    "model_json_schema", "model_validate", "model_validate_json",
    "model_rebuild", "model_construct", "model_namespace",
    "validate", "dict", "json", "schema", "update_forward_refs",
    "clean", "construct", "copy", "from_orm", "schema_json",
    "validate_model",
}

# Well-known runtime event type attributes to extract for action/specialist
_ACTION_ATTRS = (
    "summary", "action", "recommendation", "command", "reason",
    "classification", "session_title", "challenged_claim",
)

_SOURCE_ATTRS = (
    "specialist", "node_id", "consumer", "challenger", "source",
)


class WebBridge:
    """Asyncio WebSocket server that bridges runtime events to web clients.

    Usage:
        bridge = WebBridge(host="127.0.0.1", port=8765)
        bridge.subscribe_to_runtime(runtime_bus)
        await bridge.start()
        ...
        await bridge.stop()
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
    ):
        self._host = host
        self._port = port
        self._server: websockets.WebSocketServer | None = None
        self._connections: Set[WebSocketServerProtocol] = set()
        self._runtime_bus = None
        self._running = False
        self._agent = None
        self._orchestrator = None
        self._db_path = ""
        self._kernel = None
        self._mcp_cli = None
        self._runtime_cli = None
        self._provider_runtime = None

    @property
    def url(self) -> str:
        return f"ws://{self._host}:{self._port}"

    def subscribe_to_runtime(self, runtime_bus) -> None:
        """Subscribe to a runtime EventBus to forward all events to web clients.

        The runtime bus must have a subscribe_all(callback) method.
        """
        self._runtime_bus = runtime_bus
        if hasattr(runtime_bus, "subscribe_all"):
            runtime_bus.subscribe_all(self._on_runtime_event)
            log.info("WebBridge subscribed to runtime EventBus")

    def bind_agent(
        self,
        agent,
        orchestrator=None,
        db_path: str = "",
        kernel=None,
        mcp_cli=None,
        runtime_cli=None,
        provider_runtime=None,
    ) -> None:
        """Bind the agent + orchestrator so user chat messages execute real turns."""
        self._agent = agent
        self._orchestrator = orchestrator
        self._db_path = db_path
        self._kernel = kernel
        self._mcp_cli = mcp_cli
        self._runtime_cli = runtime_cli
        self._provider_runtime = provider_runtime

    def _maybe_hot_swap_agent(self) -> bool:
        """Bring the agent online when the app booted without a provider.

        Called after a key is saved from the Providers page (or lazily on the
        first chat turn). Re-runs provider detection — the key is now in the
        vault and/or environment — and constructs a fresh AelvoAgent so chat
        works without a restart. Returns True when an agent is active.
        """
        if self._agent is not None:
            return True
        if self._orchestrator is None:
            return False
        try:
            from core.registry import MODEL_REGISTRY
            from core.startup import detect_provider

            provider_name, provider_config, api_key, model = detect_provider(
                MODEL_REGISTRY
            )
            if provider_config is None:
                return False

            from main import AelvoAgent  # local import: main boots the bridge

            self._agent = AelvoAgent(
                api_key=api_key,
                model=model,
                provider_name=provider_name,
                provider_config=provider_config,
                provider_runtime=self._provider_runtime,
            )
            log.info(
                "Hot-swapped agent online: provider=%s model=%s",
                provider_name, model,
            )
            return True
        except Exception as exc:
            log.warning("Agent hot-swap failed: %s", exc)
            return False

    async def start(self) -> None:
        """Start the WebSocket server."""
        if self._running:
            return
        self._running = True

        self._server = await websockets.serve(
            self._handle_connection,
            self._host,
            self._port,
            ping_interval=30,
            ping_timeout=10,
        )
        log.info("WebBridge server started on %s", self.url)

    async def stop(self) -> None:
        """Stop the WebSocket server and disconnect all clients."""
        self._running = False

        if self._connections:
            await asyncio.gather(
                *[ws.close(1001, "Server shutting down") for ws in self._connections],
                return_exceptions=True,
            )
            self._connections.clear()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        log.info("WebBridge server stopped")

    async def _handle_connection(
        self, websocket: WebSocketServerProtocol
    ) -> None:
        """Handle a new WebSocket client connection."""
        client_info = f"{websocket.remote_address}"
        self._connections.add(websocket)
        log.info(
            "WebBridge client connected: %s (total: %d)",
            client_info, len(self._connections),
        )

        try:
            # Send welcome event immediately
            await self._send_raw(
                websocket,
                {
                    "type": "system_online",
                    "source": "web_bridge",
                    "specialist": "",
                    "action": "Connected to AELVO WebSocket bridge",
                    "data": {"server_url": self.url},
                    "timestamp": time.time(),
                    "icon": "●",
                    "color": "#00e38c",
                },
            )

            # Listen for incoming messages from the client
            async for raw_message in websocket:
                try:
                    message = json.loads(raw_message)
                    msg_type = message.get("type", "")

                    if msg_type == "ping":
                        await websocket.send(
                            json.dumps({
                                "type": "pong",
                                "timestamp": time.time(),
                            })
                        )
                    elif msg_type == "user_message":
                        user_msg = message.get("message", "")
                        if user_msg:
                            asyncio.ensure_future(
                                self._run_user_turn(user_msg, websocket)
                            )
                    elif msg_type == "providers_list":
                        await self._handle_providers_list(websocket)
                    elif msg_type == "provider_save_key":
                        await self._handle_provider_save_key(message, websocket)
                    elif msg_type == "provider_remove_key":
                        await self._handle_provider_remove_key(message, websocket)
                except json.JSONDecodeError:
                    log.debug("WebBridge invalid JSON from %s", client_info)

        except websockets.exceptions.ConnectionClosed:
            log.info("WebBridge client disconnected: %s", client_info)
        except Exception as exc:
            log.warning("WebBridge connection error for %s: %s", client_info, exc)
        finally:
            self._connections.discard(websocket)

    async def _on_runtime_event(self, runtime_event: BaseEvent) -> None:
        """Callback invoked by the runtime EventBus for every event.

        NON-BLOCKING: The broadcast is wrapped in create_task so this
        returns immediately and does not stall the runtime event bus.
        """
        if not self._running or not self._connections:
            return

        payload = self._event_to_payload(runtime_event)
        if payload is None:
            return

        # Schedule broadcast as background task so we don't block the event bus
        asyncio.ensure_future(self._broadcast(payload))

    async def _broadcast(self, payload: Dict[str, Any]) -> None:
        """Broadcast a payload to all connected clients.

        Uses a short per-client timeout so one slow client never
        delays the broadcast to others.
        """
        if not self._connections:
            return

        message = json.dumps(payload, default=str)
        dead_connections: Set[WebSocketServerProtocol] = set()

        for ws in self._connections:
            try:
                await asyncio.wait_for(ws.send(message), timeout=2.0)
            except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                dead_connections.add(ws)
            except Exception as exc:
                log.warning("WebBridge broadcast error: %s", exc)
                dead_connections.add(ws)

        if dead_connections:
            self._connections -= dead_connections

    async def _send_raw(
        self, websocket: WebSocketServerProtocol, payload: Dict[str, Any]
    ) -> None:
        """Send a raw payload to a specific client."""
        try:
            await websocket.send(json.dumps(payload, default=str))
        except Exception as exc:
            log.warning("WebBridge send error: %s", exc)

    # ------------------------------------------------------------------
    # User chat -> agent turn execution
    # ------------------------------------------------------------------

    async def _run_user_turn(self, user_msg: str, websocket) -> None:
        """Execute a user chat message through the orchestrator and reply."""
        await self._send_raw(websocket, {
            "type": "user_message_received",
            "source": "web_bridge",
            "specialist": "HERMES",
            "action": user_msg[:120],
            "data": {"message": user_msg},
            "timestamp": time.time(),
        })

        # If the app booted without a provider, a key may have been saved from
        # the Providers page since — try to hot-swap in an agent on first turn.
        if self._agent is None:
            self._maybe_hot_swap_agent()

        if self._agent is None or self._orchestrator is None:
            await self._send_raw(websocket, {
                "type": "agent_response",
                "source": "system",
                "specialist": "",
                "action": "Agent backend is not initialized. Open the Providers page to set an API key.",
                "data": {},
                "timestamp": time.time(),
            })
            return

        # Route #kernel / #mcp / #status commands (web parity with the old CLI)
        if user_msg.lstrip().startswith("#"):
            await self._run_kernel_command(user_msg, websocket)
            return

        try:
            # execute_turn is async and publishes to the runtime EventBus (the
            # same loop the bridge runs on), so await it directly here.
            session_tracker = _SessionRecorder()
            session_tracker.user_query = user_msg
            turn_result = await self._orchestrator.execute_turn(
                self._agent,
                user_msg,
                session_tracker=session_tracker,
                db_path=self._db_path,
            )
            session_tracker.save(self._db_path)
            answer = turn_result.get("output") or "(no output)"
            await self._send_raw(websocket, {
                "type": "agent_response",
                "source": "orchestrator",
                "specialist": "HERALD",
                "action": answer,
                "data": {"tools_used": turn_result.get("specialists_active", [])},
                "timestamp": time.time(),
            })
        except Exception as exc:
            log.error("Agent turn failed: %s", exc)
            log.debug(traceback.format_exc())
            await self._send_raw(websocket, {
                "type": "agent_error",
                "source": "orchestrator",
                "specialist": "",
                "action": str(exc)[:300],
                "data": {},
                "timestamp": time.time(),
            })

    async def _run_kernel_command(self, cmd: str, websocket) -> None:
        """Execute a #kernel / #mcp / #status command and reply with its output."""
        lower = cmd.lower().strip()
        try:
            if lower.startswith("#status") and self._runtime_cli is not None:
                result = await asyncio.to_thread(self._runtime_cli.execute, cmd)
                text = json.dumps(result, default=str)[:2000]
            elif lower.startswith("#mcp") and self._mcp_cli is not None:
                result = await self._mcp_cli.execute(cmd)
                text = json.dumps(result, default=str)[:2000]
            elif self._kernel is not None:
                result = await asyncio.to_thread(self._kernel.parse_and_execute, cmd)
                text = json.dumps(result, default=str)[:2000]
            else:
                text = f"Unknown command: {cmd}"
        except Exception as exc:
            text = f"Command error: {exc}"
        await self._send_raw(websocket, {
            "type": "agent_response",
            "source": "kernel",
            "specialist": "",
            "action": text,
            "data": {},
            "timestamp": time.time(),
        })

    # ------------------------------------------------------------------
    # Provider management (web UI provider setup page)
    # ------------------------------------------------------------------

    def _provider_configs(self) -> Dict[str, Any]:
        """Provider key -> config mapping from the runtime (or the registry)."""
        if self._provider_runtime is not None and hasattr(self._provider_runtime, "provider_configs"):
            return self._provider_runtime.provider_configs or {}
        try:
            from core.registry import MODEL_REGISTRY

            return dict(MODEL_REGISTRY)
        except Exception as exc:
            log.warning("Provider registry unavailable: %s", exc)
            return {}

    def _provider_has_key(self, provider: str) -> bool:
        """True if the provider has a usable credential (vault or env)."""
        try:
            if self._provider_runtime is not None and hasattr(self._provider_runtime, "has_credentials"):
                return bool(self._provider_runtime.has_credentials(provider))
        except Exception as exc:
            log.debug("has_credentials(%s) failed: %s", provider, exc)
        try:
            store = self._credential_store()
            return store.get_for_provider(provider) is not None
        except Exception:
            return False

    def _credential_store(self):
        """Encrypted CredentialStore used by the provider runtime (same vault)."""
        if self._provider_runtime is not None and hasattr(self._provider_runtime, "credential_store"):
            return self._provider_runtime.credential_store
        from auth.cred_storage import CredentialStore
        from core.provider_runtime import DEFAULT_VAULT_PATH

        return CredentialStore(db_path=DEFAULT_VAULT_PATH)

    def _provider_payload(self, provider: str, cfg: Any) -> Dict[str, Any]:
        """Serialize a provider config for the web UI (never leaks the key)."""
        sdk = getattr(cfg, "sdk", None)
        sdk_val = sdk.value if hasattr(sdk, "value") else (str(sdk) if sdk else "")
        env_key = getattr(cfg, "env_key", "") or getattr(getattr(cfg, "auth", None), "env_var", "") or ""
        return {
            "key": provider,
            "name": getattr(cfg, "name", provider),
            "env_key": env_key,
            "default_model": getattr(cfg, "default_model", "") or "",
            "sdk": sdk_val,
            "local": bool(getattr(cfg, "local", False)),
            "has_key": self._provider_has_key(provider),
            "base_url": getattr(cfg, "base_url", None) or "",
        }

    async def _handle_providers_list(self, websocket) -> None:
        """Reply with all registered providers and their key status."""
        providers = []
        for key, cfg in sorted(self._provider_configs().items()):
            providers.append(self._provider_payload(key, cfg))
        await self._send_raw(websocket, {
            "type": "providers_list",
            "source": "web_bridge",
            "specialist": "",
            "action": f"{len(providers)} providers",
            "data": {"providers": providers},
            "timestamp": time.time(),
        })

    async def _handle_provider_save_key(self, message: Dict[str, Any], websocket) -> None:
        """Save an API key for a provider to the encrypted vault."""
        provider = (message.get("provider") or "").strip().lower()
        api_key = (message.get("api_key") or "").strip()

        if not provider or not api_key:
            await self._send_raw(websocket, self._provider_result(
                provider, False, "Provider and API key are required."
            ))
            return

        cfg = self._provider_configs().get(provider)
        if cfg is None:
            await self._send_raw(websocket, self._provider_result(
                provider, False, f"Unknown provider: {provider}"
            ))
            return

        try:
            import time as _time
            import uuid

            from auth.types import Credential, CredentialType

            store = self._credential_store()
            cred = Credential(
                id=f"key_{provider}_{uuid.uuid4().hex[:8]}",
                provider=provider,
                credential_type=CredentialType.API_KEY,
                value=api_key,
                label=f"{provider} API key (set from web UI)",
                created_at=_time.time(),
                is_valid=True,
                metadata={"source": "web_ui", "model": getattr(cfg, "default_model", "") or ""},
            )
            store.store(cred)

            # Also surface it in the environment so a restart picks it up.
            env_key = getattr(cfg, "env_key", "") or ""
            if env_key:
                os.environ[env_key] = api_key

            # Hot-swap: if the app booted without a provider, bring the agent
            # online immediately so chat works without a restart. Only claim
            # activation when the agent was actually offline before — an agent
            # already on another provider keeps using that provider until restart.
            was_active = self._agent is not None
            activated = self._maybe_hot_swap_agent()

            await self._send_raw(websocket, self._provider_result(
                provider, True,
                f"API key saved securely for {provider}."
                + (" Agent activated — you can start chatting now." if activated and not was_active else ""),
                key_present=True,
            ))
            # Broadcast so other tabs update immediately
            await self._broadcast({
                "type": "providers_updated",
                "source": "web_bridge",
                "specialist": "",
                "action": f"API key saved for {provider}",
                "data": {"provider": provider, "has_key": True},
                "timestamp": time.time(),
            })
        except Exception as exc:
            log.error("Failed to save key for %s: %s", provider, exc)
            await self._send_raw(websocket, self._provider_result(
                provider, False, f"Failed to save key: {exc}"
            ))

    async def _handle_provider_remove_key(self, message: Dict[str, Any], websocket) -> None:
        """Remove the stored key for a provider from the encrypted vault."""
        provider = (message.get("provider") or "").strip().lower()
        if not provider:
            await self._send_raw(websocket, self._provider_result(
                provider, False, "Provider is required."
            ))
            return

        try:
            store = self._credential_store()
            cred = store.get_for_provider(provider)
            if cred is not None:
                store.delete(cred.id)

            # Clear the env var if we set it
            cfg = self._provider_configs().get(provider)
            env_key = getattr(cfg, "env_key", "") if cfg else ""
            if env_key:
                os.environ.pop(env_key, None)

            await self._send_raw(websocket, self._provider_result(
                provider, True, f"API key removed for {provider}.", key_present=False
            ))
            await self._broadcast({
                "type": "providers_updated",
                "source": "web_bridge",
                "specialist": "",
                "action": f"API key removed for {provider}",
                "data": {"provider": provider, "has_key": False},
                "timestamp": time.time(),
            })
        except Exception as exc:
            log.error("Failed to remove key for %s: %s", provider, exc)
            await self._send_raw(websocket, self._provider_result(
                provider, False, f"Failed to remove key: {exc}"
            ))

    @staticmethod
    def _provider_result(provider: str, success: bool, message: str, key_present: bool = False) -> Dict[str, Any]:
        """Build a provider_operation_result payload."""
        return {
            "type": "provider_operation_result",
            "source": "web_bridge",
            "specialist": "",
            "action": message[:120],
            "data": {"provider": provider, "success": success, "message": message, "has_key": key_present},
            "timestamp": time.time(),
        }

    @staticmethod
    def _event_to_payload(event: BaseEvent) -> Optional[Dict[str, Any]]:
        """Convert a runtime BaseEvent to a JSON-serialisable dict."""
        try:
            etype = getattr(event, "type", None)
            if etype is None:
                return None

            etype_str = str(etype.value) if hasattr(etype, "value") else str(etype)

            action = _extract_action(event, etype_str)
            specialist = _extract_source(event)
            data = _extract_data(event)

            # Note: Consumption trail matching between blackboard_publication
            # and finding_consumed events requires the backend to carry the
            # same entry identifier in both event types.
            return {
                "type": etype_str,
                "source": getattr(event, "source", "") or "runtime",
                "specialist": specialist,
                "action": action[:120],
                "data": data,
                "timestamp": getattr(event, "timestamp", time.time()),
                "icon": _get_icon(etype_str),
                "color": _get_color(etype_str),
            }

        except Exception as exc:
            log.debug("WebBridge event conversion error: %s", exc)
            return None


# ── Conversion Helpers ───────────────────────────────────────────

def _extract_action(event: BaseEvent, etype: str) -> str:
    """Extract a human-readable action summary from a runtime event."""
    for attr in _ACTION_ATTRS:
        val = getattr(event, attr, None)
        if val:
            return str(val)
    return etype.replace("_", " ").title()


def _extract_source(event: BaseEvent) -> str:
    """Extract the source/specialist identifier from a runtime event."""
    for attr in _SOURCE_ATTRS:
        val = getattr(event, attr, None)
        if val:
            return str(val)
    return ""


def _extract_data(event: BaseEvent) -> Dict[str, Any]:
    """Extract event-specific data fields as a JSON-serialisable dict.

    Uses model_dump() for Pydantic models, then strips out the known
    metaclass fields so only the event-specific attributes remain.
    """
    data: Dict[str, Any] = {}

    try:
        # Pydantic v2 models have model_dump()
        raw = event.model_dump() if hasattr(event, "model_dump") else {}
    except Exception:
        raw = {}

    # If model_dump worked, use it and filter
    if raw:
        for key, val in raw.items():
            if key in ("type", "timestamp", "event_id", "correlation_id"):
                continue
            if val is None:
                continue
            data[key] = val
        return data

    # Fallback: walk attributes manually (for non-Pydantic events)
    for attr_name in dir(event):
        if attr_name.startswith("_") or attr_name in _PYDANTIC_SKIP:
            continue
        try:
            val = getattr(event, attr_name)
            if val is None or callable(val):
                continue
            if attr_name in ("type", "timestamp", "event_id", "correlation_id"):
                continue
            # Test serialisability
            json.dumps(val)
            data[attr_name] = val
        except (TypeError, ValueError) as _ex:
            log.warning("Silenced exception: %s", _ex)

    return data


_EVENT_ICONS: Dict[str, str] = {
    "blackboard_publication": "◆",
    "finding_consumed": "▷",
    "challenge_raised": "⚠",
    "consensus_formed": "↻",
    "architect_decision": "◉",
    "execution_started": "▶",
    "execution_completed": "✓",
    "report_generated": "★",
    "recovery_initiated": "🔄",
    "recovery_completed": "✅",
    "node_transition": "◈",
    "graph_completed": "✓",
    "graph_started": "▶",
    "task_created": "○",
    "task_assigned": "→",
    "task_completed": "✓",
    "task_failed": "✗",
    "system_online": "●",
}

_EVENT_COLORS: Dict[str, str] = {
    "blackboard_publication": "#8c5cff",
    "finding_consumed": "#00d889",
    "challenge_raised": "#ff5c7a",
    "consensus_formed": "#19f5a5",
    "architect_decision": "#3b82f6",
    "execution_started": "#f7b731",
    "execution_completed": "#00e38c",
    "report_generated": "#39c8ff",
    "recovery_initiated": "#3b82f6",
    "recovery_completed": "#00e38c",
    "node_transition": "#a565ff",
    "graph_completed": "#00e38c",
    "graph_started": "#f7b731",
    "task_created": "#52627f",
    "task_assigned": "#a565ff",
    "task_completed": "#00e38c",
    "task_failed": "#ff5c7a",
    "system_online": "#00e38c",
}


def _get_icon(etype: str) -> str:
    return _EVENT_ICONS.get(etype, "•")


def _get_color(etype: str) -> str:
    return _EVENT_COLORS.get(etype, "#52627f")
