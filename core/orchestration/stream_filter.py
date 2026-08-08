"""
stream_filter.py — Decide which LLM tokens are user-visible.

The agent's responses alternate between two shapes:

* **Tool-call JSON** — batches of ``{"tool": ..., "args": {...}}`` objects
  the orchestrator parses and executes. This is machine plumbing and must
  NEVER be streamed to the terminal.
* **User-facing prose** — the final answer (or the consolidated pipeline
  plan). This is what the user should see appear token-by-token as the
  model generates it.

``TokenStreamFilter`` wraps the CLI's live-token sink. It buffers the
leading chunks and sniffs the response's first characters: responses that
start with a tool marker (``{``, ``[`` or ``#`` after leading whitespace)
are suppressed entirely, while prose is forwarded live once we are
confident it is not JSON.
"""

from __future__ import annotations

from typing import Callable, Optional

#: Characters that begin tool-call JSON or a kernel command. Anything the
#: model emits starting with one of these (after whitespace) is plumbing,
#: not an answer.
_TOOL_MARKERS = ("{", "[", "#")

#: A response that OPENS with a markdown code fence is almost always a
#: fenced tool-call JSON batch (````` ```json\n[...]`````) or a fenced
#: kernel command — weak models wrap their machine output in fences. The
#: sniff markers above only catch bare ``{``/``[``, so without this the raw
#: JSON would stream to the terminal before the tool loop hides it.
_FENCE_MARKER = "```"


class TokenStreamFilter:
    """Stateful filter that forwards prose tokens live, hides tool JSON.

    Modes:
      ``sniff``     — buffering leading chunks, undecided.
      ``live``      — prose confirmed; every token forwarded to the sink.
      ``suppressed``— tool JSON / kernel command; nothing forwarded.

    ``streamed`` is True once any visible token was forwarded, which lets
    callers skip the final whole-answer callback (already on screen).
    """

    def __init__(
        self,
        sink: Optional[Callable[[str], None]],
        max_sniff_chars: int = 256,
    ) -> None:
        self._sink = sink
        self._max_sniff = max(16, int(max_sniff_chars))
        self._buffer = ""
        self._mode = "sniff"  # sniff | live | suppressed
        #: True when at least one visible token was forwarded to the sink.
        self.streamed = False

    def __call__(self, token: str) -> None:
        """Feed one streamed chunk from the provider SDK."""
        if not token:
            return

        if self._mode == "live":
            if self._sink is not None:
                self._sink(token)
            return
        if self._mode == "suppressed":
            return

        # ── sniff mode ────────────────────────────────────────────────────
        self._buffer += token
        stripped = self._buffer.lstrip()
        if stripped.startswith(_TOOL_MARKERS) or stripped.startswith(_FENCE_MARKER):
            # JSON array/object of tool calls, a '#command', or a fenced
            # (```json) tool batch. Suppress.
            self._mode = "suppressed"
            return
        if len(stripped) >= self._max_sniff:
            # Long enough without a tool marker — it is prose. Flush what we
            # buffered and go live.
            self._mode = "live"
            self.streamed = True
            if self._sink is not None:
                self._sink(self._buffer)

    def flush(self) -> None:
        """End-of-stream: release any buffered prose that never reached the
        sniff threshold (short answers). Suppressed streams stay hidden."""
        if self._mode == "sniff" and self._buffer:
            self._mode = "live"
            self.streamed = True
            if self._sink is not None:
                self._sink(self._buffer)

    @property
    def suppressed(self) -> bool:
        """True when the whole stream was hidden (tool JSON, not prose)."""
        return self._mode == "suppressed"
