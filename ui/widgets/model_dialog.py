"""model_dialog.py — Model selector overlay (Ctrl+O)

Browse and select LLM provider + model.
"""

from textual.screen import ModalScreen
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import Static


PROVIDERS = {
    "nvidia": ["nemotron-3-super-120b-a12b", "nemotron-4-340b-instruct"],
    "openai": ["gpt-4o", "gpt-4o-mini", "gpt-4", "o3-mini"],
    "anthropic": ["claude-sonnet-4-20250514", "claude-3-5-sonnet-20241022"],
    "google": ["gemini-2.5-pro", "gemini-2.5-flash"],
    "groq": ["llama-3.3-70b-versatile", "mixtral-8x7b-32768"],
}


class ModelDialog(ModalScreen):
    """Model selector dialog."""

    CSS = """
    ModelDialog {
        align: center middle;
    }

    #model-dialog-box {
        width: 60;
        height: auto;
        max-height: 30;
        background: #161b22;
        border: solid #30363d;
        padding: 1 2;
    }

    #model-dialog-title {
        height: 1;
        color: #f0f6fc;
        text-style: bold;
        margin-bottom: 1;
    }

    #model-providers {
        height: auto;
        color: #8b949e;
        margin-bottom: 1;
    }

    #model-list {
        height: auto;
        max-height: 20;
        color: #c9d1d9;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("up", "prev", "Previous"),
        Binding("down", "next", "Next"),
        Binding("left", "prev_provider", "Prev Provider"),
        Binding("right", "next_provider", "Next Provider"),
        Binding("enter", "select", "Select"),
        Binding("k", "prev", "Previous", show=False),
        Binding("j", "next", "Next", show=False),
        Binding("h", "prev_provider", "Prev Provider", show=False),
        Binding("l", "next_provider", "Next Provider", show=False),
    ]

    def __init__(self, current_provider: str, current_model: str, callback=None):
        super().__init__()
        self.providers = list(PROVIDERS.keys())
        self._provider_idx = 0
        if current_provider in self.providers:
            self._provider_idx = self.providers.index(current_provider)
        self._model_idx = 0
        models = PROVIDERS.get(self.providers[self._provider_idx], [])
        if current_model in models:
            self._model_idx = models.index(current_model)
        self._callback = callback

    def compose(self):
        with Vertical(id="model-dialog-box"):
            yield Static(" SELECT MODEL", id="model-dialog-title")
            yield Static(id="model-providers")
            yield Static(id="model-list")

    def on_mount(self) -> None:
        self._render()

    def _render(self) -> None:
        provider = self.providers[self._provider_idx]
        models = PROVIDERS.get(provider, [])

        # Provider bar
        prov_parts = []
        for i, p in enumerate(self.providers):
            marker = ">" if i == self._provider_idx else " "
            color = "#58a6ff" if i == self._provider_idx else "#8b949e"
            prov_parts.append(f"[{color}]{marker} {p}[/]")
        self.query_one("#model-providers", Static).update("  ".join(prov_parts))

        # Model list
        lines = []
        for i, m in enumerate(models):
            marker = ">" if i == self._model_idx else " "
            color = "#f0f6fc" if i == self._model_idx else "#c9d1d9"
            lines.append(f" [{color}]{marker} {m}[/]")
        if not models:
            lines.append(" [#8b949e] (no models)[/]")
        self.query_one("#model-list", Static).update("\n".join(lines))

    def action_prev(self) -> None:
        PROVIDERS.get(self.providers[self._provider_idx], [])
        self._model_idx = max(0, self._model_idx - 1)
        self._render()

    def action_next(self) -> None:
        models = PROVIDERS.get(self.providers[self._provider_idx], [])
        self._model_idx = min(len(models) - 1, self._model_idx + 1)
        self._render()

    def action_prev_provider(self) -> None:
        self._provider_idx = (self._provider_idx - 1) % len(self.providers)
        self._model_idx = 0
        self._render()

    def action_next_provider(self) -> None:
        self._provider_idx = (self._provider_idx + 1) % len(self.providers)
        self._model_idx = 0
        self._render()

    def action_select(self) -> None:
        provider = self.providers[self._provider_idx]
        models = PROVIDERS.get(provider, [])
        if models and self._callback:
            self._callback(provider, models[self._model_idx])
        self.dismiss()

    def action_close(self) -> None:
        self.dismiss()
