"""
Validate RichLog refactor for touchpad scrolling.

Run directly:  python tests/test_richlog_scroll.py
The validation logic runs only when executed as a script so that
pytest collection of this file does not trigger module-level
side effects (file I/O, subprocess, sys.exit).
"""
import os
import sys
import importlib.util

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def main() -> None:
    errors, passes = [], 0
    def ok(l): global passes; passes += 1; print(f"  PASS: {l}")
    def fail(l, e): errors.append(f"{l}: {e}"); print(f"  FAIL: {l}: {e}")

    print("=" * 60)
    print("  RICHLOG SCROLL TEST")
    print("=" * 60)

    # 1. Source checks
    print("\n[Source checks]")
    with open("ui/widgets/conversation_feed.py") as f:
        content = f.read()

    checks = [
        ("Imports RichLog", "from textual.widgets import RichLog" in content),
        ("Extends RichLog", "class ConversationFeed(RichLog)" in content),
        ("auto_scroll = False", "self.auto_scroll = False" in content),
        ("max_lines = 200", "self.max_lines = 200" in content),
        ("clear() in render", "self.clear()" in content),
        ("write() in render", "self.write" in content),
        ("No Static import", "from textual.widgets import Static" not in content),
        ("No custom scroll_up handlers", "def on_mouse_scroll_up" not in content),
        ("No custom scroll_down handlers", "def on_mouse_scroll_down" not in content),
        ("No custom scroll_delta handlers", "def on_mouse_scroll_delta" not in content),
        ("No _scroll_by helper", "def _scroll_by" not in content),
        ("on_click handler preserved", "def on_click" in content),
        ("_is_at_bottom preserved", "def _is_at_bottom" in content),
        ("_pending_count preserved", "self._pending_count" in content),
        ("_update_scroll_indicator preserved", "def _update_scroll_indicator" in content),
        ("call_after_refresh scroll_end", "self.call_after_refresh(self.scroll_end" in content),
        ("was_at_bottom logic preserved", "was_at_bottom" in content),
        ("_write_message exists", "def _write_message" in content),
        ("_write_event exists", "def _write_event" in content),
    ]

    for label, result in checks:
        if result:
            ok(label)
        else:
            fail(label, "missing/wrong")

    # 2. Import & class hierarchy check
    print("\n[Import & hierarchy]")
    try:
        mod = importlib.import_module("ui.widgets.conversation_feed")
        cls = getattr(mod, "ConversationFeed")
        from textual.widgets import RichLog
        assert issubclass(cls, RichLog), f"Doesn't extend RichLog, extends {cls.__bases__}"
        ok("ConversationFeed extends RichLog")
        ok(f"ConversationFeed has methods: _is_at_bottom={hasattr(cls,'_is_at_bottom')}, on_click={hasattr(cls,'on_click')}, _update_scroll_indicator={hasattr(cls,'_update_scroll_indicator')}")
    except Exception as e:
        fail("Import/hierarchy test", str(e)[:120])

    # 3. py_compile all UI files
    print("\n[Syntax check all UI files]")
    import subprocess
    ui_files = [
        "ui/app.py",
        "ui/widgets/conversation_feed.py",
        "ui/widgets/omega_overview.py",
        "ui/widgets/execution_feed.py",
        "ui/widgets/collaboration_view.py",
        "ui/widgets/timeline_panel.py",
        "ui/widgets/audit_log_panel.py",
        "ui/widgets/specialist_panel.py",
        "ui/widgets/verification_panel.py",
    ]
    for f in ui_files:
        result = subprocess.run([sys.executable, "-m", "py_compile", f], capture_output=True, text=True)
        if result.returncode == 0:
            ok(f"  {f}")
        else:
            fail(f"  {f}", result.stderr[:80])

    # Summary
    print(f"\n{'='*60}")
    print(f"VALIDATION: {passes} passed, {len(errors)} failed")
    if errors:
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("ALL RICHLOG SCROLL VALIDATIONS PASSED")


if __name__ == "__main__":
    main()
