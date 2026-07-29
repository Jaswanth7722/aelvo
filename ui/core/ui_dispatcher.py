"""ui/core/ui_dispatcher.py — UIEventDispatcher: Routes UIEvents to Widgets

Phase 11: UI Event Standardization.

The bridge emits UIEvents. Widgets subscribe to specific UIEventType
values through the dispatcher. No direct widget method calls from the
bridge. This is the single event routing layer for all visible actions.
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional

from ui.core.ui_event import UIEvent, UIEventType

log = logging.getLogger("aelvo.ui.dispatcher")


class UIEventDispatcher:
    """Routes UIEvent instances to registered handler functions.

    Widgets (or any display component) subscribe to the UIEventType
    values they care about. The bridge dispatches UIEvents through
    this class. No direct coupling between event producers and consumers.

    Usage:
        dispatcher = UIEventDispatcher()

        # Widget subscribes to finding events
        dispatcher.subscribe(UIEventType.FINDING_PUBLISHED, my_handler)

        # Bridge emits an event
        dispatcher.dispatch(UIEvent(
            type=UIEventType.FINDING_PUBLISHED,
            specialist="ORACLE",
            action="Found 3 vulnerabilities",
            data={"confidence": 0.92},
        ))
    """

    def __init__(self):
        self._handlers: Dict[UIEventType, List[Callable[[UIEvent], None]]] = {}

    def subscribe(
        self, event_type: UIEventType, handler: Callable[[UIEvent], None]
    ) -> None:
        """Register a handler for a specific UIEventType.

        Args:
            event_type: The UIEventType to subscribe to.
            handler: Callback receiving a UIEvent instance.
        """
        self._handlers.setdefault(event_type, []).append(handler)

    def subscribe_all(
        self,
        event_types: List[UIEventType],
        handler: Callable[[UIEvent], None],
    ) -> None:
        """Register a handler for multiple UIEventType values."""
        for etype in event_types:
            self.subscribe(etype, handler)

    def unsubscribe(
        self, event_type: UIEventType, handler: Callable[[UIEvent], None]
    ) -> None:
        """Remove a previously registered handler."""
        handlers = self._handlers.get(event_type, [])
        if handler in handlers:
            handlers.remove(handler)

    def dispatch(self, event: UIEvent) -> None:
        """Fan out a UIEvent to all subscribed handlers.

        Handlers are called synchronously in subscription order.
        Exceptions from individual handlers are caught and logged
        so one bad handler never breaks the dispatch chain.
        """
        handlers = self._handlers.get(event.type, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                log.warning(
                    "UIEvent dispatch error for %s: %s", event.type.value, e
                )

    def clear(self) -> None:
        """Remove all registered handlers."""
        self._handlers.clear()

    @property
    def subscriber_count(self) -> int:
        """Return total number of handler registrations across all types."""
        return sum(len(h) for h in self._handlers.values())
