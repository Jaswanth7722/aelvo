import asyncio
import logging
import tempfile
from pathlib import Path

import pytest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test_eventbus")


@pytest.fixture
def bus():
    from runtime_next.events.bus import EventBus
    log_file = Path(tempfile.mktemp(prefix="aelvo_events_", suffix=".log"))
    b = EventBus(log_path=str(log_file))
    yield b
    if log_file.exists():
        log_file.unlink()


@pytest.mark.asyncio
async def test_publish_and_subscribe(bus):
    from runtime_next.models.events import BaseEvent, EventType
    received = []

    async def cb(event):
        received.append(event)

    bus.subscribe(EventType.LOG_MESSAGE, cb)
    await bus.start()

    event = BaseEvent(id="e1", type=EventType.LOG_MESSAGE, payload={"msg": "hello"})
    await bus.publish(event)

    for _ in range(50):
        if received:
            break
        await asyncio.sleep(0.05)

    assert len(received) == 1
    assert received[0].id == "e1"
    await bus.stop()


@pytest.mark.asyncio
async def test_global_subscriber(bus):
    from runtime_next.models.events import BaseEvent, EventType
    received = []

    async def cb(event):
        received.append(event)

    bus.subscribe_all(cb)
    await bus.start()

    for i in range(5):
        await bus.publish(BaseEvent(id=f"e{i}", type=EventType.LOG_MESSAGE))

    for _ in range(100):
        if len(received) >= 5:
            break
        await asyncio.sleep(0.05)

    assert len(received) == 5
    await bus.stop()


@pytest.mark.asyncio
async def test_event_logging_and_replay(bus):
    from runtime_next.models.events import BaseEvent, EventType
    await bus.start()

    for i in range(10):
        await bus.publish(BaseEvent(id=f"e{i}", type=EventType.LOG_MESSAGE, payload={"n": i}))

    await asyncio.sleep(0.3)
    await bus.stop()

    assert bus._log_path is not None and bus._log_path.exists()
    replayed = []

    async def replay_cb(event):
        replayed.append(event)

    new_bus = type(bus)(log_path=str(bus._log_path))
    await new_bus.replay(str(bus._log_path), replay_cb)
    assert len(replayed) == 10


@pytest.mark.asyncio
async def test_high_load(bus):
    from runtime_next.models.events import BaseEvent, EventType
    count = 500
    received = []

    async def cb(event):
        received.append(event)

    bus.subscribe_all(cb)
    await bus.start()

    for i in range(count):
        await bus.publish(BaseEvent(id=f"e{i}", type=EventType.LOG_MESSAGE))

    for _ in range(200):
        if len(received) >= count:
            break
        await asyncio.sleep(0.05)

    assert len(received) == count
    await bus.stop()
