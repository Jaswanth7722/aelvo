import asyncio
import logging
import shutil
import tempfile
import time
from pathlib import Path

import pytest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test_stress")


@pytest.fixture
def test_dir():
    d = Path(tempfile.mkdtemp(prefix="aelvo_stress_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_event_bus_high_throughput():
    from runtime_next.events.bus import EventBus
    from runtime_next.models.events import BaseEvent, EventType
    bus = EventBus()

    count = 2000
    received = []

    async def cb(event):
        received.append(event)

    bus.subscribe_all(cb)
    await bus.start()

    start = time.time()
    for i in range(count):
        await bus.publish(BaseEvent(id=f"e{i}", type=EventType.LOG_MESSAGE))
    elapsed = time.time() - start

    for _ in range(100):
        if len(received) >= count:
            break
        await asyncio.sleep(0.05)

    rate = count / elapsed if elapsed > 0 else 0
    assert len(received) == count
    log.info(f"PASS: {count} events at {rate:.0f} events/sec")
    await bus.stop()


@pytest.mark.asyncio
async def test_file_mutex_stress(test_dir):
    from runtime_next.engine.file_mutex import FileMutex
    mutex = FileMutex()
    f = str(test_dir / "stress_shared.txt")
    iterations = 50
    worker_count = 10
    counter = {"val": 0}

    async def worker(wid: int):
        for _ in range(iterations):
            await mutex.acquire([f])
            curr = counter["val"]
            await asyncio.sleep(0.002)
            counter["val"] = curr + 1
            mutex.release([f])

    start = time.time()
    await asyncio.gather(*(worker(i) for i in range(worker_count)))
    elapsed = time.time() - start
    expected = iterations * worker_count
    assert counter["val"] == expected
    log.info(f"PASS: {worker_count} workers, {iterations} iterations, {elapsed:.2f}s")


@pytest.mark.asyncio
async def test_concurrent_file_mutex_deadlock_free(test_dir):
    from runtime_next.engine.file_mutex import FileMutex
    mutex = FileMutex()
    files = [str(test_dir / f"file_{i}.txt") for i in range(5)]
    counter = {"val": 0}

    async def worker(wid: int):
        for _ in range(20):
            subset = files[:3] if wid % 2 == 0 else files[2:]
            await mutex.acquire(subset)
            counter["val"] += 1
            await asyncio.sleep(0.001)
            mutex.release(subset)

    await asyncio.gather(*(worker(i) for i in range(8)))
    assert counter["val"] == 8 * 20


@pytest.mark.asyncio
async def test_recovery_stress(test_dir):
    from runtime_next.events.bus import EventBus
    from runtime_next.engine.file_mutex import FileMutex
    from runtime_next.engine.engine import ExecutionGraph
    from runtime_next.recovery.engine import RecoveryEngine
    from runtime_next.models.node import NodeDefinition

    bus = EventBus()
    mutex = FileMutex()
    graph = ExecutionGraph(bus, mutex)
    recovery = RecoveryEngine(graph)
    bus.subscribe_all(recovery.on_event)
    await bus.start()

    node_count = 20
    start = time.time()

    for i in range(node_count):
        node = NodeDefinition(id=f"FAIL_{i}", description=f"Fail test {i}", specialist="FORGE")
        graph.add_node(node)

    for i in range(node_count):
        await recovery.handle_failure(f"FAIL_{i}", "SyntaxError: test failure")

    await asyncio.sleep(0.5)
    elapsed = time.time() - start
    assert len(recovery._recovery_history) == node_count
    log.info(f"PASS: {node_count} recoveries in {elapsed:.2f}s")
    await bus.stop()
