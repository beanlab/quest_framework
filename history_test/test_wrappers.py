import asyncio

import pytest

from history import History
from history.external import event, wrap_as_event
from history.wrappers import wrap_steps
from history.serializer import NoopSerializer
from .utils import timeout


@pytest.mark.asyncio
async def test_step_class():
    class CallMe:
        async def __call__(self):
            await asyncio.sleep(0.01)

    history = History('test', CallMe(), [], serializer=NoopSerializer())
    await history.run()


@pytest.mark.asyncio
@timeout(3)
async def test_wrap_steps():
    calls = []

    class Useful:
        async def foo(self):
            calls.append('foo')
            return 1

        async def bar(self):
            calls.append('bar')
            return 2

    async def workflow():
        useful = wrap_steps(Useful())
        await useful.foo()
        await useful.bar()
        async with event('gate', None) as gate:
            await gate.wait()
        await useful.foo()
        await useful.bar()

    history = History('test', workflow, [], serializer=NoopSerializer())
    history.run()

    with history.get_resource_stream(None) as resource_stream:
        updates = aiter(resource_stream)
        await anext(updates)  # First update should be empty
        resources = await anext(updates)  # second event should now show the 'gate' Event
        assert ('gate', None) in resources

        await history.suspend()

    wtask = history.run()

    with history.get_resource_stream(None) as resource_stream:
        updates = aiter(resource_stream)
        resources = await anext(updates)  # should include 'gate' already because that is where the first run left off
        assert ('gate', None) in resources
        gate = wrap_as_event('gate', None, history)
        await gate.set()

    await wtask  # good hygiene

    assert calls == ['foo', 'bar', 'foo', 'bar']
