import pytest

from src.quest import Historian
from src.quest.external import event
from src.quest.wrappers import wrap_steps
from quest.serializer import NoopSerializer
from utils import timeout


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

    historian = Historian('test', workflow, [], serializer=NoopSerializer())
    historian.run()

    updates = aiter(historian.stream_resources(None))
    resources = await anext(updates)  # First update should be empty
    resources = await anext(updates)  # second event should now show the 'gate' Event
    assert 'gate' in resources

    await historian.suspend()

    wtask = historian.run()
    updates = aiter(historian.stream_resources(None))
    resources = await anext(updates)  # should include 'gate' already because that is where the first run left off
    await resources['gate'].set()

    await historian.wait_for_completion(None)

    await wtask  # good hygiene

    assert calls == ['foo', 'bar', 'foo', 'bar']
