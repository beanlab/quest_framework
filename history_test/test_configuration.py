import asyncio
import functools

import pytest

from history import History, queue, step
from history.serializer import NoopSerializer


@pytest.mark.asyncio
async def test_configuration():
    messages = []

    state = {
        'foos': [],
        'bars': []
    }

    @step
    async def display(message):
        messages.append(message)

    async def application():
        async with queue('foobar', None) as inputs:
            while True:
                item = await inputs.get()
                if item in state['foos']:
                    await display(item + ' is foo')
                elif item in state['bars']:
                    await display(item + ' is bar')
                else:
                    await display(item + '?')

    async def configure_state(config):
        nonlocal state
        # Remove foos no longer needed (put them in bars)
        for foo in state['foos']:
            if foo not in config['foos']:
                state['bars'].append(foo)
                state['foos'].remove(foo)

        for foo in config['foos']:
            # Add new foos
            if foo not in state['foos']:
                state['foos'].append(foo)

        print('Configure complete')

    config1 = {
        'foos': ['a', 'b']
    }

    book = []
    history = History('test', application, book, serializer=NoopSerializer())
    history.configure(configure_state, config1)
    history.run()
    await asyncio.sleep(0.1)

    await history.record_external_event('foobar', None, 'put', 'a')
    await history.record_external_event('foobar', None, 'put', 'b')
    await history.record_external_event('foobar', None, 'put', 'c')

    await asyncio.sleep(0.1)

    await history.suspend()

    # Application reboots with new config
    # 'b' is no longer a foo, and should be moved to bar
    config2 = {
        'foos': ['a']
    }

    history = History('test', application, book, serializer=NoopSerializer())
    history.configure(configure_state, config1)
    history.configure(configure_state, config2)
    history.run()
    await asyncio.sleep(0.1)

    await history.record_external_event('foobar', None, 'put', 'a')
    await history.record_external_event('foobar', None, 'put', 'b')
    await history.record_external_event('foobar', None, 'put', 'c')

    await asyncio.sleep(0.1)

    await history.suspend()

    assert messages == [
        'a is foo',
        'b is foo',
        'c?',
        'a is foo',
        'b is bar',
        'c?'
    ]
