import asyncio
import functools

import pytest

from quest import Historian, queue, step
from quest.serializer import NoopSerializer


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

    history = []
    historian = Historian('test', application, history, serializer=NoopSerializer())
    historian.configure(configure_state, config1)
    historian.run()
    await asyncio.sleep(0.1)

    await historian.record_external_event('foobar', None, 'put', 'a')
    await historian.record_external_event('foobar', None, 'put', 'b')
    await historian.record_external_event('foobar', None, 'put', 'c')

    await asyncio.sleep(0.1)

    await historian.suspend()

    # Application reboots with new config
    # 'b' is no longer a foo, and should be moved to bar
    config2 = {
        'foos': ['a']
    }

    historian = Historian('test', application, history, serializer=NoopSerializer())
    historian.configure(configure_state, config1)
    historian.configure(configure_state, config2)
    historian.run()
    await asyncio.sleep(0.1)

    await historian.record_external_event('foobar', None, 'put', 'a')
    await historian.record_external_event('foobar', None, 'put', 'b')
    await historian.record_external_event('foobar', None, 'put', 'c')

    await asyncio.sleep(0.1)

    await historian.suspend()

    assert messages == [
        'a is foo',
        'b is foo',
        'c?',
        'a is foo',
        'b is bar',
        'c?'
    ]
