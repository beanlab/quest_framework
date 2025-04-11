import asyncio
import pytest
from websockets import Headers

from history import state, queue
from history.client import Client
from history.server import Server
from history_test.utils import create_in_memory_historian


def authorize(headers: Headers) -> bool:
    if 'Authorization' not in headers:
        pytest.fail('No authorization header')
        return False
    if headers['authorization'] == "C@n'tT0uchThis!":
        return True
    pytest.fail('Authorization key incorrect')
    return False


async def serve(historian, port, authorizer):
    async with Server(historian, 'localhost', port, authorizer):
        await asyncio.sleep(1)


async def connect(wid):
    await asyncio.sleep(0.1)
    async with Client('ws://localhost:8000', {'authorization': "C@n'tT0uchThis!"}) as client:
        messages_seen = False
        async for resources in client.stream_resources(wid, None):
            print(resources)
            if ('messages', None) in resources and not messages_seen:
                messages_seen = True
                await client.send_event(wid, 'messages', None, 'put', ['Hello world!'])

        if not messages_seen:
            pytest.fail('Message not found in resources')

async def connect_exception(wid):
    await asyncio.sleep(0.1)
    async with Client('ws://localhost:8000', {}) as client:
        try:
            async for _ in client.stream_resources(wid, None):
                pytest.fail('Did not receive exception')
        except KeyError:
            pass
        except Exception as e:
            pytest.fail('Unexpected exception')


async def workflow():
    async with state('phrase', None, '') as phrase:
        async with queue('messages', None) as messages:
            messages = await messages.get()
            await phrase.set(messages)

@pytest.mark.asyncio
async def test_websockets():
    wid = 'test'
    historian = create_in_memory_historian({'workflow': workflow})
    historian.start_soon('workflow', wid)
    await asyncio.gather(serve(historian, 8000, authorize), connect(wid))

@pytest.mark.asyncio
async def test_websockets_exception():
    wid = 'test_exception'
    historian = create_in_memory_historian({'workflow': workflow})
    historian.start_soon('workflow', wid)
    await asyncio.gather(serve(historian, 8000, lambda h: True), connect_exception('fail'))
    await asyncio.sleep(0.1)
    await historian.delete_workflow(wid)
