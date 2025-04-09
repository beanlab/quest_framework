import asyncio
import pytest
from history import state, queue
from history.client import Client
from history.server import Server
from history_test.utils import create_in_memory_historian


def authorize(headers: dict[str, str]) -> bool:
    if 'Authorization' not in headers:
        pytest.fail('No authorization header')
        return False
    if headers['Authorization'] == "C@n'tT0uchThis!":
        return True
    pytest.fail('Authorization key incorrect')
    return False


async def serve(historian):
    async with Server(historian, 'localhost', 8000, authorizer=authorize):
        await asyncio.sleep(1)


async def connect():
    async with Client('ws://localhost:8000', {'Authorization': "C@n'tT0uchThis!"}) as client:
        messages_seen = False
        async for resources in client.stream_resources('workflow1', None):
            print("Resources:", resources)
            if ('messages', None) in resources and not messages_seen:
                messages_seen = True
                response = await client.send_event('workflow1', 'messages', None, 'put', ['Hello world!'])
                print("Response:", response)

        if not messages_seen:
            pytest.fail('Message not found in resources')


@pytest.mark.asyncio
async def test_websockets():
    async def workflow():
        async with state('phrase', None, '') as phrase:
            async with queue('messages', None) as messages:
                messages = await messages.get()
                await phrase.set(messages)

    historian = create_in_memory_historian({'workflow': workflow})
    historian.start_soon('workflow', 'workflow1')
    await asyncio.gather(serve(historian), connect())
