import asyncio

from quest import state, queue
from quest.server import Server
from quest.client import Client
from utils import create_in_memory_workflow_manager


async def serve(manager):
    async with Server(manager, 'localhost', 8000):
        await asyncio.Future()


async def connect():
    async with Client('ws://localhost:8000', "C@n'tT0uchThis!") as client:
        messages_seen = False
        async for resources in client.stream_resources('workflow1', None):
            print("Resources:", resources)
            if "('messages', None)" in resources and not messages_seen:
                messages_seen = True
                response = await client.send_event('workflow1', 'messages', None, 'put', ['Hello world!'])
                print("Response:", response)


async def main():
    async def workflow():
        async with state('phrase', None, '') as phrase:
            async with queue('messages', None) as messages:
                messages = await messages.get()
                await phrase.set(messages)

    manager = create_in_memory_workflow_manager({'workflow': workflow})
    manager.start_workflow('workflow', 'workflow1')
    await asyncio.gather(serve(manager), connect())


if __name__ == '__main__':
    asyncio.run(main())
