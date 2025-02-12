import asyncio
import json
import websockets

from quest import state, queue
from quest.server import Server
from utils import create_in_memory_workflow_manager


async def workflow():
    async with state('phrase', None, '') as phrase:
        async with queue('messages', None) as messages:
            messages = await messages.get()
            await phrase.set(messages)


manager = create_in_memory_workflow_manager({'workflow': workflow})


async def serve():
    async with Server.serve(manager, 'localhost', 8000):
        await asyncio.Future()


async def connect():
    print("Connecting...")
    async with websockets.connect('ws://localhost:8000/stream') as ws:
        first_message = {
            'wid': 'workflow1',
            'identity': None,
        }

        await ws.send(json.dumps(first_message))

        response = await ws.recv()
        response_data = json.loads(response)
        print("Response:", response_data)


async def main():
    manager.start_workflow('workflow', 'workflow1')
    await asyncio.gather(serve(), connect())


if __name__ == '__main__':
    asyncio.run(main())