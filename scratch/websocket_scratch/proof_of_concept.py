import asyncio
import json

import websockets

from scratch.websocket_scratch.server import Server, RemoteTargetCallMessage

class Target:
    def hello(self):
        print('Hello world!')

async def serve():
    async with Server.serve(Target(), 'localhost', 8000):
        await asyncio.Future()

async def connect():
    print("Connecting...")
    async with websockets.connect('ws://localhost:8000') as ws:
        call: RemoteTargetCallMessage = {
            'method': 'hello',
            'args': [],
            'kwargs': {},
        }

        await ws.send(json.dumps(call))

        response = await ws.recv()
        response_data = json.loads(response)
        print("Response:", response_data)

async def main():
    await asyncio.gather(serve(), connect())

if __name__ == '__main__':
    asyncio.run(main())