import asyncio
import websockets

from src.quest import Historian, step
from src.quest.external import state


async def myWebsocket():
    @step
    async def hello(websocket):
        async with state('name', None, await websocket.recv()) as name:
            async with state('greeting', None, f'Hello {await name.get()}!') as greeting:
                print(f"<<< {await name.get()}")
                await websocket.send(await greeting.get())
                print(f">>> {await greeting.get()}")

    async def workflow():
        async with websockets.serve(hello, "localhost", 8765):
            await asyncio.Future()

    history = []
    historian = Historian('websocket_test', workflow, history)
    historian.run()

    index = 0
    async for resources in historian.stream_resources(None):
        if index == 0:  # 'name' created
            assert 'name' in resources
            assert resources['name']['value'] == 'Kyle'

        elif index == 2:
            assert 'greeting' in resources
            assert resources['greeting']['value'] == 'Hello Kyle!'

        # elif index == 7:
        #     break

        index += 1

    # await historian.suspend()
    # historian.run()
    #
    # index = 0
    # async for resources in historian.stream_resources(None):
    #     if index == 0:
    #         pass

asyncio.run(myWebsocket())
