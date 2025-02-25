import sys
import asyncio
import websockets

async def converse(websocket):
    async def listen():
        async for message in websocket:
            print(f"< {message}")

    async def write():
        while True:
            message = await ainput('> ')
            await websocket.send(message)

    await asyncio.gather(listen(), write())

async def main(is_server):
    if is_server:
        async with websockets.serve(converse, "localhost", 8765):
            await asyncio.Future()
    else:
        uri = "ws://localhost:8765"  # The URI of your websocket server
        async with websockets.connect(uri) as websocket:
            await converse(websocket)

# For asynchronous input (aiinput function)
async def ainput(prompt: str = ""):
    return await asyncio.get_event_loop().run_in_executor(None, input, prompt)

# Entry point
if __name__ == "__main__":
    asyncio.run(main(len(sys.argv) > 1))