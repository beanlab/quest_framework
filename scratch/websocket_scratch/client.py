import asyncio
import websockets
import jwt

from quest import ainput

SECRET_KEY = "C@n'tT0uchThis!"
TOKEN = jwt.encode({"ident": "client123"}, SECRET_KEY, algorithm="HS256")

async def send_messages(websocket):
    while True:
        message = await ainput("Enter message to send: ")
        await websocket.send(message)

async def receive_messages(websocket):
    async for message in websocket:
        print(f"Received message: {message}")

async def main():
    uri = "ws://localhost:8765"
    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    async with websockets.connect(uri, extra_headers=headers) as websocket:
        await asyncio.gather(
            send_messages(websocket),
            receive_messages(websocket)
        )

if __name__ == "__main__":
    asyncio.run(main())
