#!/usr/bin/env python

import asyncio
import websockets
import jwt

from jwt.exceptions import InvalidTokenError
from src.quest import WorkflowManager

SECRET_KEY = "C@n'tT0uchThis!"

async def receive_messages(manager: WorkflowManager, ident, websocket, path):
    async for message in websocket:
        print("Received message: ", message)
        # await manager.send_event(ident, message)

async def send_messages(manager, ident, websocket, path):
    # How exactly are we calling stream_resources on the manager?
    async for status in manager.stream_resources(ident):
        websocket.send(status)

async def authorize(websocket):
    try:
        token = websocket.request_headers['Authorization']
        if token.startswith("Bearer "):
            token = token.split(" ")[1]
        else:
            return False

        decoded_token = jwt.decode(token, SECRET_KEY, algorithms="HS256")
        return decoded_token
    except (KeyError, InvalidTokenError):
        return False

async def default_handler(manager, websocket, path):
    authorized = await authorize(websocket)
    if not authorized:
        await websocket.close(code=websocket.CLOSE_STATUS_POLICY_VIOLATION, reason="Unauthorized")
        return

    ident = authorized['ident']

    await asyncio.gather(
        receive_messages(manager, ident, websocket, path),
        send_messages(manager, ident, websocket, path)
    )

async def serve(manager, host, port):
    # Start the WebSocket server
    async with websockets.serve(lambda ws, p: default_handler(manager, ws, p), host, port):
        await asyncio.Future()

async def initialize(namespace, storage, create_history, workflow):
    async with WorkflowManager(namespace, storage, create_history, lambda w_type: workflow) as manager:
        await serve(manager, "localhost", 8765)

if __name__ == "__main__":
    asyncio.run(initialize())
