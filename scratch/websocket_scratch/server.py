#!/usr/bin/env python

import json
from typing import TypedDict
import websockets
from websockets import WebSocketServerProtocol


class RemoteTargetCallMessage(TypedDict):
    method: str
    args: list
    kwargs: dict


class Server:
    def __init__(self, target, host: str, port: int):
        """
        Initialize the server.

        :param target: Object whose methods will be called remotely.
        :param host: Host address for the server.
        :param port: Port for the server.
        """
        self.target = target
        self.host = host
        self.port = port
        self.server = None

    @classmethod
    def serve(cls, target, host: str, port: int):
        """
        Create and return a server instance for use in an async context.
        """
        instance = cls(target, host, port)
        return instance

    async def __aenter__(self):
        """
        Start the server in an async with context.
        """
        self.server = await websockets.serve(self.handler, self.host, self.port)
        print(f"Server started at ws://{self.host}:{self.port}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Stop the server when exiting the context.
        """
        self.server.close()
        await self.server.wait_closed()
        print("Server stopped")

    async def handler(self, websocket: WebSocketServerProtocol, path: str):
        """
        Handle incoming WebSocket connections and messages.

        :param websocket: The WebSocket connection.
        :param path: The requested path.
        """
        print(f"New connection: {path}")
        async for message in websocket:
            try:
                print(f"Received message: {message}")
                data = json.loads(message)

                # Do we really need this typing? It feels like extra work
                parsed_data: RemoteTargetCallMessage = RemoteTargetCallMessage(
                    method=data.get("method"),
                    args=data.get("args", []),
                    kwargs=data.get("kwargs", {})
                )

                method_name = parsed_data["method"]
                args = parsed_data["args"]
                kwargs = parsed_data["kwargs"]

                if not hasattr(self.target, method_name):
                    response = {"error": f"Method '{method_name}' not found"}
                else:
                    method = getattr(self.target, method_name)
                    if callable(method):
                        result = method(*args, **kwargs)
                        response = {"result": result}
                    else:
                        response = {"error": f"'{method_name}' is not callable"}

            except (TypeError, ValueError) as e:
                # Handle JSON parsing errors or incorrect data types
                response = {"error": "Invalid message format", "details": str(e)}
            except Exception as e:
                # Catch any other unexpected exceptions
                response = {"error": str(e)}

            await websocket.send(json.dumps(response))
