#!/usr/bin/env python
import asyncio
import json
import traceback
import websockets
from websockets import WebSocketServerProtocol

from quest import WorkflowManager
from quest.utils import quest_logger


class Server:
    def __init__(self, manager: WorkflowManager, host: str, port: int):
        """
        Initialize the server.

        :param manager: Workflow manager whose methods will be called remotely.
        :param host: Host address for the server.
        :param port: Port for the server.
        """
        self.manager: WorkflowManager = manager
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
        quest_logger.info(f'Server started at ws://{self.host}:{self.port}')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Stop the server when exiting the context.
        """
        self.server.close()
        await self.server.wait_closed()
        quest_logger.info(f'Server at ws://{self.host}:{self.port} stopped')

    async def handler(self, ws: WebSocketServerProtocol, path: str):
        """
        Handle incoming WebSocket connections and messages.

        :param ws: The WebSocket connection.
        :param path: The requested path.
        """
        quest_logger.info(f'New connection: {path}')
        if path == '/call':
            await self.handle_call(ws)
        elif path == '/stream':
            await self.handle_stream(ws)
        else:
            # TODO: Maybe provide some information about correct paths?
            response = {'error': 'Invalid path'}
            await ws.send(json.dumps(response))

    async def handle_call(self, ws: WebSocketServerProtocol):
        async for message in ws:
            try:
                print(f'Received message: {message}')
                data = json.loads(message)

                method_name = data['method']
                args = data.get('args', [])
                kwargs = data.get('kwargs', {})

                if not hasattr(self.manager, method_name):
                    response = {'error': f'Method {method_name} not found'}
                else:
                    method = getattr(self.manager, method_name)
                    if callable(method):
                        result = method(*args, **kwargs)
                        if asyncio.iscoroutine(result):
                            result = await result
                        response = {'result': result}
                    else:
                        response = {'error': f'{method_name} is not callable'}

            except (TypeError, ValueError) as e:
                # Handle JSON parsing errors or incorrect data types
                response = {
                    'error': 'Invalid message format',
                    'details': str(e),
                    'traceback': traceback.format_exc()
                }
            except Exception as e:
                # Catch any other unexpected exceptions
                response = {
                    'error': 'Error occurred during execution',
                    'details': str(e),
                    'traceback': traceback.format_exc()
                }

            await ws.send(json.dumps(response))

    async def handle_stream(self, ws: WebSocketServerProtocol):
        # Receive initial parameters
        message = await ws.recv()
        params = json.loads(message)
        wid = params.get('wid')
        ident = params.get('identity')

        with self.manager.get_resource_stream(wid, ident) as stream:
            async for resources in stream:
                await ws.send(json.dumps(resources))
