#!/usr/bin/env python
import asyncio
import json
import traceback
from collections.abc import Callable
from typing import Dict

import websockets
from websockets import WebSocketServerProtocol

from quest import WorkflowManager, MasterSerializer
from quest.utils import quest_logger


class Server:
    def __init__(self, manager: WorkflowManager, host: str, port: int, authorizer: Callable[[Dict[str, str]], bool]):
        """
        Initialize the server.

        :param manager: Workflow manager whose methods will be called remotely.
        :param host: Host address for the server.
        :param port: Port for the server.
        """
        self._manager: WorkflowManager = manager
        self._host = host
        self._port = port
        self._authorizer = authorizer
        self._server = None

    async def __aenter__(self):
        """
        Start the server in an async with context.
        """
        self._server = await websockets.serve(self.handler, self._host, self._port)
        quest_logger.info(f'Server started at ws://{self._host}:{self._port}')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Stop the server when exiting the context.
        """
        self._server.close()
        await self._server.wait_closed()
        quest_logger.info(f'Server at ws://{self._host}:{self._port} stopped')

    async def handler(self, ws: WebSocketServerProtocol, path: str):
        """
        Handle incoming WebSocket connections and messages.

        :param ws: The WebSocket connection.
        :param path: The requested path.
        """
        if not (self._authorizer(ws.request_headers)):
            await ws.close(reason="Unauthorized")
            return

        quest_logger.info(f'New connection: {path}')
        if path == '/call':
            await self.handle_call(ws)
        elif path == '/stream':
            await self.handle_stream(ws)
        else:
            response = {'error': 'Invalid path'}
            await ws.send(json.dumps(response))

    async def handle_call(self, ws: WebSocketServerProtocol):
        async for message in ws:
            try:
                data = json.loads(message)

                method_name = data['method']
                args = data['args']
                kwargs = data['kwargs']

                if not hasattr(self._manager, method_name):
                    response = {'error': f'Method {method_name} not found'}
                else:
                    method = getattr(self._manager, method_name)
                    if callable(method):
                        result = method(*args, **kwargs)
                        if asyncio.iscoroutine(result):
                            result = await result
                        response = {'result': result}
                    else:
                        response = {'error': f'{method_name} is not callable'}

            # TODO: Use built-in Quest Serialization
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
        wid = params['wid']
        ident = params['identity']

        with self._manager.get_resource_stream(wid, ident) as stream:
            async for resources in stream:
                # Serialize tuple keys into strings joined by '||'
                resources = await self._serialize_resources(resources)
                await ws.send(json.dumps(resources))

    async def _serialize_resources(self, resources):
        serialized_resources = {}
        for key, value in resources.items():
            assert isinstance(key, tuple)
            # Should 'None' be a protected string for valid identities?
            new_key = '||'.join(k if k is not None else 'None' for k in key)
            serialized_resources[new_key] = value
        return serialized_resources
