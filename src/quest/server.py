#!/usr/bin/env python
import asyncio
import json
from typing import Callable
from websockets import WebSocketException, Headers
from websockets.asyncio.server import serve, ServerConnection
from websockets.exceptions import ConnectionClosedOK

from quest import WorkflowManager
from quest.utils import quest_logger, serialize_exception


class MethodNotFoundException(Exception):
    pass


class InvalidParametersException(Exception):
    pass


class InvalidPathException(Exception):
    pass


async def serialize_resources(resources):
    serialized_resources = {}
    for key, value in resources.items():
        assert isinstance(key, tuple)
        new_key = '||'.join(k if k is not None else '' for k in key)
        serialized_resources[new_key] = value
    return {'resources': serialized_resources}


class Server:
    def __init__(self, manager: WorkflowManager, host: str, port: int,
                 authorizer: Callable[[Headers], bool] = lambda headers: True):
        """
        Initialize the server.

        :param manager: Workflow manager whose methods will be called remotely.
        :param host: Host address for the server.
        :param port: Port for the server.
        :param authorizer: Used to authenticate incoming connections.
        """
        self._server = None
        self._manager: WorkflowManager = manager
        self._host = host
        self._port = port
        self._authorizer = authorizer

    async def __aenter__(self):
        """
        Start the server in an async with context.
        """
        self._server = serve(self.handler, self._host, self._port)
        await self._server.__aenter__()
        quest_logger.info(f'Server started at ws://{self._host}:{self._port}')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Stop the server when exiting the context.
        """
        await self._server.__aexit__(exc_type, exc_val, exc_tb)
        quest_logger.info(f'Server at ws://{self._host}:{self._port} stopped')

    async def handler(self, ws: ServerConnection):
        """
        Handle incoming WebSocket connections and messages.

        :param ws: The WebSocket server connection.
        """
        if not (self._authorizer(ws.request.headers)):
            await ws.close(reason="Unauthorized")
            quest_logger.info(f'Unauthorized attempt to connect from {ws.remote_address[0]}')
            return

        try:
            quest_logger.info(f'Opened connection id: {ws.id}, address: {ws.remote_address[0]}')
            match ws.request.path:
                case "/call":
                    await self.handle_call(ws)
                case "/stream":
                    await self.handle_stream(ws)
                case _:
                    response = {
                        'exception': serialize_exception(InvalidPathException(f'Invalid path: {ws.request.path}'))}
                    await ws.send(json.dumps(response))
        except ConnectionClosedOK:
            pass
        finally:
            quest_logger.info(f'Closed connection id: {ws.id}, address: {ws.remote_address[0]}')

    async def handle_call(self, ws: ServerConnection):
        async for message in ws:
            try:
                data = json.loads(message)

                if 'method' not in data or 'args' not in data or 'kwargs' not in data:
                    raise InvalidParametersException()
                method_name = data['method']
                args = data['args']
                kwargs = data['kwargs']

                if not hasattr(self._manager, method_name):
                    raise MethodNotFoundException(f'{method_name} is not a valid method')
                method = getattr(self._manager, method_name)
                if not callable(method):
                    raise MethodNotFoundException(f'{method_name} is not callable')

                result = method(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    result = await result
                response = {'result': result}
            except WebSocketException as e:
                raise e
            except Exception as e:
                response = {'exception': serialize_exception(e)}
            await ws.send(json.dumps(response))

    async def handle_stream(self, ws: ServerConnection):
        try:
            # Receive initial parameters
            message = await ws.recv()
            params = json.loads(message)
            if 'wid' not in params or 'identity' not in params:
                raise InvalidParametersException()
            wid = params['wid']
            ident = params['identity']

            # Stream resource updates via ws messages
            with self._manager.get_resource_stream(wid, ident) as stream:
                async for resources in stream:
                    # Serialize tuple keys into strings joined by '||'
                    resources = await serialize_resources(resources)
                    await ws.send(json.dumps(resources))
        except WebSocketException as e:
            raise e
        except Exception as e:
            response = {'exception': serialize_exception(e)}
            await ws.send(json.dumps(response))
