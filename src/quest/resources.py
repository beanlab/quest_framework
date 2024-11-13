import asyncio
import logging
from typing import Callable, Coroutine


# noinspection PyProtectedMember
class ResourceStreamManager:
    def __init__(self):
        self._resource_streams: dict[str | None, set[ResourceStreamManager.ResourceStream]] = {}

    class ResourceStream:
        def __init__(self,
                     get_resources: Callable[[], Coroutine],
                     is_workflow_complete: Callable[[], bool],
                     on_open: Callable[['ResourceStreamManager.ResourceStream'], None],
                     on_close: Callable[['ResourceStreamManager.ResourceStream'], None]
                     ):
            self._stream_gate = asyncio.Event()
            self._update_event = asyncio.Event()
            self._update_cancelled = False
            self._is_entered = False

            self._get_resources = get_resources
            self._is_workflow_complete = is_workflow_complete
            self._on_open = on_open
            self._on_close = on_close

        def __enter__(self):
            self._is_entered = True
            self._on_open(self)
            logging.debug(f'Resource stream opened for {id(self)}')
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._is_entered = False
            self._on_close(self)
            logging.debug(f'Resource stream closed for {id(self)}')

        async def __aiter__(self):
            """
            Provide a stream of resource snapshots for this workflow.
            Everytime the workflow resource state changes, an update will be published.

            NOTE: Once you start the resource stream,
            the workflow will not progress unless you iterate this stream or exit the `with` context.
            """

            if not self._is_entered:
                raise Exception('ResourceStream must be used in a `with` context.')

            yield await self._get_resources()  # Yield the current resources immediately

            # TODO: What if the workflow is complete but we are already in this loop awaiting a update_event? How close?
            # Yield new resources updates as they become available
            while not self._is_workflow_complete():
                await self._update_event.wait()
                if self._update_cancelled:
                    self.__exit__(asyncio.CancelledError, 'Workflow was suspended', None)
                    break
                self._update_event.clear()
                yield await self._get_resources()
                self._stream_gate.set()

    def _on_open(self, identity, res_stream: ResourceStream):
        if identity not in self._resource_streams:
            self._resource_streams[identity] = set()

        self._resource_streams[identity].add(res_stream)

    def _on_close(self, identity, res_stream: ResourceStream):
        res_stream._stream_gate.set()
        self._resource_streams[identity].remove(res_stream)

        if not self._resource_streams[identity]:  # Clean up dictionary values if needed
            self._resource_streams.pop(identity)

    def get_resource_stream(self,
                            identity,
                            get_resources: Callable[[], Coroutine],
                            is_workflow_complete: Callable[[], bool]
                            ):
        rs = ResourceStreamManager.ResourceStream(
            get_resources,
            is_workflow_complete,
            lambda res_stream: self._on_open(identity, res_stream),
            lambda res_stream: self._on_close(identity, res_stream)
        )
        return rs

    async def update(self, identity):
        try:
            # If there is no resource stream associated with `identity`, no update needed.
            if identity is not None and identity not in self._resource_streams:
                return

            # Set streams to a copy to avoid set size changed exception
            if identity is None:
                streams = {key: value.copy() for key, value in self._resource_streams.items()}
            else:
                streams = {identity: self._resource_streams[identity].copy()}

            for stream_identity, stream_set in streams.items():
                for stream in stream_set:
                    if stream not in self._resource_streams[stream_identity]:  # Continue if the stream has closed
                        continue
                    stream._update_event.set()
                    await stream._stream_gate.wait()
                    stream._stream_gate.clear()

        except asyncio.CancelledError:
            for stream_identity, stream_set in self._resource_streams.items():
                for stream in stream_set:
                    stream._update_cancelled = True
                    stream._update_event.set()
