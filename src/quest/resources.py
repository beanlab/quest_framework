import asyncio
import logging
from typing import Callable, Coroutine


# noinspection PyProtectedMember
class ResourceStreamManager:
    def __init__(self):
        self._resource_streams: dict[str, set[ResourceStreamManager.ResourceStream]] = {}
        self._streams_to_close = []
        self._streams_to_open = []
        self._streams_set_locks: dict[str, asyncio.Event] = {}

    class ResourceStream:
        def __init__(self,
                     get_resources: Callable[[], Coroutine],
                     is_workflow_complete: Callable[[], bool],
                     on_open: Callable[['ResourceStreamManager.ResourceStream'], Coroutine],
                     on_close: Callable[['ResourceStreamManager.ResourceStream'], Coroutine]
                     ):
            self._stream_gate = asyncio.Event()
            self._update_event = asyncio.Event()
            self._get_resources = get_resources
            self._is_workflow_complete = is_workflow_complete
            self._on_open = on_open
            self._on_close = on_close
            self._is_entered = False

        async def __aenter__(self):
            self._is_entered = True
            await self._on_open(self)
            logging.debug(f'Resource stream opened for {id(self)}')
            return self

        async def __aexit__(self, exc_type, exc_value, traceback):
            await self._on_close(self)
            logging.debug(f'Resource stream closed for {id(self)}')

        async def __aiter__(self):
            """
            Provide a stream of resource snapshots for this workflow.
            Everytime the workflow resource state changes, an update will be published.

            NOTE: Once you start the resource stream,
            the workflow will not progress unless you iterate this stream.
            """

            if not self._is_entered:
                raise Exception('ResourceStream must be used in a `with` context.')

            # TODO: Do we still need this explanation?
            # The caller of this function lives outside the step management of the historian
            #         -> don't replay, just yield event changes in realtime

            yield await self._get_resources()  # Yield the current resources immediately

            # TODO: What if the workflow is complete but we are already in this loop awaiting a update_event? How close?
            # Yield new resources updates as they become available
            while not self._is_workflow_complete():
                await self._update_event.wait()
                self._update_event.clear()
                yield await self._get_resources()
                self._stream_gate.set()

    async def _on_open(self, identity, res_stream: ResourceStream):
        if identity not in self._resource_streams:
            assert identity not in self._streams_set_locks
            self._streams_set_locks[identity] = asyncio.Event()
            self._streams_set_locks[identity].set()

            self._resource_streams[identity] = set()

        await self._streams_set_locks[identity].wait()
        self._resource_streams[identity].add(res_stream)

    async def _on_close(self, identity, res_stream: ResourceStream):
        res_stream._stream_gate.set()

        await self._streams_set_locks[identity].wait()
        self._resource_streams[identity].remove(res_stream)

        if not self._resource_streams.get(identity):  # Clean up dictionary values if needed
            self._resource_streams.pop(identity)
            del self._streams_set_locks[identity]

    # TODO: Change all usages to await
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
        # If there is no resources stream associated with `identity`, no update needed.
        if identity not in self._resource_streams:
            return

        self._streams_set_locks[identity].clear()  # Block other tasks from modifying the resource_streams set

        # Notify each stream of `identity`
        for stream in self._resource_streams[identity]:
            stream._update_event.set()
            await stream._stream_gate.wait()
            stream._stream_gate.clear()

        self._streams_set_locks[identity].set()  # Allow other tasks to modify the resource_streams set
