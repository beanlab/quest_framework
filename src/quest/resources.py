import asyncio
import logging
from typing import Callable, Coroutine


class ResourceStreamManager:
    def __init__(self):
        self.resource_streams: dict[str, set[ResourceStreamManager.ResourceStream]] = {}

    class ResourceStreamContext:
        def __init__(self, on_close: Callable):
            self._on_close = on_close

        def __aenter__(self):
            logging.debug(f'Resource stream opened for {id(self)}')
            return self

        async def __aexit__(self, exc_type, exc_value, traceback):
            self._on_close()
            logging.debug(f'Resource stream closed for {id(self)}')

    class ResourceStream:
        def __init__(self,
                     get_resources: Callable[[], Coroutine],
                     is_workflow_complete: Callable[[], bool],
                     on_close: Callable[['ResourceStreamManager.ResourceStream'], None]
                     ):
            self._stream_gate = asyncio.Event()
            self._update_event = asyncio.Event()
            self._get_resources = get_resources
            self._is_workflow_complete = is_workflow_complete
            self._on_close = on_close
            # TODO - set any gates?

        async def __aiter__(self):
            """
            Provide a stream of resource snapshots for this workflow.
            Everytime the workflow resource state changes, an update will be published.

            NOTE: Once you start the resource stream,
            the workflow will not progress unless you iterate this stream.
            Use wait_for_completion() to automatically iterate (and ignore) the remaining updates.
            """

            # The caller of this function lives outside the step management of the historian
            #         -> don't replay, just yield event changes in realtime

            async with ResourceStreamManager.ResourceStreamContext(lambda: self._on_close(self)):
                # Yield the current resources immediately
                yield await self._get_resources()

                while not self._is_workflow_complete():
                    # Yield new resources updates as they become available
                    await self._update_event.wait()
                    self._update_event.clear()
                    yield await self._get_resources()
                    self._stream_gate.set()

    def get_resource_stream(self,
                            identity,
                            get_resources: Callable[[], Coroutine],
                            is_workflow_complete: Callable[[], bool]
                            ):
        rs = ResourceStreamManager.ResourceStream(
            get_resources,
            is_workflow_complete,
            lambda res_stream: self.resource_streams[identity].remove(res_stream)
        )
        self.resource_streams[identity].add(rs)
        return rs

    async def update(self, identity):
        for stream in self.resource_streams[identity]:
            stream._update_event.set()
            await stream._stream_gate.wait()
            stream._stream_gate.clear()
