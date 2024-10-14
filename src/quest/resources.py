import asyncio
import logging
from typing import Callable, Coroutine


class ResourceStreamManager:
    def __init__(self):
        self._resource_streams: dict[str, set[ResourceStreamManager.ResourceStream]] = {}

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
            self._is_entered = False
            # TODO - set any gates?

        def __enter__(self):
            self._is_entered = True
            logging.debug(f'Resource stream opened for {id(self)}')
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            # TODO: How do we let the workflow continue if it is already awaiting the stream_gate?
            self._stream_gate.set()
            self._on_close(self)
            logging.debug(f'Resource stream closed for {id(self)}')

        async def __aiter__(self):
            """
            Provide a stream of resource snapshots for this workflow.
            Everytime the workflow resource state changes, an update will be published.

            NOTE: Once you start the resource stream,
            the workflow will not progress unless you iterate this stream.
            Use wait_for_completion() to automatically iterate (and ignore) the remaining updates.
            """

            if not self._is_entered:
                raise Exception('ResourceStream must be used in a `with` context.')

            # The caller of this function lives outside the step management of the historian
            #         -> don't replay, just yield event changes in realtime

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
            lambda res_stream: self._on_close(identity, res_stream)
        )
        if identity not in self._resource_streams:
            self._resource_streams[identity] = set()
        self._resource_streams[identity].add(rs)
        return rs

    # noinspection PyProtectedMember
    async def update(self, identity):
        # If there is no resources stream associated with `identity`, no update needed.
        if identity not in self._resource_streams:
            return

        # TODO: Say a stream is added but then removed, the identity still exists as a key but just has an empty set.
        # Is it okay to leave it as such?
        for stream in self._resource_streams[identity]:
            stream._update_event.set()
            await stream._stream_gate.wait()
            stream._stream_gate.clear()

    def _on_close(self, identity, res_stream: ResourceStream):
        self._resource_streams[identity].remove(res_stream)
        if not self._resource_streams.get(identity):
            self._resource_streams.pop(identity)
