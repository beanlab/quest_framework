import asyncio
import logging
from typing import Callable, Coroutine


# noinspection PyProtectedMember
class ResourceStreamManager:
    def __init__(self):
        self._resource_streams: dict[str, set[ResourceStreamManager.ResourceStream]] = {}
        self._streams_to_close = []
        self._streams_to_open = []

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
            # We need to allow the workflow to continue but we can't remove from the set if we are in the for each loop
            self._on_close(self)
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
        self._streams_to_open.append((identity, rs))
        return rs

    async def update(self, identity):
        # Open any pending resource streams for updates
        for rs_identity, rs in self._streams_to_open:
            if rs_identity not in self._resource_streams:
                self._resource_streams[rs_identity] = set()
            self._resource_streams[rs_identity].add(rs)
        self._streams_to_open.clear()

        # If there is no resources stream associated with `identity`, no update needed.
        if identity not in self._resource_streams:
            return

        # Notify each stream of `identity`
        for stream in self._resource_streams[identity]:
            stream._update_event.set()
            await stream._stream_gate.wait()
            stream._stream_gate.clear()

        """ Problem... It is true that any streams of `identity` will be triggered for close if needed 
        before update finishes notifying. However isn't it possible that a stream of a different identity
        is triggered for close after successful notification?
        
        - Notify none resource stream about update to resource of none identity
        - Caller successfully receives update and calls `anext` allowing `update` to exit.
        - While waiting for the next update of `none` the caller errors and calls __exit on resource stream.
        - Now rs of `none` is now triggered for close
        - workflow triggers an update of identity: `kyle`
        - update finishes notifying kyle and attempts to clean up streams triggered for close.
        - assertion fails. identity: `none` != identity: `kyle`
        
        This depends on if a caller of resource stream is even given execution time while workflow is running
        between resource updates. Does a raising of exception in the caller trigger execution?
        """

        """ Clean up resource streams that were triggered for close.
        This is done here to avoid changing the resource_streams set while being iterated.
        It is safe to assume that all streams triggered for close will be of the same identity for this update
        because update does not complete until 
        """
        for rs_identity, rs in self._streams_to_close:
            assert rs_identity == identity
            self._resource_streams[identity].remove(rs)
        self._streams_to_close.clear()

        if not self._resource_streams.get(identity):  # Clean up dictionary values if needed
            self._resource_streams.pop(identity)

    def _on_close(self, identity, res_stream: ResourceStream):
        self._streams_to_close.append((identity, res_stream))
        res_stream._stream_gate.set()
