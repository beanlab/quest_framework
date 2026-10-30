from functools import wraps, WRAPPER_ASSIGNMENTS
from typing import Self

from .core.historian import Historian


class Workflow:
    # This class embodies the public interface of Historian
    
    @wraps(Historian.__init__)
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    async def __aenter__(self) -> Self:
        self._historian = Historian(*self._args, **self._kwargs)
        await self._historian.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._historian.__aexit__(exc_val, exc_val, exc_tb)

    def __await__(self):
        return self._historian.__await__()

    @wraps(Historian.get_resource_stream)
    def resource_stream(self, *args, **kwargs):
        return self._historian.get_resource_stream(*args, **kwargs)

    @wraps(
        Historian.external_invoke_action,
        assigned=tuple(
            attr for attr in WRAPPER_ASSIGNMENTS
            if attr not in {"__name__", "__qualname__"}
        ),
    )
    async def invoke_action(self, *args, **kwargs):
        return await self._historian.external_invoke_action(*args, **kwargs)
