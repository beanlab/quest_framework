from contextvars import Context
from typing import Generator, Any


class ContextDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __enter__(self):
        for key, value in self.items():
            if hasattr(value, '__enter__'):
                self[key] = value.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for key, value in self.items():
            if hasattr(value, '__exit__'):
                value.__exit__(exc_type, exc_val, exc_tb)

    async def __aenter__(self):
        for key, value in self.items():
            if hasattr(value, '__aenter__'):
                self[key] = await value.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for key, value in self.items():
            if hasattr(value, '__aexit__'):
                await value.__aexit__(exc_type, exc_val, exc_tb)


class ContextList(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def __aenter__(self):
        for index, value in enumerate(self):
            if hasattr(value, '__aenter__'):
                self[index] = await value.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for value in self:
            if hasattr(value, '__aexit__'):
                await value.__aexit__(exc_type, exc_val, exc_tb)

    async def add(self, context):
        if hasattr(context, '__aenter__'):
            self.append(await context.__aenter__())
        else:
            self.append(context)


def these(collection_of_contexts: dict | list | Generator[Context, Any, None]):
    if isinstance(collection_of_contexts, dict):
        return ContextDict(collection_of_contexts)
    else:
        return ContextList(collection_of_contexts)
