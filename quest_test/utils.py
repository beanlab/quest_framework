import asyncio
import sys
from functools import wraps


def timeout(delay):
    if 'pydevd' in sys.modules:  # i.e. debug mode
        # Return a no-op decorator
        return lambda func: func

    def decorator(func):
        @wraps(func)
        async def new_func(*args, **kwargs):
            async with asyncio.timeout(delay):
                return await func(*args, **kwargs)

        return new_func

    return decorator

