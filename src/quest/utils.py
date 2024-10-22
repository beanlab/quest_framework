import asyncio
import importlib


async def ainput(*args):
    return await asyncio.to_thread(input, *args)


def get_obj_name(obj):
    # Return name of the object (module + qualified name)
    return obj.__module__ + '.' + obj.__qualname__


def import_object(full_name: str):
    # Import and return object with its full name
    module_name, attr_name = full_name.rsplit('.', 1)
    module = __import__(module_name, fromlist=[attr_name])
    return getattr(module, attr_name)
