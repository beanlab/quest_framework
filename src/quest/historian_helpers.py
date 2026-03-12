from datetime import datetime


def get_function_name(func):
    if hasattr(func, '__name__'):
        return func.__name__
    return func.__class__.__name__


def _get_id(item):
    if isinstance(item, dict):
        return tuple((k, _get_id(v)) for k, v in item.items())

    if isinstance(item, list):
        return tuple(_get_id(v) for v in item)

    return item


def _get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


def _create_resource_id(name: str, identity: str | None) -> str:
    return f'{name}|{identity}' if identity is not None else name


def _get_type_name(obj):
    return obj.__class__.__module__ + '.' + obj.__class__.__name__


def _get_qualified_version(module_name, function_name, version_name: str) -> str:
    """
    A version is defined by the module and name of the function that is versioned.
    If you move a function to a new module (or rename the module), it has become a new function.
    If you change the function you are calling in a replay (i.e. its name has changed),
    you may not get the expected results.

    You've been warned.
    """
    return '.'.join([module_name, function_name, version_name])
