import datetime


def get_current_timestamp() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def get_id(item):
    if isinstance(item, dict):
        return tuple((k, get_id(v)) for k, v in item.items())

    if isinstance(item, list):
        return tuple(get_id(v) for v in item)

    return item


def get_function_name(func):
    if hasattr(func, '__name__'):
        return func.__name__
    return func.__class__.__name__


