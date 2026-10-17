from datetime import datetime


def get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


def get_id(item):
    if isinstance(item, dict):
        return tuple((k, get_id(v)) for k, v in item.items())

    if isinstance(item, list):
        return tuple(get_id(v) for v in item)

    return item
