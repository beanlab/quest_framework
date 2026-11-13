import datetime
import traceback


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


def _get_type_name(obj):
    return obj.__class__.__module__ + '.' + obj.__class__.__name__


def _get_exception_class(exception_type: str):
    module_name, class_name = exception_type.rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    exception_class = getattr(module, class_name)
    return exception_class


def serialize_exception(ex: BaseException) -> dict:
    return {
        "type": _get_type_name(ex),
        "args": ex.args,
        "traceback": traceback.format_exc()
    }


def deserialize_exception(data: dict) -> Exception:
    exc_cls = _get_exception_class(data["type"])
    exc_instance: Exception = exc_cls(*data.get("args", ()))
    exc_instance._original_traceback = data["traceback"]  # Include original tracebook info
    return exc_instance
