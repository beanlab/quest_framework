import logging
from contextvars import ContextVar
import traceback
import asyncio

task_name_getter = ContextVar("task_name_getter", default=lambda: "-")


async def ainput(*args):
    return await asyncio.to_thread(input, *args)


class TaskFieldFilter(logging.Filter):
    def filter(self, record):
        record.task = task_name_getter.get()()
        return True


# Class to be used to add our TaskFieldFilter to any new loggers
class TaskFieldLogger(logging.getLoggerClass()):
    def __init__(self, name):
        super().__init__(name)
        self.addFilter(TaskFieldFilter())


# Set class to be used for instantiating loggers
logging.setLoggerClass(TaskFieldLogger)

logging.getLogger().addFilter(TaskFieldFilter())  # Add filter on root logger
quest_logger = logging.getLogger('quest')  # Create custom quest logger

# Add filter on any existing loggers
for logger_name in logging.root.manager.loggerDict.keys():
    logger = logging.getLogger(logger_name)
    logger.addFilter(TaskFieldFilter())


def get_type_name(obj):
    return obj.__class__.__module__ + '.' + obj.__class__.__name__


def get_exception_class(exception_type: str):
    module_name, class_name = exception_type.rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    exception_class = getattr(module, class_name)
    return exception_class


def serialize_exception(ex: BaseException) -> dict:
    return {
        "type": get_type_name(ex),
        "args": ex.args,
        "traceback": traceback.format_exc()
    }


def deserialize_exception(data: dict) -> Exception:
    exc_cls = get_exception_class(data["type"])
    exc_instance: Exception = exc_cls(*data.get("args", ()))
    exc_instance._original_traceback = data["traceback"]  # Include original tracebook info
    return exc_instance
