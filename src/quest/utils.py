import asyncio
import logging
import os
import sys
from contextvars import ContextVar
import traceback

task_name_getter = ContextVar("task_name_getter", default=lambda: "-")

import asyncio


async def ainput(*args):
    return await asyncio.to_thread(input, *args)


class TaskFieldFilter(logging.Filter):
    def filter(self, record):
        record.task = task_name_getter.get()()
        return True


logging.getLogger().addFilter(TaskFieldFilter())
quest_logger = logging.getLogger('quest')

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
        "details": traceback.format_exc()
    }


def deserialize_exception(data: dict) -> Exception:
    exc_cls = get_exception_class(data["type"])
    return exc_cls(*data.get("args", ()))
