import asyncio
import logging
from contextvars import ContextVar
import sys

task_name_getter = ContextVar("task_name_getter", default=lambda : "-")

async def ainput(*args):
    return await asyncio.to_thread(input, *args)

# class QuestLogger(logging.Logger):
#     def makeRecord(self, *args, **kwargs):
#         rv = super(QuestLogger, self).makeRecord(*args, **kwargs)
#         task_name = task_name_getter.get()()
#         rv.__dict__["task"] = rv.__dict__.get("task", task_name)
#         return rv
#
# quest_logger = QuestLogger('quest')

class TaskFieldFilter(logging.Filter):
    def filter(self, record):
        record.task = task_name_getter.get()()
        return True

logging.getLogger().addFilter(TaskFieldFilter())
quest_logger = logging.getLogger('quest')

for logger_name in logging.root.manager.loggerDict.keys():
    logger = logging.getLogger(logger_name)
    logger.addFilter(TaskFieldFilter())