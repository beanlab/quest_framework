import logging
from contextvars import ContextVar
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
