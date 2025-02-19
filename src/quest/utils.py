import asyncio
import logging
import os
import sys
from contextvars import ContextVar

task_name_getter = ContextVar("task_name_getter", default=lambda: "-")

import asyncio
import sys

async def stdio():
    if sys.platform == "win32":
        raise NotImplementedError()

    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)

    await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, sys.stdin.buffer)

    return reader

async def ainput(prompt: str, reader=None):
    print(prompt, end="", flush=True)
    reader = reader or await stdio()
    return (await reader.readline()).decode().strip()

class TaskFieldFilter(logging.Filter):
    def filter(self, record):
        record.task = task_name_getter.get()()
        return True


logging.getLogger().addFilter(TaskFieldFilter())
quest_logger = logging.getLogger('quest')

for logger_name in logging.root.manager.loggerDict.keys():
    logger = logging.getLogger(logger_name)
    logger.addFilter(TaskFieldFilter())
