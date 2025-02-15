import asyncio
import logging
import os
import sys
from contextvars import ContextVar

task_name_getter = ContextVar("task_name_getter", default=lambda: "-")


async def stdio(loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    if sys.platform == "win32":
        raise NotImplementedError()

    reader = asyncio.StreamReader(loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)

    read_pipe = os.fdopen(sys.stdin.fileno(), "rb", buffering=0)

    await loop.connect_read_pipe(lambda: protocol, read_pipe)

    return reader


# def _win32_stdio(loop):
#     class Win32StdinReader:
#         def __init__(self):
#             self.stdin = sys.stdin.buffer
#
#         async def readline(self):
#             return await loop.run_in_executor(None, sys.stdin.readline)
#
#     return Win32StdinReader()


async def ainput(prompt: str, reader=None):
    """
    Reads the stream until the end of the current content
    Stops waiting for content after `timeout` seconds
    Returns decoded content (i.e. str not bytes)
    """
    print(prompt, end="", flush=True)

    if reader is None:
        reader = await stdio()

    line = await reader.readline()
    return line.decode()


# async def read_from_stdin(reader):
#     buffer = []
#     while True:
#         token = await reader.read(1)
#         if not token:
#             # stream.read() returns an empty byte when EOF is reached
#             break
#         if token == '\n':
#             return ''.join(buffer)
#         token = token.decode()
#         buffer.append(token)
#
#         await asyncio.sleep(0)

class TaskFieldFilter(logging.Filter):
    def filter(self, record):
        try:
            record.task = task_name_getter.get()()
        except Exception as e:
            record.task = "UNKNOWN_TASK"  # Fallback if there's an issue
            print(f"Logging filter error: {e}")
        return True

logging.getLogger().addFilter(TaskFieldFilter())
quest_logger = logging.getLogger('quest')
# Create a StreamHandler to print logs to the terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Set level for the handler

# Create and set a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(task)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handler to the logger
quest_logger.addHandler(console_handler)
quest_logger.propagate=True

# Ensure root logger has a handler
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
if not root_logger.hasHandlers():
    root_logger.addHandler(console_handler)

for logger_name in logging.root.manager.loggerDict.keys():
    logger = logging.getLogger(logger_name)
    logger.addFilter(TaskFieldFilter())