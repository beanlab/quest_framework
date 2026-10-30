import dis
import inspect
import logging
import sys
from contextvars import ContextVar
from functools import lru_cache
from pathlib import Path

current_task_id = ContextVar('task_id')


class TaskFieldFilter(logging.Filter):
    def filter(self, record):
        record.task = current_task_id.get("-")
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

_suggested_handler: logging.Handler | None = None


def set_suggested_log_format() -> None:
    """Configure quest logging with the suggested console format."""
    global _suggested_handler

    formatter = logging.Formatter(
        "%(asctime)s %(levelname).4s %(task)s %(filename)s:%(lineno)d %(message)s"
    )
    if _suggested_handler is None:
        _suggested_handler = logging.StreamHandler()
        quest_logger.addHandler(_suggested_handler)

    _suggested_handler.setFormatter(formatter)
    # The handler above formats quest logs itself; avoid duplicate output via root.
    quest_logger.propagate = False


TRACE = 5
logging.addLevelName(TRACE, "TRACE")
REPO_ROOT = Path(__file__).resolve().parents[4]


def enable_trace_logging() -> None:
    """Log function calls, returns, and coroutine suspensions in this repository."""
    root = REPO_ROOT.resolve()
    quest_logger.setLevel(TRACE)
    set_suggested_log_format()

    def is_in_repo(filename: str) -> bool:
        if not filename or filename.startswith("<"):
            return False
        try:
            Path(filename).resolve().relative_to(root)
            return True
        except (OSError, ValueError):
            return False

    @lru_cache(maxsize=None)
    def instructions(code):
        return tuple(dis.get_instructions(code))

    def is_yield_suspension(frame) -> bool:
        code_instructions = instructions(frame.f_code)
        for index, instruction in enumerate(code_instructions):
            if instruction.offset != frame.f_lasti:
                continue
            if instruction.opname == "YIELD_VALUE":
                return True
            # In newer CPython versions, f_lasti can point at the RESUME
            # immediately following the YIELD_VALUE that suspended the coroutine.
            return (
                instruction.opname == "RESUME"
                and index > 0
                and code_instructions[index - 1].opname == "YIELD_VALUE"
            )
        return False

    emitting_trace_log = False

    def is_task_filter_frame(frame) -> bool:
        while frame is not None:
            if frame.f_code.co_qualname == "TaskFieldFilter.filter":
                return True
            frame = frame.f_back
        return False

    def format_call_arguments(frame) -> str:
        arg_info = inspect.getargvalues(frame)
        arguments = [
            f"{name}={arg_info.locals[name]!r}"
            for name in arg_info.args
            if name in arg_info.locals
        ]
        if arg_info.varargs:
            arguments.append(f"*{arg_info.varargs}={arg_info.locals[arg_info.varargs]!r}")
        if arg_info.keywords:
            arguments.append(f"**{arg_info.keywords}={arg_info.locals[arg_info.keywords]!r}")
        return ", ".join(arguments)

    def trace(frame, event, arg):
        nonlocal emitting_trace_log
        # Don't trace logging/filter machinery or calls made by the task filter.
        if emitting_trace_log or is_task_filter_frame(frame):
            return None

        detail = ""
        stacklevel = 2
        if event == "call":
            if not is_in_repo(frame.f_code.co_filename):
                return None
            caller = frame.f_back
            if caller is None or not is_in_repo(caller.f_code.co_filename):
                # Keep tracing this local frame, but don't report an outside-repo callsite.
                return trace
            message = "calling"
            detail = f"({format_call_arguments(frame)})"
            stacklevel = 3
        elif event == "return":
            if is_yield_suspension(frame):
                if frame.f_code.co_flags & inspect.CO_COROUTINE:
                    message = "awaiting"
                else:
                    # Ignore ordinary generator yields; they are not returns or awaits.
                    return trace
            else:
                message = "returning"
        elif event == "exception":
            exception_type, exception_value, _ = arg
            if issubclass(exception_type, StopIteration):
                return trace
            message = "raising"
            detail = f": {exception_type.__name__}: {exception_value}"
        else:
            return trace

        emitting_trace_log = True
        try:
            quest_logger.log(
                TRACE,
                "%s %s%s",
                message,
                frame.f_code.co_qualname,
                detail,
                stacklevel=stacklevel,
            )
        finally:
            emitting_trace_log = False
        return trace

    sys.settrace(trace)
