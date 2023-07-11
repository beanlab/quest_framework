import inspect
import traceback
from asyncio import Task
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import Any, Protocol, Optional, Callable, TypedDict, Literal, Coroutine
from .events import UniqueEvent, EventManager
from dataclasses import dataclass
import logging
import asyncio

ARGUMENTS = "INITIAL_ARGUMENTS"
KW_ARGUMENTS = "INITIAL_KW_ARGUMENTS"

workflow_context = ContextVar('workflow')


# Creation or modification of state must be identifiable and cacheable
# - anything that is non-deterministic must be cached
#   - long-running steps, random actions, or information from outside parties
# - this includes actions taken by external parties (e.g. via the API)
# - events produced by the code must be restored from cache
# - events produced by outside parties must be restored in the correct sequence
# - even though I don't need to cache deletion or observation of state,
#   I have to record these actions so I know when to replay modifications to state
# Deletion or observation of state is allowed to replay
# Creation of and interaction between threads is allowed to replay
# Standard asyncio resources should be fair game for use within a workflow among its threads
# - only state/resources visible to the outside need to be handled specially
# The workflow can use asyncio resources
# - they do not need to be serialized because the history will reconstruct all state
# - provide basic wrappers to register the resource on the workflow

# Proposed replay algorithm:
# - As the code runs, match up cached calls with the next step in the history
#   - if they don't match, that's an identified error
# - After matching a step, advance the history to the next step needing matching
#   - Play all external events found
#   - the framework needs to be able to replay an external event based on the information stored
#     - the resource, the method, the arguments

class WorkflowSuspended(BaseException):
    pass


class InvalidIdentityError(BaseException):
    pass


class WorkflowFunction(Protocol):
    def __init__(self, *args, **kwargs): ...

    def __call__(self, *args, **kwargs) -> Any: ...


Status = Literal['RUNNING', 'SUSPENDED', 'COMPLETED', 'ERRORED']


class ExceptionDetails(TypedDict):
    name: str
    details: str


class StepEntry(TypedDict):
    step_id: str
    name: str
    value: Any
    args: tuple
    kwargs: dict


@dataclass
class WorkflowStatus:
    status: Status
    started: Optional[str]
    ended: Optional[str]
    steps: dict[str, StepEntry]
    result: Any | None
    error: ExceptionDetails | None

    def to_json(self):
        return {
            'status': self.status,
            'started': self.started,
            'ended': self.ended,
            'steps': self.steps,
            'result': self.result,
            'error': self.error
        }


def _step(func):
    @wraps(func)
    async def new_func(self, *args, **kwargs):
        return await self.handle_step(func.__name__, func, self, *args, **kwargs)

    return new_func


def _alambda(value):
    async def run():
        return value

    return run


def _get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


class Workflow:
    """
    Function decorator
    """

    def __init__(
            self,
            workflow_id: str,
            func: WorkflowFunction,
            event_loop: asyncio.AbstractEventLoop,
            step_manager: EventManager[StepEntry],
            unique_ids: EventManager[UniqueEvent],
    ):
        self.workflow_id = workflow_id
        self.started = _get_current_timestamp()
        self.ended = None
        self.status: Status = 'RUNNING'
        self.result = None
        self.error = None

        self._func = func
        self._event_loop = event_loop
        self._tasks = set()
        self._prefix = {}  # task name: str => prefix: list[str]
        self._reset_prefix()  # initializes with current task

        self.steps: EventManager[StepEntry] = step_manager
        self.unique_ids: EventManager[UniqueEvent] = unique_ids

    def workflow_type(self) -> str:
        """Used by serializer"""
        if (name := self._func.__class__.__name__) == 'function':
            return self._func.__name__
        else:
            return name

    def get_status(self):
        status = WorkflowStatus(
            self.status,
            self.started,
            self.ended,
            dict(self.steps.items()),
            self.result,
            self.error
        )
        logging.debug(f'STATUS: {status}')
        return status

    def _get_task_name(self):
        return asyncio.current_task(self._event_loop).get_name()

    def _get_task_callback(self):
        def cancel_on_exception(the_task):
            self._tasks.remove(the_task)
            if (ex := the_task.exception()) is not None:
                logging.error(
                    f'{self.workflow_id} CREATE_TASK: Task {the_task.get_name()} finished with exception {ex}')
                for t in self._tasks:
                    if not t.done():
                        t.cancel(f'Sibling task {the_task.get_name()} errored with {ex}')

        return cancel_on_exception

    def create_task(self, task_name: str, func: Coroutine):
        unique_name = self._get_unique_id(task_name)
        logging.debug(f'{self.workflow_id} {self._get_task_name()} CREATE TASK: {unique_name}')
        task = self._event_loop.create_task(func, name=unique_name)
        self._tasks.add(task)
        task.add_done_callback(self._get_task_callback())
        self._prefix[unique_name] = [unique_name]
        return task

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self._prefix.get(self._get_task_name(), ['external'])) + '.' + event_name

    def _get_unique_id(self, event_name: str, replay=True) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self.unique_ids:
            self.unique_ids[prefixed_name] = UniqueEvent(prefixed_name, replay=replay)
        return next(self.unique_ids[prefixed_name])

    async def handle_step(self, step_name: str, func: Callable, *args, replay=True, **kwargs):
        """This is called by the @step decorator"""
        step_id = self._get_unique_id(step_name, replay=replay)

        if step_id in self.steps:
            logging.debug(f'{self.workflow_id} HANDLE_STEP CACHE: {step_id} {step_name} {args}')
            return self.steps[step_id]['value']
        else:
            logging.debug(f'{self.workflow_id} HANDLE_STEP RUN: {step_id} {step_name} {args}')
            task_name = self._get_task_name()
            self._prefix[task_name].append(step_id)
            payload = await func(*args, **kwargs)
            self._prefix[task_name].pop(-1)
            if args and args[0] is self:
                args = args[1:]
            self.steps[step_id] = StepEntry(
                step_id=step_id,
                name=step_name,
                value=payload,
                args=args,
                kwargs=kwargs
            )
            return payload

    def _reset_prefix(self):
        self._prefix = {asyncio.current_task().get_name(): [asyncio.current_task().get_name()]}

    def _reset(self):
        self._reset_prefix()
        self._tasks = set()
        for _, ue in self.unique_ids.items():
            ue.reset()

    def start(self, *args, **kwargs) -> Task[WorkflowStatus]:
        workflow_context.set(self)
        task = self._event_loop.create_task(self._start(*args, **kwargs), name='main')
        self._tasks.add(task)
        task.add_done_callback(self._get_task_callback())
        return task

    async def _start(self, *args, **kwargs) -> WorkflowStatus:
        logging.debug(f'{self.workflow_id} {self._get_task_name()} START: {args} {kwargs}')
        self._reset()
        try:
            args = await self.handle_step(ARGUMENTS, _alambda(args))
            kwargs = await self.handle_step(KW_ARGUMENTS, _alambda(kwargs))
            result = await self._func(*args, **kwargs)
            self.ended = _get_current_timestamp()
            logging.debug(f'{self.workflow_id} {self._get_task_name()} COMPLETED')
            self.status = 'COMPLETED'
            self.result = result

        except Exception as e:
            logging.warning(f'{self.workflow_id} {self._get_task_name()} ERRORED {e}')
            logging.debug(f'{self.workflow_id} {self._get_task_name()} ERRORED {e} {traceback.format_exc()}')
            self.status = 'ERRORED'
            self.ended = _get_current_timestamp()
            self.error = {'name': str(e), 'details': traceback.format_exc()}
            raise

        return self.get_status()


class WorkflowNotFoundException(Exception):
    pass


def find_workflow() -> Workflow:
    if (workflow := workflow_context.get()) is not None:
        return workflow

    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise WorkflowNotFoundException("Workflow object not found in event stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Workflow)
    return outer_frame.f_locals.get('self')


if __name__ == '__main__':
    pass
