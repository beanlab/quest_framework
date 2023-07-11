import asyncio
import inspect
import traceback
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import TypedDict, Any, Callable, Literal

from src.quest.events import UniqueEvent


# external replay:
# - there are possibly multiple threads interacting with the same resource
# - if a resource is published, external parties can interact with it
# - I need to make sure the external event is replayed at the right time
#   - i.e. when the appropriate thread is ready to observe the result
#
#  E   A   B
#  2   .   .
#  .   3   .
#  .   . ->2
#  .   . ->3
#
# Guidelines
# - external events do not have a workflow thread
# - the sequence of external events is on the resource, not on the thread
# - so a given thread can't interact with a resource until the events from other threads have processed
#
# Strategy
# - break up the history into resource sequences
# - advance each resource queue as threads interact
# - when an external event is next in the sequence, replay it

# History pruning
# - cutting out nested steps
# - but how do we know which external events "belonged" to a given step
# - several external events may come in for one task while the other performs a step
# - can an external event be associated with a task?
# - the external event IS associated with a resource
# - all resources are created in a task; are all associations with a resource unique to that task?
#  - they ARE associated with that task an all sub-tasks.
#  - but that means that a resource can be created in the top task and passed to two sub-tasks
#  - then one task can interact with the resource and external events while the other performs sub-steps
# - so...we'd have to know which later task was affected by the external event
#
#  E    A
#  .    S
#  1    |    # We don't want to miss replaying this event, even though it falls between the step records
#  .    S
#  .  ->1
#
#  E    A
#  .    S
#  1    |    # This event is safe to skip because the event resource is scoped to the step
#  .  ->1
#  .    S
#
# If a resource was created and destroyed within a step, all events associated with that resource can be skipped.
# If a resource was created outside a step, actions on that resource should be preserved.
# If the author puts "side effect" work on a resource inside a step, that's a problem.
# Principle: all resources, associated events, and substeps that happen in a step will be entirely skipped.
#            Never have side effects that are not entirely scoped to the step.
#
# To prune correctly, I need to turn process the sequence like a tree
# Each task and step is a separate branch
# I need to look for resources that are open in each branch and match the relevant events

class ExceptionDetails(TypedDict):
    type: str
    args: tuple
    details: str


class StepStartRecord(TypedDict):
    type: Literal['start']
    timestamp: str
    task_id: str
    step_id: str


class StepEndRecord(TypedDict):
    type: Literal['end']
    timestamp: str
    task_id: str
    step_id: str
    result: Any
    exception: Any | None


class ResourceAccessEvent(TypedDict):
    type: Literal['external', 'internal']
    timestamp: str
    task_id: str
    resource_id: str
    event_id: str
    action: str
    args: tuple
    kwargs: dict
    result: Any


class TaskEvent(TypedDict):
    type: Literal['start_task', 'complete_task']
    timestamp: str
    parent_task: str
    task_id: str  # the name of the task created/completed


EventRecord = StepStartRecord | StepEndRecord | ResourceAccessEvent | TaskEvent


def _get_id(item):
    if isinstance(item, dict):
        return tuple((k, _get_id(v)) for k, v in item.items())

    if isinstance(item, list):
        return tuple(_get_id(v) for v in item)

    return item


def _prune(history) -> list[EventRecord]:
    """
    Remove substep work
    Keep external events that belong to resources created outside the step
    """
    keepers: list[EventRecord | None] = [None for _ in history]
    pos = 0
    outer_scope_ids = set()
    while pos < len(history):
        record = history[pos]

        if record['type'] == 'create_resource':
            outer_scope_ids.add(record['resource_id'])
            pos += 1
            continue

        elif record['type'] == 'remove_resource':
            outer_scope_ids.remove(record['resource_id'])
            pos += 1
            continue

        elif record['type'] == 'start':
            # Find the associated 'end' record and cut out the rest
            end_pos = pos + 1
            while end_pos < len(history) and history[end_pos].get('step_id', None) != record['step_id']:
                if history[end_pos]['type'] == 'external' and \
                        history[end_pos]['resource_id'] in outer_scope_ids:
                    keepers[end_pos] = history[end_pos]
                end_pos += 1

            if end_pos == len(history):
                # no end found, so this step isn't finished
                keepers[pos] = history[pos]
                pos += 1
                continue

            else:
                # found the end, keep it and move on
                keepers[end_pos] = history[end_pos]
                pos = end_pos + 1
                continue
        else:
            # Keep it
            keepers[pos] = history[pos]
            pos += 1
            continue

    return [record for record in keepers if record is not None]


def _get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


historian_context = ContextVar('historian')


class Historian:
    def __init__(self, workflow_id: str, workflow: Callable, history):
        self.workflow_id = workflow_id
        self.workflow = workflow

        self._history: list[EventRecord] = history
        self._pruned = _prune(history)
        self._resources = {}
        self._unique_ids = {}
        self._replay = {}
        self._record_replays = {}
        self._replay_pos = -1
        self._prefix = {'external': []}
        self._event_loop = asyncio.get_event_loop()

    def _get_task_name(self):
        try:
            name = asyncio.current_task(self._event_loop).get_name()
            if name in self._replay:
                return name
            else:
                return 'external'
        except RuntimeError:
            return 'external'

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self._prefix[self._get_task_name()]) + '.' + event_name

    def _get_unique_id(self, event_name: str, replay=True) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self._unique_ids:
            self._unique_ids[prefixed_name] = UniqueEvent(prefixed_name, replay=replay)
        return next(self._unique_ids[prefixed_name])

    def _reset_replay(self):
        self._record_replays = {
            _get_id(record): asyncio.Event()
            for record in self._pruned
        }
        self._replay_pos = 0

    async def _task_records(self, task_name):
        while self._replay_pos < len(self._pruned):
            while (record := self._pruned[self._replay_pos])['task_id'] != task_name:
                await self._record_replays[_get_id(record)].wait()

            yield record

            # TODO - ensure that every task that calls _task_records is sure to reach the StopIteration
            self._replay_pos += 1
            self._record_replays[_get_id(record)].set()

    async def _external_handler(self):
        async for record in self._task_records('external'):
            self._replay_external_event(record)

    async def _next_record(self):
        if self._replay[self._get_task_name()] is None:
            return None

        try:
            return await self._replay[self._get_task_name()].asend(None)

        except StopAsyncIteration:
            self._replay[self._get_task_name()] = None
            return None

    async def handle_step(self, func_name, func: Callable, *args, **kwargs):
        step_id = self._get_unique_id(func_name)

        if (record := await self._next_record()) is None:
            self._history.append(StepStartRecord(
                type='start',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                step_id=step_id
            ))

            try:
                result = func(*args, **kwargs)
                if hasattr(result, '__await__'):
                    result = await result

                self._history.append(StepEndRecord(
                    type='end',
                    timestamp=_get_current_timestamp(),
                    task_id=self._get_task_name(),
                    step_id=step_id,
                    result=result,
                    exception=None
                ))
                return result

            except Exception as ex:
                self._history.append(StepEndRecord(
                    type='end',
                    timestamp=_get_current_timestamp(),
                    task_id=self._get_task_name(),
                    step_id=step_id,
                    result=None,
                    exception=ExceptionDetails(
                        type=str(type(ex)),
                        args=ex.args,
                        details=traceback.format_exc()
                    )
                ))
                raise

        else:
            # Rehydrate step from history
            assert record['type'] == 'end'
            assert record['step_id'] == step_id
            if record['exception'] is None:
                return record['result']
            else:
                raise globals()[record['exception']['type']](*record['exception']['args'])

    def _record_external_event(self, resource_id, action, *args, **kwargs):
        """
        When an external event occurs, this method is called.
        """
        resource = self._resources[resource_id]
        result = getattr(resource, action)(*args, **kwargs)
        self._history.append(ResourceAccessEvent(
            type='external',
            timestamp=_get_current_timestamp(),
            task_id=self._get_task_name(),
            resource_id=resource_id,
            action=action,
            args=args,
            kwargs=kwargs,
            result=result
        ))

    def _replay_external_event(self, record: ResourceAccessEvent):
        """
        When an external event is replayed, this method is called
        """
        getattr(self._resources[record['resource_id']], record['action'])(*record['args'], **record['kwargs'])

    async def _handle_internal_event(self, resource_id, action, *args, **kwargs):
        """
        Internal events are always played
        If the event is replayed, the details are asserted
        If the event is new, it is recorded
        """
        resource = self._resources[resource_id]
        result = getattr(resource, action)(*args, **kwargs)

        if (record := await self._next_record()) is None:
            self._history.append(ResourceAccessEvent(
                type='internal',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                resource_id=resource_id,
                action=action,
                args=args,
                kwargs=kwargs,
                result=result
            ))

        else:
            assert 'internal' == record['type']
            assert resource_id == record['event_id']
            assert action == record['action']
            assert args == record['args']
            assert kwargs == record['kwargs']
            assert result == record['result']

    def start_task(self, func, *args, task_name=None, **kwargs):
        historian_context.set(self)
        parent = self._get_task_name()
        task_id = task_name or self._get_unique_id(func.__name__)

        @wraps(func)
        async def _func(*a, **kw):
            record = await self._next_record()
            if record is not None:
                assert record['type'] == 'start_task'
                assert record['parent_task'] == parent
                assert record['task_id'] == task_id
            else:
                self._history.append(TaskEvent(
                    type='start_task',
                    timestamp=_get_current_timestamp(),
                    parent_task=parent,
                    task_id=task_id
                ))

            result = await func(*a, **kw)

            record = await self._next_record()
            if record is not None:
                assert record['type'] == 'complete_task'
                assert record['parent_task'] == parent
                assert record['task_id'] == task_id
            else:
                self._history.append(TaskEvent(
                    type='complete_task',
                    timestamp=_get_current_timestamp(),
                    parent_task=parent,
                    task_id=task_id
                ))
            return result

        task = self._event_loop.create_task(
            _func(*args, **kwargs),
            name=task_id,
        )

        self._prefix[task.get_name()] = []
        self._replay[task.get_name()] = self._task_records(task.get_name())
        return task

    async def _start(self, *args, **kwargs):
        args = await self.handle_step('args', lambda: args)
        kwargs = await self.handle_step('kwargs', lambda: kwargs)
        result = await self.workflow(*args, **kwargs)
        return result

    def start_workflow(self, *args, **kwargs):
        self._reset_replay()
        main_task = self.start_task(self._start, *args, **kwargs, task_name='main')
        return main_task


class HistorianNotFoundException(Exception):
    pass


def find_historian() -> Historian:
    if (workflow := historian_context.get()) is not None:
        return workflow

    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise HistorianNotFoundException("Historian object not found in event stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Historian)
    return outer_frame.f_locals.get('self')
