import asyncio
import collections
import inspect
import logging
import traceback
from asyncio import TaskGroup, Task
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import TypedDict, Any, Callable, Literal, Protocol, Iterable

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


class ResourceEntry(TypedDict):
    name: str
    identity: str | None
    type: str
    resource: Any


class ResourceLifecycleEvent(TypedDict):
    type: Literal['create_resource', 'delete_resource']
    timestamp: str
    task_id: str
    resource_id: str
    resource_type: str


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
    task_id: str  # the name of the task created/completed


EventRecord = StepStartRecord | StepEndRecord | ResourceAccessEvent | TaskEvent


def _get_type_name(obj):
    return obj.__class__.__module__ + '.' + obj.__class__.__name__


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

        elif record['type'] == 'delete_resource':
            outer_scope_ids.remove(record['resource_id'])

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

        # Keep it
        keepers[pos] = history[pos]
        pos += 1

    return [record for record in keepers if record is not None]


def _get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


# Resource names should be unique to the workflow and identity
def _create_resource_id(name: str, identity: str) -> str:
    return f'{name}|{identity}' if identity is not None else name


historian_context = ContextVar('historian')


class History(Protocol):
    def append(self, item): ...

    def __iter__(self): ...

    def __getitem__(self, item): ...

    def __len__(self): ...


class UniqueEvents(Protocol):
    def __setitem__(self, key: str, value: UniqueEvent): ...

    def __getitem__(self, item: str) -> UniqueEvent: ...

    def __contains__(self, item: UniqueEvent): ...

    def values(self) -> Iterable[UniqueEvent]: ...


class Historian:
    def __init__(self, workflow_id: str, workflow: Callable, history: History, unique_events: UniqueEvents):
        self.workflow_id = workflow_id
        self.workflow = workflow

        # These things need to be serialized
        self._history: History = history
        self._unique_ids: UniqueEvents = unique_events

        # This is set in run
        self._task_factory: TaskGroup | None = None
        self._main_task = None

        # These values are reset every time you call start_workflow
        # See _reset_replay() (called in run)
        self._open_tasks: set[Task] = set()
        self._resources = {}
        self._prefix = {'external': []}
        self._replay = {}
        self._record_replays = {}
        self._replay_pos = -1
        self._pruned = []

    def _reset_replay(self):
        logging.debug('Resetting replay')
        self._resources = {}

        self._prefix = {'external': []}

        for ue in self._unique_ids.values():
            ue.reset()

        self._pruned = _prune(self._history)

        self._replay = {'external': None}
        self._record_replays = {
            _get_id(record): asyncio.Event()
            for record in self._pruned
        }
        self._replay_pos = 0

        self._open_tasks: set[Task] = set()

    def _get_task_name(self):
        try:
            name = asyncio.current_task().get_name()
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

    class _NextRecord:
        def __init__(self, current_record, on_close: Callable):
            self.current_record = current_record
            self.on_close = on_close

        def __enter__(self):
            return self.current_record

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.on_close(self.current_record)

    async def _task_records(self, task_name):
        while self._replay_pos < len(self._pruned):
            while self._replay_pos < len(self._pruned) and \
                    (record := self._pruned[self._replay_pos])['task_id'] != task_name:

                if (event := self._record_replays[_get_id(record)]).is_set():
                    logging.debug(f'{task_name} found {record} completed')
                else:
                    logging.debug(f'{task_name} waiting on {record}')
                await event.wait()

            if self._replay_pos == len(self._pruned):
                return

            def advance(r):
                self._replay_pos += 1
                logging.debug(f'{task_name} completing {r}')
                self._record_replays[_get_id(r)].set()

            # only unset if replay_pos == len(self._pruned), which case returns
            # noinspection PyUnboundLocalVariable
            yield self._NextRecord(record, advance)

    async def _external_handler(self):
        async for next_record in self._task_records('external'):
            with next_record as record:
                await self._replay_external_event(record)

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

        if (next_record := await self._next_record()) is None:
            self._history.append(StepStartRecord(
                type='start',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                step_id=step_id
            ))

            try:
                self._prefix[self._get_task_name()].append(step_id)
                result = func(*args, **kwargs)
                if hasattr(result, '__await__'):
                    result = await result
                self._prefix[self._get_task_name()].pop(-1)

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
                        type=_get_type_name(ex),
                        args=ex.args,
                        details=traceback.format_exc()
                    )
                ))
                raise

        else:
            with next_record as record:
                logging.debug(f'{self._get_task_name()} Replaying {record}')
                # Rehydrate step from history
                assert record['step_id'] == step_id, f'{record["step_id"]} vs {step_id}'
                if record['type'] == 'end':
                    if record['exception'] is None:
                        return record['result']
                    else:
                        raise globals()[record['exception']['type']](*record['exception']['args'])
                else:
                    assert record['type'] == 'start'

    async def record_external_event(self, name, identity, action, *args, **kwargs):
        """
        When an external event occurs, this method is called.
        """
        resource_id = _create_resource_id(name, identity)

        logging.debug(f'External {action} on {resource_id} with {args} and {kwargs}')

        event_id = self._get_unique_id(resource_id + '.' + action)
        resource = self._resources[resource_id]['resource']
        function = getattr(resource, action)
        if inspect.iscoroutinefunction(function):
            result = await function(*args, **kwargs)
        else:
            result = function(*args, **kwargs)

        self._history.append(ResourceAccessEvent(
            type='external',
            timestamp=_get_current_timestamp(),
            task_id=self._get_task_name(),
            resource_id=resource_id,
            event_id=event_id,
            action=action,
            args=args,
            kwargs=kwargs,
            result=result
        ))

        return result

    async def _replay_external_event(self, record: ResourceAccessEvent):
        """
        When an external event is replayed, this method is called
        """
        logging.debug(f'{self._get_task_name()} Replaying {record}')
        assert record['type'] == 'external'

        result = getattr(
            self._resources[record['resource_id']]['resource'],
            record['action']
        )(*record['args'], **record['kwargs'])

        if inspect.iscoroutine(result):
            result = await result

        assert result == record['result']

    async def handle_internal_event(self, name, identity, action, *args, **kwargs):
        """
        Internal events are always played
        If the event is replayed, the details are asserted
        If the event is new, it is recorded
        """
        resource_id = _create_resource_id(name, identity)
        logging.debug(f'{self._get_task_name()} calling {action} on {resource_id} with {args} and {kwargs}')
        event_id = self._get_unique_id(resource_id + '.' + action)

        resource = self._resources[resource_id]['resource']
        function = getattr(resource, action)
        if inspect.iscoroutinefunction(function):
            result = await function(*args, **kwargs)
        else:
            result = function(*args, **kwargs)

        if (next_record := await self._next_record()) is None:
            self._history.append(ResourceAccessEvent(
                type='internal',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                resource_id=resource_id,
                event_id=event_id,
                action=action,
                args=args,
                kwargs=kwargs,
                result=result
            ))

        else:
            with next_record as record:
                logging.debug(f'{self._get_task_name()} Replaying {record}')
                assert 'internal' == record['type']
                assert resource_id == record['resource_id']
                assert action == record['action']
                assert args == record['args']
                assert kwargs == record['kwargs']
                assert result == record['result']

        return result

    async def register_resource(self, name, identity, resource):
        resource_id = _create_resource_id(name, identity)
        # TODO - support the ability to limit the exposed API on the resource

        if resource_id in self._resources:
            raise Exception(f'A resource for {identity} named {name} already exists in this workflow')
            # TODO - custom exception

        logging.debug(f'Creating {resource_id}')
        self._resources[resource_id] = ResourceEntry(
            name=name,
            identity=identity,
            type=_get_type_name(resource),
            resource=resource
        )

        if (next_record := await self._next_record()) is None:
            self._history.append(ResourceLifecycleEvent(
                type='create_resource',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                resource_id=resource_id,
                resource_type=_get_type_name(resource)
            ))

        else:
            with next_record as record:
                logging.debug(f'{self._get_task_name()} Replaying {record}')
                assert record['type'] == 'create_resource'
                assert record['resource_id'] == resource_id

        return resource_id

    async def delete_resource(self, name, identity):
        resource_id = _create_resource_id(name, identity)
        if resource_id not in self._resources:
            raise Exception(f'No resource for {identity} named {name} found')
            # TODO - custom exception

        logging.debug(f'Removing {resource_id}')
        resource_entry = self._resources.pop(resource_id)

        if (next_record := await self._next_record()) is None:
            self._history.append(ResourceLifecycleEvent(
                type='delete_resource',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                resource_id=resource_id,
                resource_type=resource_entry['type']
            ))

        else:
            with next_record as record:
                logging.debug(f'{self._get_task_name()} Replaying {record}')
                assert record['type'] == 'delete_resource'
                assert record['resource_id'] == resource_id

    def start_task(self, func, *args, task_name=None, **kwargs):
        historian_context.set(self)
        task_id = task_name or self._get_unique_id(func.__name__)
        logging.debug(f'{self._get_task_name()} has requested {task_id} start')

        @wraps(func)
        async def _func(*a, **kw):
            if (next_record := await self._next_record()) is None:
                logging.debug(f'Starting task {task_id}')
                self._history.append(TaskEvent(
                    type='start_task',
                    timestamp=_get_current_timestamp(),
                    task_id=task_id
                ))
            else:
                with next_record as record:
                    logging.debug(f'{self._get_task_name()} Replaying {record}')
                    assert record['type'] == 'start_task'
                    assert record['task_id'] == task_id

            result = await func(*a, **kw)

            if (next_record := await self._next_record()) is None:
                logging.debug(f'Completing task {task_id}')
                self._history.append(TaskEvent(
                    type='complete_task',
                    timestamp=_get_current_timestamp(),
                    task_id=task_id
                ))

            else:
                with next_record as record:
                    logging.debug(f'{self._get_task_name()} Replaying {record}')
                    assert record['type'] == 'complete_task'
                    assert record['task_id'] == task_id

            return result

        task = self._task_factory.create_task(
            _func(*args, **kwargs),
            name=task_id,
        )

        self._prefix[task.get_name()] = [task.get_name()]
        self._replay[task.get_name()] = self._task_records(task.get_name())
        self._open_tasks.add(task)
        task.add_done_callback(lambda t: self._open_tasks.remove(t))

        return task

    async def _run(self, *args, **kwargs):
        args = await self.handle_step('args', lambda: args)
        kwargs = await self.handle_step('kwargs', lambda: kwargs)
        result = await self.workflow(*args, **kwargs)
        return result

    async def run(self, *args, **kwargs):
        self._reset_replay()
        async with asyncio.TaskGroup() as self._task_factory:
            self._task_factory.create_task(self._external_handler(), name=f'{self.workflow_id}.external_replay')
            self._main_task = self.start_task(self._run, *args, **kwargs, task_name=self.workflow_id)
            return await self._main_task

    async def cancel(self):
        for task in self._open_tasks:
            if not task.cancelled() and not task.cancelling() and not task.done():
                logging.debug(f'Cancelling task {task.get_name()}')
                task.cancel()
        logging.debug(f'Cancelling task {self._main_task.get_name()}')
        self._main_task.cancel()

    def get_resources(self, identity):
        # Get public resources first
        resources = {
            entry['name']: {
                'type': entry['type'],
                'value': res.value() if hasattr((res := entry['resource']), 'value') else repr(res)
            } for entry in self._resources.values()
            if entry['identity'] is None
        }

        # Now add in (and override) identity-specific resources
        for entry in self._resources.values():
            if entry['identity'] == identity:
                resources[entry['name']] = {
                    'type': entry['type'],
                    'value': res.value() if hasattr((res := entry['resource']), 'value') else repr(res)
                }

        return resources


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
