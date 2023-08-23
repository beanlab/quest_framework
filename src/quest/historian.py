import asyncio
import inspect
import logging
import traceback
from asyncio import TaskGroup, Task
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import TypedDict, Any, Callable, Literal, Protocol, Reversible

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

SUSPENDED = '__WORKFLOW_SUSPENDED__'


class ExceptionDetails(TypedDict):
    type: str
    args: tuple
    details: str


class StepStartRecord(TypedDict):
    type: Literal['start']
    timestamp: str
    step_id: str
    task_id: str


class StepEndRecord(TypedDict):
    type: Literal['end']
    timestamp: str
    step_id: str
    task_id: str
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
    step_id: str
    task_id: str
    resource_id: str
    resource_type: str


class ResourceAccessEvent(TypedDict):
    type: Literal['external', 'internal']
    timestamp: str
    step_id: str
    task_id: str
    resource_id: str
    action: str
    args: tuple
    kwargs: dict
    result: Any


class TaskEvent(TypedDict):
    type: Literal['start_task', 'complete_task']
    timestamp: str
    step_id: str
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


def _prune(step_id: str, history: "History"):
    """
    Remove substep work
    Records whose step_ids are prefixed by the step_id of the step are substep work
    Keep external events that belong to resources created outside the step
    """
    items = reversed(history)
    to_delete = []

    # Last record should be a step with the give step ID
    record = next(items)
    assert record['type'] == 'end', f'{record["type"]} != end'
    assert record['step_id'] == step_id, f'{record["step_id"]} != {step_id}'

    try:
        while record := next(items):
            if record['step_id'].startswith(step_id):
                # Found a sub-record of the step, we can delete it
                to_delete.append(record)

            if record['step_id'] == step_id:
                # Found the beginning of the step, we can stop searching
                break

    except StopIteration:
        pass

    for record in to_delete:
        history.remove(record)

def _get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


# Resource names should be unique to the workflow and identity
def _create_resource_id(name: str, identity: str) -> str:
    return f'{name}|{identity}' if identity is not None else name


historian_context = ContextVar('historian')


class History(Protocol, Reversible):
    def append(self, item): ...

    def remove(self, item): ...

    def __iter__(self): ...

    def __reversed__(self): ...

    def __getitem__(self, pos): ...

    def __len__(self): ...


class Historian:
    def __init__(self, workflow_id: str, workflow: Callable, history: History):
        self.workflow_id = workflow_id
        self.workflow = workflow

        # These things need to be serialized
        self._history: History = history

        # This is set in run
        self._task_factory: TaskGroup | None = None

        # These values are reset every time you call start_workflow
        # See _reset_replay() (called in run)
        self._open_tasks: list[Task] = []
        self._resources = {}
        self._prefix = {'external': []}
        self._unique_ids: set[str] = set()
        self._existing_history = []
        self._replay = {}
        self._record_gates = {}

    def _reset_replay(self):
        logging.debug('Resetting replay')
        self._existing_history = list(self._history)
        self._resources = {}

        self._prefix = {'external': []}
        self._unique_ids = set()

        self._replay = {'external': None}
        self._record_gates = {
            _get_id(record): asyncio.Event()
            for record in self._history
        }

        self._open_tasks: list[Task] = []

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

    def _get_unique_id(self, event_name: str) -> str:
        prefixed_name_root = self._get_prefixed_name(event_name)
        prefixed_name = prefixed_name_root
        counter = 0
        while prefixed_name in self._unique_ids:
            counter += 1
            prefixed_name = f'{prefixed_name_root}_{counter}'
        self._unique_ids.add(prefixed_name)
        return prefixed_name

    class _NextRecord:
        def __init__(self, current_record, on_close: Callable):
            self.current_record = current_record
            self.on_close = on_close

        def __enter__(self):
            return self.current_record

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.on_close(self.current_record)

    async def _task_replay_records(self, task_id):
        """Yield the tasks for this task ID"""
        for record in self._existing_history:
            # If the record belongs to another task, we need to wait
            #  for that other task to finish with the record
            #  before we move on
            if record['task_id'] != task_id:
                if (event := self._record_gates[_get_id(record)]).is_set():
                    logging.debug(f'{task_id} found {record} completed')
                else:
                    logging.debug(f'{task_id} waiting on {record}')
                await event.wait()

            else:  # task ID matches
                def complete(r):
                    logging.debug(f'{task_id} completing {r}')
                    self._record_gates[_get_id(r)].set()

                # noinspection PyUnboundLocalVariable
                logging.debug(f'{self._get_task_name()} replaying {record}')
                yield self._NextRecord(record, complete)

    async def _external_handler(self):
        async for next_record in self._task_replay_records('external'):
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
        unique_func_name = step_id.split('.')[-1]

        if (next_record := await self._next_record()) is not None:
            with next_record as record:
                assert record['step_id'] == step_id, f'{record["step_id"]} != {step_id}'
                if record['type'] == 'end':
                    # Rehydrate step from history
                    assert record['type'] == 'end'
                    if record['exception'] is None:
                        return record['result']
                    else:
                        raise globals()[record['exception']['type']](*record['exception']['args'])
                else:
                    assert record['type'] == 'start'

        if next_record is None:
            logging.debug(f'{self._get_task_name()} starting step {func_name} with {args} and {kwargs}')
            self._history.append(StepStartRecord(
                type='start',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                step_id=step_id
            ))

        prune_on_exit = True
        try:
            self._prefix[self._get_task_name()].append(unique_func_name)

            result = func(*args, **kwargs)
            if hasattr(result, '__await__'):
                result = await result
            logging.debug(f'{self._get_task_name()} completing step {func_name} with {result}')

            self._history.append(StepEndRecord(
                type='end',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                step_id=step_id,
                result=result,
                exception=None
            ))

            return result

        except asyncio.CancelledError as cancel:
            if cancel.args and cancel.args[0] == SUSPENDED:
                prune_on_exit = False
                raise asyncio.CancelledError(SUSPENDED)
            else:
                logging.exception(f'{step_id} canceled')
                self._history.append(StepEndRecord(
                    type='end',
                    timestamp=_get_current_timestamp(),
                    task_id=self._get_task_name(),
                    step_id=step_id,
                    result=None,
                    exception=ExceptionDetails(
                        type=_get_type_name(cancel),
                        args=cancel.args,
                        details=traceback.format_exc()
                    )
                ))
                raise

        except Exception as ex:
            logging.exception(f'Error in {step_id}')
            self._history.append(StepEndRecord(
                type='end',
                timestamp=_get_current_timestamp(),
                step_id=step_id,
                task_id=self._get_task_name(),
                result=None,
                exception=ExceptionDetails(
                    type=_get_type_name(ex),
                    args=ex.args,
                    details=traceback.format_exc()
                )
            ))
            raise

        finally:
            if prune_on_exit:
                _prune(step_id, self._history)
            self._prefix[self._get_task_name()].pop(-1)

    async def record_external_event(self, name, identity, action, *args, **kwargs):
        """
        When an external event occurs, this method is called.
        """
        resource_id = _create_resource_id(name, identity)
        step_id = self._get_unique_id(resource_id + '.' + action)

        logging.debug(f'External event {step_id} with {args} and {kwargs}')

        resource = self._resources[resource_id]['resource']
        function = getattr(resource, action)
        if inspect.iscoroutinefunction(function):
            result = await function(*args, **kwargs)
        else:
            result = function(*args, **kwargs)

        self._history.append(ResourceAccessEvent(
            type='external',
            timestamp=_get_current_timestamp(),
            step_id=step_id,
            task_id=self._get_task_name(),
            resource_id=resource_id,
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
        step_id = self._get_unique_id(resource_id + '.' + action)
        logging.debug(f'{self._get_task_name()} calling {step_id} with {args} and {kwargs}')

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
                step_id=step_id,
                task_id=self._get_task_name(),
                resource_id=resource_id,
                action=action,
                args=args,
                kwargs=kwargs,
                result=result
            ))

        else:
            with next_record as record:
                assert 'internal' == record['type'], f'internal != {record["type"]}'
                assert resource_id == record['resource_id']
                assert action == record['action']
                assert list(args) == list(record['args'])
                assert kwargs == record['kwargs']
                assert result == record['result']

        return result

    async def register_resource(self, name, identity, resource):
        resource_id = _create_resource_id(name, identity)
        # TODO - support the ability to limit the exposed API on the resource

        if resource_id in self._resources:
            raise Exception(f'A resource for {identity} named {name} already exists in this workflow')
            # TODO - custom exception

        step_id = self._get_unique_id(resource_id + '.' + '__init__')
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
                step_id=step_id,
                task_id=self._get_task_name(),
                resource_id=resource_id,
                resource_type=_get_type_name(resource)
            ))

        else:
            with next_record as record:
                assert record['type'] == 'create_resource'
                assert record['resource_id'] == resource_id

        return resource_id

    async def delete_resource(self, name, identity, suspending=False):
        resource_id = _create_resource_id(name, identity)
        if resource_id not in self._resources:
            raise Exception(f'No resource for {identity} named {name} found')
            # TODO - custom exception

        step_id = self._get_unique_id(resource_id + '.' + '__del__')
        logging.debug(f'Removing {resource_id}')
        resource_entry = self._resources.pop(resource_id)

        if not suspending:
            if (next_record := await self._next_record()) is None:
                self._history.append(ResourceLifecycleEvent(
                    type='delete_resource',
                    timestamp=_get_current_timestamp(),
                    step_id=step_id,
                    task_id=self._get_task_name(),
                    resource_id=resource_id,
                    resource_type=resource_entry['type']
                ))

            else:
                with next_record as record:
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
                    step_id=task_id + '.start',
                    task_id=task_id
                ))
            else:
                with next_record as record:
                    assert record['type'] == 'start_task'
                    assert record['task_id'] == task_id

            result = await func(*a, **kw)

            if (next_record := await self._next_record()) is None:
                logging.debug(f'Completing task {task_id}')
                self._history.append(TaskEvent(
                    type='complete_task',
                    timestamp=_get_current_timestamp(),
                    step_id=task_id + '.complete',
                    task_id=task_id
                ))

            else:
                with next_record as record:
                    assert record['type'] == 'complete_task'
                    assert record['task_id'] == task_id

            return result

        task = self._task_factory.create_task(
            _func(*args, **kwargs),
            name=task_id,
        )

        self._prefix[task.get_name()] = [task.get_name()]
        self._replay[task.get_name()] = self._task_replay_records(task.get_name())
        self._open_tasks.append(task)
        task.add_done_callback(lambda t: self._open_tasks.remove(t) if t in self._open_tasks else None)

        return task

    async def _run(self, *args, **kwargs):
        args = await self.handle_step('args', lambda: args)
        kwargs = await self.handle_step('kwargs', lambda: kwargs)
        result = await self.handle_step(self.workflow.__name__, self.workflow, *args, **kwargs)
        return result

    async def run(self, *args, **kwargs):
        logging.debug(f'Running workflow {self.workflow_id}')
        self._reset_replay()
        async with asyncio.TaskGroup() as self._task_factory:
            self._task_factory.create_task(self._external_handler(), name=f'{self.workflow_id}.external_replay')
            return await self.start_task(self._run, *args, **kwargs, task_name=self.workflow_id)

    async def suspend(self):
        # Cancelling these in reverse order is critical
        # If a parent thread cancels, it will cancel a child.
        # We want to be the one that cancels every task,
        #  so we cancel the children before the parents.
        for task in list(reversed(self._open_tasks)):
            if not task.done() or task.cancelled() or task.cancelling():
                logging.debug(f'Suspending task {task.get_name()}')
                task.cancel(SUSPENDED)
        for task in list(self._open_tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass

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
