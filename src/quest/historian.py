import asyncio
import inspect
import traceback
from asyncio import Task
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import Callable, TypeVar

from .history import History
from .quest_types import ConfigurationRecord, VersionRecord, StepStartRecord, StepEndRecord, \
    ResourceAccessEvent, ResourceEntry, ResourceLifecycleEvent, TaskEvent
from .resources import ResourceStreamManager
from .serializer import StepSerializer
from .utils import quest_logger, task_name_getter
from .utils import (
    serialize_exception,
    deserialize_exception
)

QUEST_VERSIONS = "_quest_versions"
GLOBAL_VERSION = "_global_version"

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


def suspendable(func):
    """
    Makes a __aexit__ or __exit__ method suspendable
    With this decorator, the exit method will not be called
      when the workflow is suspending.
    It will only be called when the with context exits for other reasons.
    """
    if inspect.iscoroutinefunction(func):
        @wraps(func)
        async def new_func(self, exc_type, exc_val, exc_tb):
            if exc_type is asyncio.CancelledError and exc_val.args[0] == SUSPENDED:
                return
            await func(self, exc_type, exc_val, exc_tb)
    else:
        @wraps(func)
        def new_func(self, exc_type, exc_val, exc_tb):
            if exc_type is asyncio.CancelledError and exc_val.args[0] == SUSPENDED:
                return
            func(self, exc_type, exc_val, exc_tb)

    return new_func


T = TypeVar('T')


class _Wrapper:
    pass


def wrap_methods_as_historian_events(resource: T, name: str, identity: str | None, historian: 'Historian',
                                     internal=True) -> T:
    wrapper = _Wrapper()

    historian_action = historian.handle_internal_event if internal else historian.record_external_event

    for field in dir(resource):
        if field.startswith('_'):
            continue

        if callable(method := getattr(resource, field)):
            # Use default-value kwargs to force value binding instead of late binding
            @wraps(method)
            async def record(*args, _name=name, _identity=identity, _field=field, **kwargs):
                return await historian_action(_name, _identity, _field, *args, **kwargs)

            setattr(wrapper, field, record)

    return wrapper


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

    end_of_life_resources = set()
    try:
        while record := next(items):
            if record['step_id'].startswith(step_id):
                # Found a sub-record of the step, we can delete it
                # But if it is a resource scoped to this step, keep track of it first
                if record['type'] == 'delete_resource':
                    end_of_life_resources.add(record['resource_id'])
                to_delete.append(record)

            if record['type'] == 'external' and record['resource_id'] in end_of_life_resources:
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


def _get_qualified_version(module_name, function_name, version_name: str) -> str:
    """
    A version is defined by the module and name of the function that is versioned.
    If you move a function to a new module (or rename the module), it has become a new function.
    If you change the function you are calling in a replay (i.e. its name has changed),
    you may not get the expected results.

    You've been warned.
    """
    return '.'.join([module_name, function_name, version_name])


# Resource names should be unique to the workflow and identity
def _create_resource_id(name: str, identity: str | None) -> str:
    return f'{name}|{identity}' if identity is not None else name


historian_context = ContextVar('historian')


def get_function_name(func):
    if hasattr(func, '__name__'):  # regular functions
        return func.__name__
    else:  # Callable classes
        return func.__class__.__name__


def _get_exception_class(exception_type: str):
    module_name, class_name = exception_type.rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    exception_class = getattr(module, class_name)
    return exception_class


class Historian:
    def __init__(self, workflow_id: str, workflow: Callable, history: History, serializer: StepSerializer):
        # TODO - change nomenclature (away from workflow)? Maybe just use workflow.__name__?
        self.workflow_id = workflow_id
        self.workflow = workflow
        self._configurations: list[tuple[Callable, list, dict]] = []

        # This indicates whether the workflow function has completed
        # Suspending the workflow does not affect this value
        self._workflow_completed = False

        # These things need to be serialized
        self._history: History = history

        self._serializer: StepSerializer = serializer

        # The remaining properties defined in __init__
        # are reset every time you call start_workflow
        # See _reset_replay() (called in _run)

        # If the application fails, this future will know.
        # See also get_resources() and _run_with_exception_handling()
        self._fatal_exception = asyncio.Future()

        # Keep track of configuration position during the replay
        self._configuration_pos = 0

        # Keep track of the versions of the workflow function
        self._versions = {}

        # Keep track of the discovered, unprocessed versions
        # While the code runs, it finds functions that are versioned
        # However, we only process the versions during live play
        #  (not replay), so we need to save these until the replay is complete
        # Then we add events to the history to record the new versions.
        # As the workflow proceeds, it will now use the latest versions observed.
        self._discovered_versions = {}

        # These are the resources available to the outside world.
        #  This could include values that can be accessed,
        #  queues to push to, etc.
        # See also external.py
        self._resources: dict[str, ResourceEntry] = {}

        # This is the resource stream manager that handles calls to stream the historian's resources
        self._resource_stream_manager = ResourceStreamManager()

        # We keep track of all open tasks so we can properly suspend them
        self._open_tasks: list[Task] = []

        # The prefix tracks the call stack for each task
        #  so we can create accurate, informative step and task IDs
        self._prefix = {}

        # When a function is called twice in the same stack frame,
        #  the second call would have the same ID as the first.
        #  Unique IDs keeps track of existing IDs so we can ensure
        #  each new ID is unique. See _get_unique_id
        self._unique_ids: set[str] = set()

        # Replay Started ensures that no one tries to access resources
        #  until the resources have been rebuilt after resuming
        # It makes sure the workflow has actually started running
        # Then we wait until the replay has completed (see _replay_complete)
        self._replay_started = asyncio.Event()

        # The existing history is a copy of the initial history
        #  This ensures that we only replay the pre-existing records
        self._existing_history = []

        # Each task needs to replay its separate event records,
        #  but the records are all interleaved.
        #  We use _task_replay_records to create a stream of records
        #  that belong to a specific task.
        # _replay_records stores the generators for each task ID
        # See also _next_record
        self._replay_records = {}

        # To ensure that past events are replayed in the exact order
        #  they were created, a record gate exists for each record.
        #  When a task finishes with a record, it opens the gate
        #  which allows every task to move to the next record
        # If something goes wrong while processing a record,
        #  the error is put here, leading to the replay failing
        self._record_gates: dict[str, asyncio.Future] = {}

        # noinspection PyTypeChecker
        self._last_record_gate: asyncio.Future = None

    def _reset_replay(self):
        quest_logger.debug('Resetting replay')

        self._configuration_pos = 0

        self._versions = {}

        self._existing_history = list(self._history)
        self._resources = {}

        # The workflow ID is used as the task name for the root task
        self._prefix = {
            self.workflow_id: [],
            self._get_external_task_name(): [],
        }

        self._unique_ids = set()

        self._task_replays = {}

        # We add the workflow ID and the external task name
        #  so that the root task (see run()) and the external event handler
        #  are both recognized as "belonging" to this historian
        #  See also _get_task_name()
        self._replay_records = {
            self.workflow_id: None,
            self._get_external_task_name(): None
        }

        self._record_gates = {
            _get_id(record): asyncio.Future()
            for record in self._existing_history
        }

        for self._last_record_gate in self._record_gates.values():
            pass  # i.e. when this loop finishes, _last_record_gate will be the last one

        self._open_tasks: list[Task] = []

        self._replay_started.set()

    async def _replay_complete(self):
        if self._last_record_gate is not None:
            await self._last_record_gate

        quest_logger.debug(f'{self.workflow_id} -- Replay Complete --')
        # TODO - log this only once?

        self._process_discovered_versions()

    def _get_external_task_name(self):
        return f'{self.workflow_id}.external'

    def _get_task_name(self):
        try:
            name = asyncio.current_task().get_name()
            if name in self._replay_records:
                return name
            else:
                return self._get_external_task_name()
        except RuntimeError:
            # TODO - is this catch necessary? It smells REALLY bad.
            return self._get_external_task_name()

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
            self.on_close(self.current_record, exc_type, exc_val, exc_tb)

    async def _task_replay_records(self, task_id):
        task_replay = asyncio.Event()
        self._task_replays[task_id] = task_replay

        """Yield the tasks for this task ID"""
        for record in self._existing_history:
            # If the record belongs to another task, we need to wait
            #  for that other task to finish with the record
            #  before we move on
            if record['task_id'] != task_id:
                if (gate := self._record_gates[_get_id(record)]).done():
                    if gate.exception() is not None:
                        quest_logger.debug(f'{task_id} found {record} errored: {gate.exception()}')
                    else:
                        quest_logger.debug(f'{task_id} found {record} completed')
                else:
                    quest_logger.debug(f'{task_id} waiting on {record}')
                # We await either way so if the gate has an error we see it
                await gate

            else:  # task ID matches
                def complete(r, exc_type, exc_val, exc_tb):
                    if exc_type is not None:
                        exc_info = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
                        quest_logger.debug(f'Noting that record {r} raised: \n{exc_info}')
                    # Note:
                    # While Futures, the record gates are only used as gates
                    # The return values are never used
                    # Thus, even if there was an error when the task completed
                    # we simply want to indicate the gate is finished
                    # The relevant error will be raised in handle_step
                    quest_logger.debug(f'{task_id} completing {r}')
                    self._record_gates[_get_id(r)].set_result(None)

                # noinspection PyUnboundLocalVariable
                quest_logger.debug(f'{self._get_task_name()} replaying {record}')
                yield self._NextRecord(record, complete)

        quest_logger.debug(f'Replay for {self._get_task_name()} complete')
        task_replay.set()
        await self._replay_complete()

    async def _external_handler(self):
        try:
            quest_logger.debug(f'External event handler {self._get_task_name()} starting')
            async for next_record in self._task_replay_records(self._get_external_task_name()):
                with next_record as record:
                    if record['type'] == 'external':
                        await self._replay_external_event(record)

                    elif record['type'] == 'set_version':
                        self._replay_version(record)

                    elif record['type'] == 'configuration':
                        await self._run_configuration(record)

            quest_logger.debug(f'External event handler {self._get_task_name()} completed')
        except Exception:
            quest_logger.exception('Error in _external_handler')
            raise

    async def _next_record(self):
        if self._replay_records[self._get_task_name()] is None:
            return None

        try:
            return await self._replay_records[self._get_task_name()].asend(None)

        except StopAsyncIteration:
            self._replay_records[self._get_task_name()] = None
            return None

    async def _run_configuration(self, config_record: ConfigurationRecord):
        config_function, args, kwargs = self._configurations[self._configuration_pos]
        quest_logger.debug(f'Running configuration: {get_function_name(config_function)}(*{args}, **{kwargs})')

        assert config_record['function_name'] == get_function_name(config_function), str(config_record)
        assert config_record['args'] == args, str(config_record)
        assert config_record['kwargs'] == kwargs, str(config_record)

        await config_function(*args, **kwargs)
        self._configuration_pos += 1

    def get_version(self, module_name, function_name, version_name=GLOBAL_VERSION):
        version = self._versions.get(_get_qualified_version(module_name, function_name, version_name), None)
        quest_logger.debug(
            f'{self._get_task_name()} get_version({module_name}, {function_name}, {version_name} returned "{version}"')
        return version

    def _discover_versions(self, function, versions: dict[str, str]):
        self._discovered_versions.update({
            _get_qualified_version(function.__module__, function.__qualname__, version_name): version
            for version_name, version in versions.items()
        })

        # If the replay has already finished...
        if self._last_record_gate is not None and self._last_record_gate.done():
            self._process_discovered_versions()

    def _process_discovered_versions(self):
        for version_name, version in self._discovered_versions.items():
            # These records are replayed by the external handler
            self._record_version_event(version_name, version)
        self._discovered_versions = {}

    def _record_version_event(self, version_name, version):
        if self._versions.get(version_name, None) == version:
            return  # Version not changed

        quest_logger.debug(f'Version record: {version_name} = {version}')
        self._versions[version_name] = version

        self._history.append(VersionRecord(
            type='set_version',
            timestamp=_get_current_timestamp(),
            step_id=version_name,
            task_id=self._get_external_task_name(),  # the external task owns these
            version=version
        ))

    def _replay_version(self, record: VersionRecord):
        quest_logger.debug(f'{self._get_task_name()} setting version {record["step_id"]} = "{record["version"]}"')
        self._versions[record['step_id']] = record['version']

    # TODO - keep or discard?
    async def _after_version(self, module_name, func_name, version_name, version):
        version_name = _get_qualified_version(module_name, func_name, version_name)
        quest_logger.debug(f'{self._get_task_name()} is waiting for version {version_name}=={version}')

        found = False
        for record in self._existing_history:
            if record['type'] == 'version' \
                    and record['version_name'] == version_name \
                    and record['version'] == version:
                found = True
                await self._record_gates[_get_id(record)]

        if not found:
            quest_logger.error(f'{self._get_task_name()} did not find version {version_name}=={version}')
            raise Exception(f'{self._get_task_name()} did not find version {version_name}=={version}')

        if (next_record := await self._next_record()) is not None:
            with next_record as record:
                assert record['type'] == 'after_version', str(record)
                assert record['version_name'] == version_name, str(record)
                assert record['version'] == version, str(record)

        else:
            self._history.append(VersionRecord(
                type='after_version',
                timestamp=_get_current_timestamp(),
                step_id='version',
                task_id=self._get_task_name(),
                version=version
            ))

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
                        deserialized_result = await self._serializer.deserialize(record['result'])
                        return deserialized_result
                    else:
                        exception_data = record['exception']
                        ex = deserialize_exception(exception_data)
                        raise ex
                else:
                    assert record['type'] == 'start'

        if next_record is None:
            quest_logger.debug(f'{self._get_task_name()} starting step {func_name} with {args} and {kwargs}')
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
            quest_logger.debug(f'{self._get_task_name()} completing step {func_name} with {result}')

            serialized_result = await self._serializer.serialize(result)

            self._history.append(StepEndRecord(
                type='end',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                step_id=step_id,
                result=serialized_result,
                exception=None
            ))

            return result

        except asyncio.CancelledError as cancel:
            if cancel.args and cancel.args[0] == SUSPENDED:
                prune_on_exit = False
                raise asyncio.CancelledError(SUSPENDED) from cancel
            else:
                quest_logger.exception(f'{step_id} canceled')
                serialized_exception = serialize_exception(cancel)
                self._history.append(StepEndRecord(
                    type='end',
                    timestamp=_get_current_timestamp(),
                    task_id=self._get_task_name(),
                    step_id=step_id,
                    result=None,
                    exception=serialized_exception
                ))
                raise

        except Exception as ex:
            quest_logger.exception(f'Error in {step_id}')
            serialized_exception = serialize_exception(ex)
            self._history.append(StepEndRecord(
                type='end',
                timestamp=_get_current_timestamp(),
                step_id=step_id,
                task_id=self._get_task_name(),
                result=None,
                exception=serialized_exception
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

        quest_logger.debug(f'External event {step_id} with {args} and {kwargs}')

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
            args=list(args),
            kwargs=kwargs,
            result=result
        ))

        return result

    async def _replay_external_event(self, record: ResourceAccessEvent):
        """
        When an external event is replayed, this method is called
        """
        assert record['type'] == 'external', str(record)

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

        resource = self._resources[resource_id]['resource']
        function = getattr(resource, action)

        if (next_record := await self._next_record()) is None:
            self._history.append(ResourceAccessEvent(
                type='internal_start',
                timestamp=_get_current_timestamp(),
                step_id=step_id,
                task_id=self._get_task_name(),
                resource_id=resource_id,
                action=action,
                args=list(args),
                kwargs=kwargs,
                result=None
            ))
        else:
            with next_record as record:
                assert 'internal_start' == record['type'], str(record)
                assert resource_id == record['resource_id'], str(record)
                assert action == record['action'], str(record)
                assert list(args) == list(record['args']), str(record)
                assert kwargs == record['kwargs'], str(record)

        quest_logger.debug(f'Calling {step_id} with {args} and {kwargs}')
        if inspect.iscoroutinefunction(function):
            result = await function(*args, **kwargs)
        else:
            result = function(*args, **kwargs)

        if (next_record := await self._next_record()) is None:
            self._history.append(ResourceAccessEvent(
                type='internal_end',
                timestamp=_get_current_timestamp(),
                step_id=step_id,
                task_id=self._get_task_name(),
                resource_id=resource_id,
                action=action,
                args=list(args),
                kwargs=kwargs,
                result=result
            ))
            await self._update_resource_stream(identity)

        else:
            with next_record as record:
                assert 'internal_end' == record['type'], f'internal != {record["type"]}'
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
        quest_logger.debug(f'Creating {resource_id}')

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
            await self._update_resource_stream(identity)

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
        quest_logger.debug(f'Removing {resource_id}')
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
                await self._update_resource_stream(identity)

            else:
                with next_record as record:
                    assert record['type'] == 'delete_resource'
                    assert record['resource_id'] == resource_id

    def start_task(self, func, *args, name=None, task_factory=asyncio.create_task, **kwargs):
        historian_context.set(self)
        task_id = name or self._get_unique_id(get_function_name(func))
        quest_logger.debug(f'Requested {task_id} start')

        @wraps(func)
        async def _func(*a, **kw):
            quest_logger.debug(f'Starting task {task_id}')

            if (next_record := await self._next_record()) is None:
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

            quest_logger.debug(f'Completing task {task_id}')

            return result

        task = task_factory(
            _func(*args, **kwargs),
            name=task_id
        )

        self._prefix[task.get_name()] = [task.get_name()]
        self._replay_records[task.get_name()] = self._task_replay_records(task.get_name())
        self._open_tasks.append(task)
        task.add_done_callback(lambda t: self._open_tasks.remove(t) if t in self._open_tasks else None)

        return task

    async def _run_with_args(self, *args, **kwargs):
        args = await self.handle_step('args', lambda: args)
        kwargs = await self.handle_step('kwargs', lambda: kwargs)
        result = await self.handle_step(get_function_name(self.workflow), self.workflow, *args, **kwargs)
        self._workflow_completed = True
        self._resource_stream_manager.notify_of_workflow_stop()
        return result

    async def _run_with_exception_handling(self, *args, **kwargs):
        try:
            task = await self._run_with_args(*args, **kwargs)
            return task
        except Exception as ex:
            self._fatal_exception.set_exception(ex)
            raise

    async def _run(self, *args, **kwargs):
        historian_context.set(self)
        task_name_getter.set(self._get_task_name)
        quest_logger.debug(f'Running workflow {self.workflow_id}')
        self._add_new_configurations()
        self._reset_replay()

        # We use a TaskGroup here to ensure that the external_replay task
        #  exits when the main task fails
        try:
            async with asyncio.TaskGroup() as _task_factory:
                external_task = _task_factory.create_task(
                    self._external_handler(),
                    name=self._get_external_task_name()
                )
                self._open_tasks.append(external_task)
                external_task.add_done_callback(lambda t: self._open_tasks.remove(t) if t in self._open_tasks else None)

                return await self.start_task(
                    self._run_with_exception_handling,
                    *args, **kwargs,
                    name=f'{self.workflow_id}.main',
                    task_factory=_task_factory.create_task
                )

        except ExceptionGroup as eg:
            if len(eg.exceptions) == 1:
                raise eg.exceptions[0]
            else:
                raise

        # Workflow logic completed

    def _clear_history(self, task):
        if self._workflow_completed:
            self._history.clear()

    def run(self, *args, **kwargs):
        self._replay_started.clear()
        task = asyncio.create_task(self._run(*args, **kwargs), name=self.workflow_id)
        task.add_done_callback(self._clear_history)
        return task

    def configure(self, config_function, *args, **kwargs):
        """
        Configuration happens when the application is run, before the run function is called.
        Here we inject a configuration event into the history,
        which is processed by the external task
        """
        if not callable(config_function):
            raise Exception(f'First argument to configure must be a callable. Received {config_function}.')

        self._configurations.append((config_function, list(args), kwargs))

    def _add_new_configurations(self):
        config_records = [
            record
            for record in self._history
            if record['type'] == 'configuration'
        ]

        # We should have a configuration to replay for each record in the past
        assert len(config_records) <= len(self._configurations)

        for record, (config_function, args, kwargs) in zip(config_records, self._configurations):
            assert record['function_name'] == get_function_name(config_function)
            assert record['args'] == args
            assert record['kwargs'] == kwargs

        # Add new configuration records
        for config_function, args, kwargs in self._configurations[len(config_records):]:
            quest_logger.debug(f'Adding new configuration: {get_function_name(config_function)}(*{args}, **{kwargs}')

            self._history.append(ConfigurationRecord(
                type='configuration',
                timestamp=_get_current_timestamp(),
                step_id='configuration',
                task_id=self._get_external_task_name(),  # the external task owns these
                function_name=get_function_name(config_function),
                args=args,
                kwargs=kwargs
            ))

    def signal_suspend(self):
        quest_logger.debug(f'-- Suspending {self.workflow_id} --')

        self._resource_stream_manager.notify_of_workflow_stop()

        # Cancelling these in reverse order is important
        # If a parent thread cancels, it will cancel a child.
        # We want to be the one that cancels every task,
        #  so we cancel the children before the parents.
        for task in list(reversed(self._open_tasks)):
            if not task.done() or task.cancelled() or task.cancelling():
                quest_logger.debug(f'Suspending task {task.get_name()}')
                task.cancel(SUSPENDED)

    async def suspend(self):
        # Once each task has been marked for cancellation
        #  we await each task in order to allow the cancellation
        #  to play out to completion.
        self.signal_suspend()

        for task in list(self._open_tasks):
            try:
                await task
            except asyncio.CancelledError:
                quest_logger.debug(f'Task {task.get_name()} was cancelled')
                pass

    async def get_resources(self, identity):
        # Wait until the replay is done.
        # This ensures that all pre-existing resources have been rebuilt.
        await self._replay_started.wait()
        await self._replay_complete()

        # If the application has failed, let the caller know
        if self._fatal_exception.done():
            await self._fatal_exception

        resources: dict[(str, str), str] = {}  # dict[(name, identity), type]
        for entry in self._resources.values():
            # Always return public resources and private resources for the specified identity
            if entry['identity'] is None or entry['identity'] == identity:
                resources[(entry['name'], entry['identity'])] = entry['type']

        return resources

    def get_resource_stream(self, identity):
        return self._resource_stream_manager.get_resource_stream(
            identity,
            lambda: self.get_resources(identity),
        )

    async def _update_resource_stream(self, identity):
        await self._resource_stream_manager.update(identity)


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
