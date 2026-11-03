import inspect
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, TypeVar

from .historian_helpers import _create_resource_id, _get_current_timestamp, _get_type_name
from .quest_types import ResourceAccessEvent, ResourceEntry, ResourceLifecycleEvent
from .resources import ResourceStreamManager
from .utils import quest_logger

T = TypeVar('T')


class _Wrapper:
    pass


def wrap_methods_as_historian_events(resource: T, name: str, identity: str | None, historian: Any,
                                     internal=True) -> T:
    wrapper = _Wrapper()

    historian_action = historian.handle_internal_event if internal else historian.record_external_event

    for field in dir(resource):
        if field.startswith('_'):
            continue

        if callable(method := getattr(resource, field)):
            @wraps(method)
            async def record(*args, _name=name, _identity=identity, _field=field, **kwargs):
                return await historian_action(_name, _identity, _field, *args, **kwargs)

            setattr(wrapper, field, record)

    return wrapper


@dataclass(slots=True)
class ResourceRuntimeContext:
    history: list
    replay_started: Any
    fatal_exception: Any
    get_next_record: Callable[[], Any]
    replay_complete: Callable[[], Any]
    get_task_name: Callable[[], str]
    make_unique_id: Callable[[str], str]


class ResourceRuntime:
    def __init__(self, context: ResourceRuntimeContext):
        self._context = context
        self._resources: dict[str, ResourceEntry] = {}
        self._resource_stream_manager = ResourceStreamManager()

    def reset(self):
        self._resources = {}

    async def record_external_event(self, name, identity, action, *args, **kwargs):
        resource_id = _create_resource_id(name, identity)
        step_id = self._context.make_unique_id(resource_id + '.' + action)

        quest_logger.debug(f'External event {step_id} with {args} and {kwargs}')

        resource = self._resources[resource_id]['resource']
        function = getattr(resource, action)
        if inspect.iscoroutinefunction(function):
            result = await function(*args, **kwargs)
        else:
            result = function(*args, **kwargs)

        self._context.history.append(ResourceAccessEvent(
            type='external',
            timestamp=_get_current_timestamp(),
            step_id=step_id,
            task_id=self._context.get_task_name(),
            resource_id=resource_id,
            action=action,
            args=list(args),
            kwargs=kwargs,
            result=result
        ))

        return result

    async def replay_external_event(self, record: ResourceAccessEvent):
        assert record['type'] == 'external', str(record)

        result = getattr(
            self._resources[record['resource_id']]['resource'],
            record['action']
        )(*record['args'], **record['kwargs'])

        if inspect.iscoroutine(result):
            result = await result

        assert result == record['result']

    async def handle_internal_event(self, name, identity, action, *args, **kwargs):
        resource_id = _create_resource_id(name, identity)
        step_id = self._context.make_unique_id(resource_id + '.' + action)

        resource = self._resources[resource_id]['resource']
        function = getattr(resource, action)

        if (next_record := await self._context.get_next_record()) is None:
            self._context.history.append(ResourceAccessEvent(
                type='internal_start',
                timestamp=_get_current_timestamp(),
                step_id=step_id,
                task_id=self._context.get_task_name(),
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

        if (next_record := await self._context.get_next_record()) is None:
            self._context.history.append(ResourceAccessEvent(
                type='internal_end',
                timestamp=_get_current_timestamp(),
                step_id=step_id,
                task_id=self._context.get_task_name(),
                resource_id=resource_id,
                action=action,
                args=list(args),
                kwargs=kwargs,
                result=result
            ))
            await self.update_resource_stream(identity)
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

        if resource_id in self._resources:
            raise Exception(f'A resource for {identity} named {name} already exists in this workflow')

        step_id = self._context.make_unique_id(resource_id + '.' + '__init__')
        quest_logger.debug(f'Creating {resource_id}')

        self._resources[resource_id] = ResourceEntry(
            name=name,
            identity=identity,
            type=_get_type_name(resource),
            resource=resource
        )

        if (next_record := await self._context.get_next_record()) is None:
            self._context.history.append(ResourceLifecycleEvent(
                type='create_resource',
                timestamp=_get_current_timestamp(),
                step_id=step_id,
                task_id=self._context.get_task_name(),
                resource_id=resource_id,
                resource_type=_get_type_name(resource)
            ))
            await self.update_resource_stream(identity)
        else:
            with next_record as record:
                assert record['type'] == 'create_resource'
                assert record['resource_id'] == resource_id

        return resource_id

    async def delete_resource(self, name, identity, suspending=False):
        resource_id = _create_resource_id(name, identity)
        if resource_id not in self._resources:
            raise Exception(f'No resource for {identity} named {name} found')

        step_id = self._context.make_unique_id(resource_id + '.' + '__del__')
        quest_logger.debug(f'Removing {resource_id}')
        resource_entry = self._resources.pop(resource_id)

        if not suspending:
            if (next_record := await self._context.get_next_record()) is None:
                self._context.history.append(ResourceLifecycleEvent(
                    type='delete_resource',
                    timestamp=_get_current_timestamp(),
                    step_id=step_id,
                    task_id=self._context.get_task_name(),
                    resource_id=resource_id,
                    resource_type=resource_entry['type']
                ))
                await self.update_resource_stream(identity)
            else:
                with next_record as record:
                    assert record['type'] == 'delete_resource'
                    assert record['resource_id'] == resource_id

    async def get_resources(self, identity):
        await self._context.replay_started.wait()
        await self._context.replay_complete()

        if self._context.fatal_exception.done():
            await self._context.fatal_exception

        resources: dict[(str, str), str] = {}
        for entry in self._resources.values():
            if entry['identity'] is None or entry['identity'] == identity:
                resources[(entry['name'], entry['identity'])] = entry['type']

        return resources

    def get_resource_stream(self, identity):
        return self._resource_stream_manager.get_resource_stream(
            identity,
            lambda: self.get_resources(identity),
        )

    async def update_resource_stream(self, identity):
        await self._resource_stream_manager.update(identity)

    def notify_of_workflow_stop(self):
        self._resource_stream_manager.notify_of_workflow_stop()
