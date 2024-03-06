import asyncio

historian = None
identity = 'me'

async for resources in historian.stream_resources(identity):
    print(resources)


class Historian:
    def __init__(self):
        # TODO - make this bullet proof: allow the same identity to have multiple streams
        self.resource_queues: dict[str, asyncio.Queue] = {}
        self._resources = {}

    async def stream_resources(self, identity):
        """
        The caller of this function lives outside the step management of the historian
        -> don't replay, just yield event changes in realtime
        """
        while True:
            await self.resource_queues[identity].get()
            return get_resources(identity)

    async def register_resource(self, name, identity, resource):
        resource_id = None
        # TODO - support the ability to limit the exposed API on the resource

        if resource_id in self._resources:
            raise Exception(f'A resource for {identity} named {name} already exists in this workflow')
            # TODO - custom exception

        step_id = None

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
            await self.resource_queues[identity].put(self._history[-1])

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
        logging.debug(f'{self._get_task_name()} Removing {resource_id}')
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
                await self.resource_queues[identity].put(self._history[-1])

            else:
                with next_record as record:
                    assert record['type'] == 'delete_resource'
                    assert record['resource_id'] == resource_id

    # TODO - internal (and external?) events