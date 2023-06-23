import asyncio
import json
import logging
import shutil
import uuid
from pathlib import Path
from src.quest import step, state, queue, WorkflowManager
from src.quest.events import UniqueEvent
from src.quest.json_seralizers import JsonMetadataSerializer, JsonEventSerializer, StatelessWorkflowSerializer
from src.quest.workflow import Status

logging.basicConfig(level=logging.DEBUG)
INPUT_EVENT_NAME = 'input'


@step
async def throw_error():
    raise Exception("This is a test")


@step
async def display(text: str):
    print(text)


@step
async def get_input(prompt: str):
    async with state('prompt', prompt), queue('input') as input:
        await display(prompt)
        return await input.pop()


async def register_user(welcome_message):
    await display(welcome_message)
    name = await get_input('Name: ')
    sid = await get_input('Student ID: ')
    return f'Name: {name}, ID: {sid}'


async def main():
    saved_state = Path('saved-state')

    # Remove data
    shutil.rmtree(saved_state, ignore_errors=True)

    workflow_manager = WorkflowManager(
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state / 'workflow_steps'),
        JsonEventSerializer(saved_state / 'workflow_state'),
        JsonEventSerializer(saved_state / 'workflow_queues'),
        JsonEventSerializer(saved_state / 'workflow_unique_ids', lambda d: UniqueEvent(**d)),
        {'register_user': StatelessWorkflowSerializer(lambda: register_user)}
    )

    workflow_id = str(uuid.uuid4())

    async with workflow_manager:
        status = await workflow_manager.start_workflow(workflow_id, 'register_user', 'Howdy')
        assert status.status == 'SUSPENDED'

        assert status.state['prompt']['value'] == 'Name: '
        assert 'input' in status.queues

        print('---')
        fingerprint = await workflow_manager.push_queue(workflow_id, 'input', 'Foo')
        assert fingerprint is not None

        status = await workflow_manager.get_status(workflow_id)
        assert status.status == 'SUSPENDED'

        assert status.state['prompt']['value'] == 'Student ID: '
        assert 'input' in status.queues

    # The manager (and all workflows) now shuts down

    async with workflow_manager:
        # The workflow manager comes back online
        print('---')
        fingerprint = await workflow_manager.push_queue(workflow_id, 'input', '123')
        assert fingerprint is not None

        status = await workflow_manager.get_status(workflow_id)
        assert status.status == 'COMPLETED'

        assert status.state['result']['value'] == 'Name: Foo, ID: 123'
        assert not status.queues

        print('---')
        print(json.dumps(dict(workflow_manager.workflows[workflow_id].state.items()), indent=2))


if __name__ == '__main__':
    asyncio.run(main())
