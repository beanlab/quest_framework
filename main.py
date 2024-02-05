import asyncio
import json
import logging
import shutil
import uuid
from pathlib import Path

from src.quest import step, create_filesystem_historian
from src.quest.external import state, queue
from src.quest.persistence import LocalFileSystemBlobStorage, PersistentHistory
from src.quest.manager import WorkflowManager

logging.basicConfig(level=logging.DEBUG)
INPUT_EVENT_NAME = 'input'


@step
async def display(text: str):
    print(text)


@step
async def get_input(prompt: str):
    async with state('prompt', None, prompt), queue('input', None) as input:
        await display(prompt)
        return await input.get()


async def register_user(welcome_message):
    await display(welcome_message)
    name = await get_input('Name: ')
    sid = await get_input('Student ID: ')
    return f'Name: {name}, ID: {sid}'


async def main():
    saved_state = Path('saved-state')

    # Remove data
    shutil.rmtree(saved_state, ignore_errors=True)

    workflow_id = str(uuid.uuid4())

    storage = LocalFileSystemBlobStorage(saved_state)
    history = PersistentHistory(workflow_id, storage)

    async with WorkflowManager(workflow_id, storage, lambda arg: history, lambda arg: register_user) as manager:
        manager.start_workflow('workflow', 'demo', 'Howdy')
        await asyncio.sleep(0.1)
        resources = await manager.get_resources('demo', None)
        assert resources['prompt']['value'] == 'Name: '
        await manager.send_event('demo', 'input', None, 'put', 'Foo')
        # by exiting, we should suspend the workflows

    async with WorkflowManager(workflow_id, storage, lambda arg: history, lambda arg: register_user) as manager:
        resources = await manager.get_resources('demo', None)
        assert resources['prompt']['value'] == 'Student ID: '
        await manager.send_event('demo', 'input', None, 'put', '123')
        result = await manager.get_workflow('demo')
        assert result == 'Name: Foo, ID: 123'

    for file in sorted(saved_state.iterdir()):
        content = json.loads(file.read_text())
        print(json.dumps(content, indent=2))

if __name__ == '__main__':
    asyncio.run(main())
