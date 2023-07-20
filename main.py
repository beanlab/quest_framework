import asyncio
import json
import logging
import shutil
import uuid
from pathlib import Path

from src.quest import step
from src.quest.external import state, queue
from src.quest.historian import Historian
from src.quest.persistence import JsonHistory, JsonDictionary

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


async def run_persistent_workflow(root_folder: Path, workflow_id: str, workflow_func, *args, **kwargs):
    with JsonHistory((root_folder / "workflow_history" / workflow_id).with_suffix(".json")) as history, \
            JsonDictionary(
                (root_folder / "workflow_unique_events" / workflow_id).with_suffix(".json")) as unique_events:
        historian = Historian(
            workflow_id,
            workflow_func,
            history,
            unique_events
        )
        return await historian.run(*args, **kwargs)


async def main():
    saved_state = Path('saved-state')

    # Remove data
    shutil.rmtree(saved_state, ignore_errors=True)

    workflow_id = str(uuid.uuid4())

    history_file = (saved_state / "workflow_history" / workflow_id).with_suffix(".json")
    unique_events_file = (saved_state / "workflow_unique_events" / workflow_id).with_suffix(".json")

    with JsonHistory(history_file) as history, JsonDictionary(unique_events_file) as unique_events:
        historian = Historian(
            workflow_id,
            register_user,
            history,
            unique_events
        )
        workflow_task = asyncio.create_task(historian.run('Howdy'))
        await asyncio.sleep(0.1)

        resources = historian.get_resources(None)
        assert resources['prompt']['value'] == 'Name: '

        await historian.record_external_event('input', None, 'put', 'Foo')
        await asyncio.sleep(0.1)
        historian.suspend()

    with JsonHistory(history_file) as history, JsonDictionary(unique_events_file) as unique_events:
        historian = Historian(
            workflow_id,
            register_user,
            history,
            unique_events
        )
        workflow_task = asyncio.create_task(historian.run('Howdy'))
        await asyncio.sleep(0.1)

        resources = historian.get_resources(None)
        assert resources['prompt']['value'] == 'Student ID: '

        await historian.record_external_event('input', None, 'put', '123')

        assert await workflow_task == 'Name: Foo, ID: 123'

        print(json.dumps(list(history), indent=2))


if __name__ == '__main__':
    asyncio.run(main())
