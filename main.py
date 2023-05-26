import asyncio
import json
import logging
import shutil
import uuid
from pathlib import Path
from quest import async_event, async_signal, WorkflowManager
from quest.default_seralizers import JsonMetadataSerializer, JsonEventSerializer, StatelessWorkflowSerializer

logging.basicConfig(level=logging.DEBUG)
INPUT_EVENT_NAME = 'input'


class RegisterUserFlow:

    @async_event
    async def display(self, text: str):
        print(text)

    @async_signal(INPUT_EVENT_NAME)
    async def get_input(self): ...

    async def get_name(self):
        await self.display('Name: ')
        return await self.get_input()

    async def get_student_id(self):
        await self.display('Student ID: ')
        return await self.get_input()

    async def __call__(self, welcome_message):
        await self.display(welcome_message)
        name = await self.get_name()
        sid = await self.get_student_id()
        await self.display(f'Name: {name}, ID: {sid}')


async def main():
    saved_state = Path('saved-state')

    # Remove data
    shutil.rmtree(saved_state, ignore_errors=True)

    workflow_manager = WorkflowManager(
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state / 'workflow_state'),
        {'RegisterUserFlow': StatelessWorkflowSerializer(RegisterUserFlow)}
    )

    workflow_id = str(uuid.uuid4())

    async with workflow_manager:
        result = await workflow_manager.start_async_workflow(workflow_id, RegisterUserFlow(), 'Howdy')
        assert result is None

        print('---')
        result = await workflow_manager.signal_async_workflow(workflow_id, INPUT_EVENT_NAME, "Foo")
        assert result is None

    async with workflow_manager:
        print('---')
        result = await workflow_manager.signal_async_workflow(workflow_id, INPUT_EVENT_NAME, '123')
        print('---')
        assert result is not None

        print(json.dumps(workflow_manager.workflows[workflow_id]._events._state, indent=2))


if __name__ == '__main__':
    asyncio.run(main())
