import asyncio
import json
import logging
import shutil
import uuid
from pathlib import Path
from quest import step, signal, WorkflowManager
from quest.json_seralizers import JsonMetadataSerializer, JsonEventSerializer, StatelessWorkflowSerializer
from quest.workflow import Status

logging.basicConfig(level=logging.DEBUG)
INPUT_EVENT_NAME = 'input'


class StateFlow:
    def __call__(self):
        ...


async def main():
    saved_state = Path('saved-state')

    # Remove data
    shutil.rmtree(saved_state, ignore_errors=True)

    workflow_manager = WorkflowManager(
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state / 'workflow_state'),
        {'StateFlow': StatelessWorkflowSerializer(StateFlow)}
    )

    workflow_id = str(uuid.uuid4())

    async with workflow_manager:
        pass


if __name__ == '__main__':
    asyncio.run(main())
