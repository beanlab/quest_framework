import asyncio
import logging
import os
import signal
from asyncio import CancelledError

import pytest

from quest_test.utils import create_in_memory_workflow_manager


# @pytest.mark.integration
@pytest.mark.asyncio
async def test_sigint_handling():
    allow_wf1_to_proceed = asyncio.Event()
    allow_wf2_to_proceed = asyncio.Event()

    async def workflow_1():
        logging.warning('w1 started')
        allow_wf2_to_proceed.set()
        logging.warning('w1 says w2 can now proceed. Waiting on wf1 permission.')
        await allow_wf1_to_proceed.wait()

    async def workflow_2():
        logging.warning('w2 started. Waiting for permission from w1.')
        await allow_wf2_to_proceed.wait()
        logging.warning('w2 about to kill')
        os.kill(os.getpid(), signal.SIGINT)
        # wf1 should actually cancel because of the SIGINT before it gets a chance to continue
        logging.warning('w2 giving permission to w1.')
        allow_wf1_to_proceed.set()

    workflows = {
        'workflow_1': workflow_1,
        'workflow_2': workflow_2,
    }
    manager = create_in_memory_workflow_manager(workflows)

    async with manager:
        manager.start_workflow('workflow_1', 'w1', delete_on_finish=False)
        manager.start_workflow('workflow_2', 'w2', delete_on_finish=False)
        await asyncio.sleep(0.1)

        with pytest.raises(CancelledError):
            await manager.get_workflow_result('w1')

        with pytest.raises(CancelledError):
            await manager.get_workflow_result('w2')

