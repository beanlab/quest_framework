import asyncio
import os
import signal
from asyncio import CancelledError

import pytest
from quest_test.utils import create_in_memory_workflow_manager
from quest.manager import find_workflow_manager


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sigint_handling():
    async def workflow_1(counter_1, gate_1_id, gate_2_id):
        manager = find_workflow_manager()
        gate_1 = await manager.get_event("w1", gate_1_id, None)
        gate_2 = await manager.get_event("w1", gate_2_id, None)

        for i in range(1, 5):
            await gate_1.wait()
            counter_1[0] += 1
            await gate_1.clear()
            await gate_2.set()

    async def workflow_2(counter_2, gate_1_id, gate_2_id):
        manager = find_workflow_manager()
        gate_1 = await manager.get_event("w1", gate_1_id, None)
        gate_2 = await manager.get_event("w1", gate_2_id, None)

        for i in range(1, 5):
            await gate_2.wait()
            counter_2[0] += 1
            await gate_2.clear()
            if i == 3:
                os.kill(os.getpid(), signal.SIGINT)
            await gate_1.set()

    workflows = {
        'workflow_1': workflow_1,
        'workflow_2': workflow_2,
    }
    manager = create_in_memory_workflow_manager(workflows)

    counter_1 = [0]
    counter_2 = [0]

    async with manager:
        manager.start_workflow('workflow_1', 'w1', counter_1, 'gate_1', 'gate_2', delete_on_finish=False)
        manager.start_workflow('workflow_2', 'w2', counter_2, 'gate_1', 'gate_2', delete_on_finish=False)

        gate_1 = await manager.get_event("w1", 'gate_1', None)
        gate_2 = await manager.get_event("w1", 'gate_2', None)

        gate_1.set()

        await asyncio.sleep(0.1)
        with pytest.raises(CancelledError):
            await manager.get_workflow_result('w1')
        with pytest.raises(CancelledError):
            await manager.get_workflow_result('w2')

    assert counter_1[0] == 3
    assert counter_2[0] == 3
