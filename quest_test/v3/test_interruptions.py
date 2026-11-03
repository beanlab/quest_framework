import asyncio
import os
import signal
from asyncio import CancelledError

import pytest

from quest import step
from quest_test.utils import create_in_memory_workflow_manager

gate_1 = asyncio.Event()
gate_2 = asyncio.Event()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sigint_handling():
    async def workflow_1(counter_1):
        for i in range(1, 5):
            await gate_1.wait()
            counter_1[0] += 1
            gate_1.clear()
            gate_2.set()

    async def workflow_2(counter_2):
        for i in range(1, 5):
            await gate_2.wait()
            counter_2[0] += 1
            gate_2.clear()
            if i == 3:
                os.kill(os.getpid(), signal.SIGINT)
            gate_1.set()

    workflows = {
        'workflow_1': workflow_1,
        'workflow_2': workflow_2,
    }
    manager = create_in_memory_workflow_manager(workflows)

    counter_1 = [0]
    counter_2 = [0]

    async with manager:
        manager.start_workflow('workflow_1', 'w1', counter_1, delete_on_finish=False)
        manager.start_workflow('workflow_2', 'w2', counter_2, delete_on_finish=False)

        gate_1.set()

        await asyncio.sleep(0.1)
        with pytest.raises(CancelledError):
            await manager.get_workflow_result("w1")

        with pytest.raises(CancelledError):
            await manager.get_workflow_result("w2")

    assert counter_1[0] == 3
    assert counter_2[0] == 3


# @pytest.mark.integration
@pytest.mark.asyncio
async def test_sigint_handling_resume():
    gate = asyncio.Event()
    data = []

    @step
    async def append(*args):
        data.append(*args)

    async def workflow():
        await append(1)
        await gate.wait()
        await append(2)

    workflows = {
        'wtype': workflow,
    }

    wm = create_in_memory_workflow_manager(workflows)
    async with wm:
        wm.start_workflow('wtype', 'wid', delete_on_finish=False)
        await asyncio.sleep(0.1)
        with pytest.raises(asyncio.CancelledError):
            os.kill(os.getpid(), signal.SIGINT)

    print('no wm')

    async with wm:
        assert wm.has_workflow('wid')
        await asyncio.sleep(0.1)
        gate.set()
        await wm.get_workflow_result('wid')
        await asyncio.sleep(0.1)

    assert data == [1, 2]
