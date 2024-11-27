import asyncio
import os
import signal
import pytest
from quest import WorkflowManager
from quest_test.utils import create_in_memory_workflow_manager


@pytest.mark.asyncio
async def test_sigint_handling():
    async def workflow_1(counter_1, gate_1, gate_2):
        for i in range(1, 5):
            await gate_1.wait()
            counter_1[0] += 1
            gate_1.clear()
            gate_2.set()

    async def workflow_2(counter_2, gate_1, gate_2):
        for i in range(1, 5):
            await gate_2.wait()
            counter_2[0] += 1
            gate_2.clear()
            if i == 3:
                os.kill(os.getpid(), signal.SIGINT)
            gate_1.set()

    # Signal handler to capture SIGINT
    sigint_received = asyncio.Event()

    def handle_sigint(signum, frame):
        sigint_received.set()

    # Register the signal handler
    signal.signal(signal.SIGINT, handle_sigint)

    workflows = {
        'workflow_1': workflow_1,
        'workflow_2': workflow_2,
    }
    manager = create_in_memory_workflow_manager(workflows)

    counter_1 = [0]
    counter_2 = [0]
    gate_1 = asyncio.Event()
    gate_2 = asyncio.Event()

    async with manager:
        manager.start_workflow('workflow_1', 'w1', counter_1, gate_1, gate_2)
        manager.start_workflow('workflow_2', 'w2', counter_2, gate_1, gate_2)

        gate_1.set()

        # Wait for SIGINT to be handled
        await asyncio.wait_for(sigint_received.wait(), timeout=1.0)

        # Allow workflows to process further if needed
        await asyncio.sleep(0.1)

        # Assertions after SIGINT
        assert counter_1[0] == 3
        assert counter_2[0] == 3
