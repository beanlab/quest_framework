import asyncio

import pytest

finished = False


def set_finished(task):
    global finished
    finished = True


async def workflow():
    return 1 + 1


def run_workflow():
    task = asyncio.create_task(workflow())
    task.add_done_callback(set_finished)
    return task


@pytest.mark.asyncio
async def test_task_completion():
    task = run_workflow()

    await asyncio.sleep(0.1)

    # the workflow finishes here

    assert not finished

    # Then we wait on the workflow
    await task

    # And now we can see that the workflow is cleaned up
    assert finished