import asyncio
import logging
from pathlib import Path
import shutil
from src.quest import step, create_filesystem_historian
from src.quest import PersistentHistory, queue
from src.quest.manager import WorkflowManager
from src.quest.persistence import InMemoryBlobStorage, LocalFileSystemBlobStorage

# global events to be set  by the workflow and indicate how for the workflow progressed
three_events: list[asyncio.Event] = [asyncio.Event(), asyncio.Event(), asyncio.Event()]

# directory for storage of json files
test_state = Path("test-state-(temp)")

# broadly used constant
MAX_ITERATIONS = 3

@step
async def throwExceptionOnTrigger(current_iteration, iteration_to_trigger):
    if(current_iteration == iteration_to_trigger):
        raise KeyboardInterrupt

async def createInterrupt(iteration_to_trigger):
    current_iteration: int = 0

    while(current_iteration < 3):
        await throwExceptionOnTrigger(current_iteration, iteration_to_trigger)
        three_events[current_iteration].set()
        current_iteration += 1

    return current_iteration

async def test_interrupt_handling(iterations):
    # test against historian
    print("Testing interrupts on filesystem Historian:")

    # test interrupt on first iteration
    historian = create_filesystem_historian(test_state, "Interrupt_Testing", createInterrupt)

    try:
        task: asyncio.Task = historian.run(0)
        await task
    except asyncio.exceptions.CancelledError as ex:
        pass
    assert task.result() == 0
    for an_event in three_events:
        assert not an_event.is_set()
    
    print("\t- Interrupt on first iteration successfully passed.")

    # test interrupt on second iteration


    # write the test for Ctrl+C here
        # it might be worth writing essentially the same test twice, but one using the
            # Historian directly, and the other running on top of a WorkflowManager so
            # that we can not only confirm the Historian handling it correctly, but also
            # that the WorkflowManager will function correctly with it. 


# TODO: we do need to figure out how this applies to SIGTERM. 
if __name__ == '__main__': 
    # the below code is basically what should go in the test function since this is where we'll do assertions and restart the test routine
    loop = asyncio.new_event_loop()
    
    iteration = 0

    while(iteration < MAX_ITERATIONS):

        try:
            print(f"Iteration number {iteration}")
            loop.run_until_complete(test_interrupt_handling(iteration))

        except Exception as ex:
            # print(f"Caught exception {ex.__class__}")
            iteration = iteration + 1
            continue


    # clean up files after the test
    shutil.rmtree(test_state, ignore_errors=True)
