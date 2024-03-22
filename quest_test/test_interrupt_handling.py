import asyncio
from pathlib import Path
import shutil
from src.quest import step, create_filesystem_historian
import sys

# NOTES:
# You can run this test without pytest. Error messages will go to err.txt in the working directory
# When running from pytest, use the "-s" argument to capture output since this test produces many asyncio errors when killing the event loop

sys.stderr = open('err.txt', 'w') # you'll want this file listed in your .gitignore file

# global events to be set by the workflow and indicate how far the workflow progressed
three_events: list[asyncio.Event] = [asyncio.Event(), asyncio.Event(), asyncio.Event()]

# directory for storage of json files
test_state = Path("saved-state")

# broadly used constant and variables
MAX_ITERATIONS = 4
SUCCESSFULL_COMPLETION = "workflow completed without exception"
interrupt_trigger = 0

@step
async def throwExceptionOnTrigger(current_iteration):
    global interrupt_trigger
    if(current_iteration == interrupt_trigger):
        raise KeyboardInterrupt

async def createInterrupt():
    current_iteration: int = 1

    while(current_iteration < MAX_ITERATIONS):
        three_events[current_iteration - 1].set()
        await throwExceptionOnTrigger(current_iteration)
        current_iteration += 1

async def run_faulty_workflow():
    # test against historian
    # print("Testing interrupts on filesystem Historian:")

    # test interrupt on first iteration
    historian = create_filesystem_historian(test_state, "Interrupt_Testing", createInterrupt)

    task: asyncio.Task = historian.run()
    await task

    return SUCCESSFULL_COMPLETION
    # write the test for Ctrl+C here
        # it might be worth writing essentially the same test twice, but one using the
            # Historian directly, and the other running on top of a WorkflowManager so
            # that we can not only confirm the Historian handling it correctly, but also
            # that the WorkflowManager will function correctly with it. 

def test_interrupt_handling():
    # clean up files before the test
    shutil.rmtree(test_state, ignore_errors=True)

    global interrupt_trigger
    iteration = 1
    while(iteration <= MAX_ITERATIONS):
        # reset the events
        for event in three_events:
            event.clear()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            interrupt_trigger = iteration
            print(f"Iteration {iteration}:")
            result = loop.run_until_complete(run_faulty_workflow())

        except Exception as ex:
            for i in range(iteration):
                assert three_events[i].is_set()

            print("PASSED\n")
            continue

        finally:
            loop.stop()
            loop.close()
            iteration = iteration + 1

        # no exceptions were thrown (4 was passed in) all events should be set
        for event in three_events:
            assert event.is_set()

        assert result == SUCCESSFULL_COMPLETION
        print("PASSED\n")
        continue

if __name__ == '__main__': 
    test_interrupt_handling()