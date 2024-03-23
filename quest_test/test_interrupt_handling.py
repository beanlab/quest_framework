import asyncio
from pathlib import Path
import shutil
from src.quest import step, create_filesystem_historian
import sys

# This test is designed to test Quest's ability to gracefully and cleanly clean up when sent a Keyboard Interrupt. 

# WHAT HAPPENS: The approach used here is to run a workflow that will raise a Keyboard Interrupt when it reaches a given 
# trigger (see throwExceptionOnTrigger). The test function (test_interrupt_handling), runs the workflow multiple 
# times within a loop. On each iteration, a new event loop is provided to asyncio and the workflow is run inside
# that loop. The trigger value is set based on which iteration the test function is on, which determines which
# iteration the workflow will raise a Keyboard Interrupt during. For example, on the first iteration, the trigger
# is set to a value of 1. Then, once the workflow is started, it iterates over MAX_ITERATIONS and compares its
# iteration number to the trigger (the comparison happens inside throwExceptionOnTrigger). If it's a match, the
# Keyboard Interrupt is raised, suspending the workflow and destroying the asyncio event loop. On iteration 2, 
# the workflow will raise the exception on its own iteration 2, etc. Each time the workflow is run, it is in an
# entirely new event loop, but pulls from the JSON files stored by the previous run of the workflow. 

# WHY THIS WORKS: There are two key aspects of this test that turn it into a valid test. First, the JSON files that
# make up the persistent memory of the workflow aree NOT deleted between runs, which means each successive run sees
# and uses the history of the one before it. This is essential because Quest needs to be able to resume, after being
# totally killed and stopped, without errors. None of the errors or exceptions should be recorded when the workflow
# is interrupted, so resuming from the previous runs records is an important part of proving the concept. However,
# the second aspect is what really proves the concept, regardless of how the memory/history is being passed around.
# We don't care how quest knows to resume, but we do care that it completes the correct steps, and we want to see
# that it progressed correctly. In this test, monitoring how far the workflow progressed before raising the exception
# is accomplished by keeping a global list of three events (three_events). Before each workflow run, the test clears
# the events to their unset state. The workflow sets one event each iteration, marking how far it got before the 
# Keyboard Interrupt was raised. Once the workflow is destroyed, the test function examins the list of events to
# ensure that the correct number of events was set. In the final case, the test function's iteration is 4, but the 
# workflow only iterates to iteration 3. This means that any number >= 4 will set all 3 events and the workflow
# will return normally. This test function also does this check if the workflow returns without incident.

# NOTES:
# This file is set up to be run as a regular script too, not just a pytest. Error messages will go to err.txt in the working directory.
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
                if i < len(three_events):
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