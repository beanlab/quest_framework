import asyncio
from pathlib import Path
import shutil
from src.quest import step, create_filesystem_historian
import sys

sys.stderr = open('err.txt', 'w')

# global events to be set by the workflow and indicate how far the workflow progressed
three_events: list[asyncio.Event] = [asyncio.Event(), asyncio.Event(), asyncio.Event()]

# directory for storage of json files
test_state = Path("test-state-(temp)")

# broadly used constant and variables
MAX_ITERATIONS = 3
SUCCESSFULL_COMPLETION = "workflow completed without exception"

class Trigger():
    def __init__(self):
        self.iteration_trigger = 0
    
    def get_trigger(self):
        return self.iteration_trigger
    
    def set_trigger(self, new_val: int):
        self.iteration_trigger = new_val

ITERATION_TRIGGER: Trigger = Trigger()

@step
async def throwExceptionOnTrigger(current_iteration):
    if(current_iteration == ITERATION_TRIGGER.get_trigger()):
        raise KeyboardInterrupt

async def createInterrupt():
    current_iteration: int = 1

    while(current_iteration <= MAX_ITERATIONS):
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

def test_manager_background():
    # clean up files before the test
    shutil.rmtree(test_state, ignore_errors=True)

    iteration = 1
    while(iteration <= MAX_ITERATIONS + 1):
        # reset the events
        for event in three_events:
            event.clear()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            ITERATION_TRIGGER.set_trigger(iteration)
            print(f"Iteration number {iteration}")
            result = loop.run_until_complete(run_faulty_workflow())

        except Exception as ex:
            for i in range(iteration):
                assert three_events[i].is_set()

            continue

        finally:
            loop.stop()
            loop.close()
            iteration = iteration + 1

        # no exceptions were thrown (4 was passed) all events should be set
        for event in three_events:
            assert event.is_set()

        assert result == SUCCESSFULL_COMPLETION
        continue

if __name__ == '__main__': 
    test_manager_background()