import asyncio
import logging
from pathlib import Path
import shutil
from src.quest import step, create_filesystem_historian
from src.quest import PersistentHistory, queue
from src.quest.manager import WorkflowManager
from src.quest.persistence import InMemoryBlobStorage, LocalFileSystemBlobStorage

async def test_manager_background():
    # storage = InMemoryBlobStorage() # this caused issues when the objects stored as values would disappear
    storage = LocalFileSystemBlobStorage(Path('test-state'))
    histories = {}

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, InMemoryBlobStorage())
        return histories[wid]

    counter_a = 0
    counter_b = 0
    total = 0

    async def workflow(arg: int):
        nonlocal counter_a, counter_b, total
        total = arg

        logging.info('workflow started')
        counter_a += 1

        async with queue('messages', None) as Q:
            while True:
                message = await Q.get()
                logging.info(f'message received: {message}')
                counter_b += 1

                if message == 0:
                    return

                total += message

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        manager.start_workflow('workflow', 'wid1', True, 1)
        await asyncio.sleep(0.1)
        await manager.send_event('wid1', 'messages', None, 'put', 2)
        await asyncio.sleep(0.1)
        # Now pause the manager and all workflows

    assert 'wid1' in histories
    assert counter_a == 1
    assert counter_b == 1

    await asyncio.sleep(.1)

    async with WorkflowManager('test-manager', storage, create_history, lambda w_type: workflow) as manager:
        # At this point, all workflows should be resumed
        await asyncio.sleep(0.1)
        await manager.send_event('wid1', 'messages', None, 'put', 3)
        assert manager.has_workflow('wid1')
        await manager.send_event('wid1', 'messages', None, 'put', 0)  # i.e. end the workflow
        await asyncio.sleep(0.1)  # workflow now finishes and removes itself
        assert not manager.has_workflow('wid1')
        assert total == 6

    assert counter_a == 2
    assert counter_b == 4  # 2, replay 2, 3, 0

    print("test_manager_background() completed successfully with no assertion failures.")

# TODO: this should become its own test file

# global events to be set  by the workflow and indicate how for the workflow progressed
three_events: list[asyncio.Event] = [asyncio.Event(), asyncio.Event(), asyncio.Event()]

@step
async def throwExceptionOnTrigger(current_iteration, iteration_to_trigger):
    if(current_iteration == iteration_to_trigger):
        raise KeyboardInterrupt

async def createInterrupt(iteration_to_trigger):
    current_iteration: int = 0
    # try:
    while(current_iteration < 3):
        await throwExceptionOnTrigger(current_iteration, iteration_to_trigger)
        three_events[current_iteration].set()
        current_iteration += 1
    # except KeyboardInterrupt:
    #     pass

    return current_iteration

async def test_interrupt_handling():
    test_state = Path("test-state-(temp)")

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

    
    # clean up files after the test
    shutil.rmtree(test_state, ignore_errors=True) 



# asyncio.run(test_manager_background())
# TODO: we do need to figure out how this applies to SIGTERM. 
if __name__ == '__main__': 
    loop = asyncio.new_event_loop()
    
    try:
        loop.run_until_complete(test_interrupt_handling())

    except Exception as ex:
        print(f"Caught exception {ex.__class__}")

    finally:
        loop.close()


