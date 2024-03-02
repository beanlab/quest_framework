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
    # TODO: declare global events for setting in-between interrupts (I would expect to need at least 2)
    # TODO: create @step that throws a KeyboardInterrupt

async def createInterrupt():
    # TODO: use a send_message type pattern (see the test_manager_background function) to tell the workflow
        # when to throw the KeyboardInterrupt
    pass    

async def test_interrupt_handling():
    test_state = Path("test-state-(temp)")
    historian = create_filesystem_historian(test_state, "Interrupt_Testing", createInterrupt)


    # write the test for Ctrl+C here
        # it might be worth writing essentially the same test twice, but one using the
            # Historian directly, and the other running on top of a WorkflowManager so
            # that we can not only confirm the Historian handling it correctly, but also
            # that the WorkflowManager will function correctly with it.

    
    # clean up files after the test
    shutil.rmtree(test_state, ignore_errors=True) 



# asyncio.run(test_manager_background())
# TODO: we do need to figure out how this applies to SIGTERM.  
asyncio.run(test_interrupt_handling())


