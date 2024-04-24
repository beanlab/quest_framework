import asyncio
from pathlib import Path
import shutil
import sys
import logging

from multi_guess_src.multi_guess_queue import game_loop
from src.quest import create_filesystem_manager
from src.quest import WorkflowManager

sys.stderr = open("stderr.txt", "w") # make sure you add this to your .gitignore file
logging.basicConfig(level=logging.DEBUG)

async def make_guess(manager: WorkflowManager, workflow_id: str, the_guess: int|str):
    resources = await manager.get_resources(workflow_id, None)
    winning_guess = the_guess == resources['valid-guess']['value']

    await manager.send_event(workflow_id, 'guess', None, 'put', the_guess)
    await asyncio.sleep(0.1)

    if not winning_guess:
        resources = await manager.get_resources(workflow_id, None)
        assert resources['current-guess']['value'] == the_guess

async def main(args):
    saved_state = Path('saved-state-main.py')

    if "--restart" in args:
        # Remove data
        shutil.rmtree(saved_state, ignore_errors=True)
        print("Json files deleted")

    printing = True
    if "--no-print" in args:
        printing = False
    
    workflow_namespace_root = 'multi-guess-game'
    workflow_number = 1

    def get_workflow(arg: str):
        return game_loop

    # initial partial run of workflows
    async with create_filesystem_manager(saved_state, workflow_namespace_root, get_workflow) as manager:
        print("Initial run of workflows:\n")

        # create and start two workflows
        workflow_1 = f'{workflow_namespace_root}-{workflow_number}'
        workflow_number = workflow_number + 1
        workflow_2 = f'{workflow_namespace_root}-{workflow_number}'
        manager.start_workflow('multi-guess', workflow_1, False, workflow_1, printing)
        await asyncio.sleep(0.1)
        manager.start_workflow('multi-guess', workflow_2, False, workflow_2, printing)
        await asyncio.sleep(0.1)

        assert manager.get_workflow(workflow_1).done() == False
        assert manager.get_workflow(workflow_2).done() == False

        # advance the first workflow once
        await make_guess(manager, workflow_1, 5)
        
        # start a third workflow
        workflow_number = workflow_number + 1
        workflow_3 = f'{workflow_namespace_root}-{workflow_number}'
        manager.start_workflow('multi-guess', workflow_3, False, workflow_3, printing)
        await asyncio.sleep(0.1)
        assert manager.get_workflow(workflow_3).done() == False

        # advance the third workflow twice
        await make_guess(manager, workflow_3, 7)
        await make_guess(manager, workflow_3, 9)
        # leave the context

    # completion of workflows
    async with create_filesystem_manager(saved_state, workflow_namespace_root, get_workflow) as manager:
        print("\nExiting, resuming, and completing workflows:\n")
        manager.start_workflow('multi-guess', workflow_1, False, workflow_1, printing)
        manager.start_workflow('multi-guess', workflow_3, False, workflow_3, printing)
        manager.start_workflow('multi-guess', workflow_2, False, workflow_2, printing)
        await asyncio.sleep(0.1)

        # workflow_1 assertions
        assert manager.get_workflow(workflow_1).done() == False
        resources = await manager.get_resources(workflow_1, None)
        current_guess = resources['current-guess']['value']
        if current_guess is not None:
            assert current_guess == 5

        # workflow_2 assertions
        assert manager.get_workflow(workflow_2).done() == False
        resources = await manager.get_resources(workflow_2, None)
        assert resources['current-guess']['value'] is None

        # workflow_3 assertions
        assert manager.get_workflow(workflow_3).done() == False
        resources = await manager.get_resources(workflow_3, None)
        current_guess = resources['current-guess']['value']
        if current_guess is not None:
            assert current_guess == 9

        # complete game 1 naturally instead of quitting it
        guess = 1
        while(guess <= 50):
            resources = await manager.get_resources(workflow_1, None)
            send = await manager.send_event(workflow_1, 'guess', None, 'put', guess)
            await asyncio.sleep(0.1)

            if 'valid-guess' in resources and resources['valid-guess']['value'] == guess:
                await manager.send_event(workflow_1, 'guess', None, 'put', 'q')
                await asyncio.sleep(0.1)
                break

            guess = guess + 1


        # make sure that the workflow_1 task actually completed once the number was correctly guessed
        assert manager.get_workflow(workflow_1).done() == True

        # kill and assert workflow_2
        await manager.send_event(workflow_2, 'guess', None, 'put', 'q')
        await asyncio.sleep(0.1)
        assert manager.get_workflow(workflow_2).done() == True

        # kill and assert workflow_3
        await manager.send_event(workflow_3, 'guess', None, 'put', 'q')
        await asyncio.sleep(0.1)
        assert manager.get_workflow(workflow_3).done() == True

        wk1 = manager.get_workflow(workflow_1)
        wk2 = manager.get_workflow(workflow_2)
        wk3 = manager.get_workflow(workflow_3)
        await wk1
        await wk2
        await wk3

        print(wk1.result())
        print(wk2.result())
        print(wk3.result())

    # attempt to start previously completed workflows - should cause no errors and output final result again
    async with create_filesystem_manager(saved_state, workflow_namespace_root, get_workflow) as manager:
        print("\nAttempting to restart completed workflows:\n")
        manager.start_workflow('multi-guess', workflow_1, False, workflow_1, printing)
        manager.start_workflow('multi-guess', workflow_2, False, workflow_2, printing)
        manager.start_workflow('multi-guess', workflow_3, False, workflow_3, printing)
        await asyncio.sleep(0.1)

        send = await manager.send_event(workflow_1, 'guess', None, 'put', 17)
        assert send == False

        send = await manager.send_event(workflow_2, 'guess', None, 'put', 17)
        assert send == False

        send = await manager.send_event(workflow_3, 'guess', None, 'put', 17)
        assert send == False

        wk1 = manager.get_workflow(workflow_1)
        wk2 = manager.get_workflow(workflow_2)
        wk3 = manager.get_workflow(workflow_3)
        await wk1
        await wk2
        await wk3

        print(wk1.result())
        print(wk2.result())
        print(wk3.result())
        # check for WorkflowManager hanging on some unfinished task

def run_main(args):
    loop = asyncio.new_event_loop()
    loop.set_debug(True)

    try:
        loop.run_until_complete(main(args)) 
        # TODO: also, did you figure out how the get_workflow on a finished task should work?

    # this actually isn't necessary. It just allows main.py to return 0 when an exception happens in its code outside of quest
    except (Exception, KeyboardInterrupt) as ex:
        print(f'main.py received and handled an exeption and will now exit:\n')
        print(ex)
        exit(0)
        
    finally:
        loop.stop()
        loop.close()
        
if __name__ == '__main__':
    run_main(sys.argv)
    