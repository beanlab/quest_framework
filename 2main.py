import asyncio
from pathlib import Path
import shutil
import sys
import logging

from multi_guess_src.multi_guess_queue import game_loop
from src.quest import create_filesystem_manager

sys.stderr = open("2stderr.txt", "w") # make sure you add this to your .gitignore file
logging.basicConfig(level=logging.DEBUG)

async def main(args):
    saved_state = Path('saved-state-main2.py')

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
        print("Starting...\n")

        # create and start two workflows
        workflow_1 = f'{workflow_namespace_root}-{workflow_number}'
        manager.start_workflow('multi-guess', workflow_1, False, workflow_1, printing)
        await asyncio.sleep(0.1)

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

        assert manager.get_workflow(workflow_1).done() == True
        result = manager.get_workflow(workflow_1)
        await result
        print(result.result())

def run_main(args):
    loop = asyncio.new_event_loop()
    loop.set_debug(True)

    try:
        loop.run_until_complete(main(args)) 
        # TODO: also, did you figure out how the get_workflow on a finished task should work?

    except (Exception, KeyboardInterrupt) as ex:
        print(f'{ex}')
        raise
    finally:
        loop.stop()
        loop.close()
        
if __name__ == '__main__':
    run_main(sys.argv)