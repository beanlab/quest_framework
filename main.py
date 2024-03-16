import asyncio
from pathlib import Path
import shutil

from demos.multiGuessQueue import game_loop
from src.quest import create_filesystem_manager, create_filesystem_historian

async def main():
    saved_state = Path('saved-state-main.py')

    # Remove data
    shutil.rmtree(saved_state, ignore_errors=True)
    workflow_namespace_root = 'multi-guess-game'
    workflow_number = 1

    def get_workflow(arg: str):
        return game_loop

    async with create_filesystem_manager(saved_state, workflow_namespace_root, get_workflow) as manager:
        
        # create and start two workflows
        workflow_1 = f'{workflow_namespace_root}-{workflow_number}'
        workflow_number = workflow_number + 1
        workflow_2 = f'{workflow_namespace_root}-{workflow_number}'
        
        manager.start_workflow('multi-guess', workflow_1, False, workflow_1, '-w', '-r', workflow_name=workflow_1)
        await asyncio.sleep(0.1)
        manager.start_workflow('multi-guess', workflow_2, False, workflow_2, '-w', '-r', workflow_name=workflow_2)

        # advance the first workflow once
        await asyncio.sleep(0.1)
        await manager.send_event(workflow_1, 'guess', None, 'put', 5)
        
        # start a third workflow
        workflow_number = workflow_number + 1
        workflow_3 = f'{workflow_namespace_root}-{workflow_number}'
        manager.start_workflow('multi-guess', workflow_3, False, workflow_3, '-w', '-r', workflow_name=workflow_3)
        await asyncio.sleep(0.1)

        # advance the third workflow twice
        await manager.send_event(workflow_3, 'guess', None, 'put', 7)
        await asyncio.sleep(0.1)
        await manager.send_event(workflow_3, 'guess', None, 'put', 9)
        await asyncio.sleep(0.1)
        # leave the context

    async with create_filesystem_manager(saved_state, workflow_namespace_root, get_workflow) as manager:
        manager.start_workflow('multi-guess', workflow_1, False, workflow_1, '-w', '-r', workflow_name=workflow_1)
        manager.start_workflow('multi-guess', workflow_2, False, workflow_2, '-w', '-r', workflow_name=workflow_2)
        manager.start_workflow('multi-guess', workflow_3, False, workflow_3, '-w', '-r', workflow_name=workflow_3)

        # TODO: this is where you'll check assertions about the states of the games

        await asyncio.sleep(0.1)
        await manager.send_event(workflow_1, 'guess', None, 'put', 'q')
        await asyncio.sleep(0.1)
        await manager.send_event(workflow_2, 'guess', None, 'put', 'q')
        await asyncio.sleep(0.1)
        await manager.send_event(workflow_3, 'guess', None, 'put', 'q')
        await asyncio.sleep(0.1)

        wk1 = manager.get_workflow(workflow_1)
        wk2 = manager.get_workflow(workflow_2)
        wk3 = manager.get_workflow(workflow_3)
        await wk1
        await wk2
        await wk3

        # result = await manager.get_workflow(workflow_1)
        # print(f'Result: {result}')
        
if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    
    try:
        loop.run_until_complete(main())

    finally:
        loop.close()