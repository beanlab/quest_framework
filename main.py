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
        workflow_1 = f'{workflow_namespace_root}-{workflow_number}'
        # workflow_1 = 'demo'
        manager.start_workflow('multi-guess', workflow_1, False, ['-w', '-r'])
        await asyncio.sleep(0.1)
        await manager.send_event(workflow_1, 'guess', None, 'put', 5)
        await asyncio.sleep(0.1)
        await manager.send_event(workflow_1, 'guess', None, 'put', 5)
        await asyncio.sleep(0.1)
        await manager.send_event(workflow_1, 'guess', None, 'put', 5)
        await asyncio.sleep(0.1)
        await manager.send_event(workflow_1, 'guess', None, 'put', 'q')
        await asyncio.sleep(0.1)
        result = await manager.get_workflow(workflow_1)
        print(f'Result: {result}')
        
if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    
    try:
        loop.run_until_complete(main())

    finally:
        loop.close()