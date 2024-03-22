import asyncio
from pathlib import Path
import shutil

from src.quest import create_filesystem_historian
from src.quest import PersistentHistory
from src.quest.manager import WorkflowManager
from src.quest.persistence import LocalFileSystemBlobStorage

from demos.multi_guess_terminal import game_loop

def usage():
    print("\nUSAGE: Please specify which method with which to run the game.")
    print("\n\t\"-w\" to use Workflow Manager, or \"-h\" to run directly on the historian.")
    print("\n\tSpecifying \"-r\" with any other flag, or none, will refresh the history.\n")
    exit(1)

async def flexible_multi_guess(args: list[str]):
    game_state = Path("game-state")
    options = ["-w", "-h", "-r"]
    if len(args) < 1:
        usage()
    elif args[0] not in options:
        usage()

    if "-r" in args:
        shutil.rmtree(game_state, ignore_errors=True)
        print("Previous JSON files have been removed.")
        args.remove("-r")
        if len(args) < 1:
            exit(0)

    result = "<PROGRAM RESULT>"

    if(args[0] == "-h"):
        historian = create_filesystem_historian(game_state, "Guessing_Game", game_loop)
        task = historian.run()
        await task
        result = task.result()

    elif(args[0] == "-w"):
        storage = LocalFileSystemBlobStorage(game_state)
        histories = {}

        def create_history(wid: str):
            if wid not in histories:
                histories[wid] = PersistentHistory(wid, LocalFileSystemBlobStorage(game_state))
            return histories[wid]
        
        async with WorkflowManager('number-game', storage, create_history, lambda wkflw: game_loop) as manager:
            myJob: asyncio.Task = manager.start_workflow('workflow', 'game1', False, 0)
            await myJob
            result = myJob.result()
    
    return result

def run_flexible_multi_guess(args: list[str]):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        result = loop.run_until_complete(flexible_multi_guess(args))
        print(result)
    
    finally:
        loop.stop()
        loop.close()