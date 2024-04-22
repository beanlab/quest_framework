import asyncio
from pathlib import Path
import shutil
import sys
import logging

from src.quest import PersistentHistory
from src.quest.manager import WorkflowManager
from src.quest.persistence import LocalFileSystemBlobStorage

from multi_guess_src.multi_guess_terminal import game_loop

sys.stderr = open("stderr3.txt", "w")
logging.basicConfig(level=logging.DEBUG)

def usage():
    print("\nUSAGE:")
    print("\n\tSpecifying \"-r\" will refresh the history.\n")
    exit(1)

async def play_multi_guess(args: list[str]):
    game_state = Path("multi-guess-game-state")

    if "-r" in args:
        shutil.rmtree(game_state, ignore_errors=True)
        print("Previous JSON files have been removed.")

    result = "<PROGRAM RESULT>"

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

def run_play_multi_guess():
    args = input("Enter -r if you wish to wipe the json files before execution.\nFlags: ")
    args = args.split(" ")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        result = loop.run_until_complete(play_multi_guess(args))
        print(result)
    
    finally:
        loop.stop()
        loop.close()

if __name__ == '__main__':
    run_play_multi_guess()