import asyncio
import uuid
import random
from pathlib import Path
import sys
import shutil

from src.quest import step, create_filesystem_historian
from src.quest.external import state, queue
from src.quest import PersistentHistory, queue
from src.quest.manager import WorkflowManager
from src.quest.persistence import InMemoryBlobStorage, LocalFileSystemBlobStorage

# This program is a nice way to test out the functionality of quest, especially when resuming from something like a Keyboard Interrupt,
    # or exploring the differences between using the Historian direction, or implementing the workflow using a Workflow Manager.

# USAGE: Run numGuessGame.py to start a new round of guessing games. The program will determine a number between 1 and 50 for the
    # user to guess. Guesses are responded to with whether or not the determined number is higher or lower than the guess. 
    # Typing "q" at the terminal stops the game, exits the game loop, and completes the workflow function. Of course,
    # typing Ctrl + C will kill the program where it stands and allow you to resume where you left off.

@step
async def getGuess():
    print("Enter your guess:")
    guess = input()
    if(guess == "q"):
        return -1
    return int(guess)

@step
async def getNum():
    return random.randint(1, 50)

@step
async def play_game():
    rNum = await getNum()
    guess = await getGuess()
    while(guess != rNum and guess != -1):
        response = f'lower than {guess}' if guess > rNum else f'higher than {guess}'
        print(response)
        guess = await getGuess()

    if(guess == -1):
        return -1
    else:
        message = f'You guessed it! The number was {rNum}'
        return message

async def game_loop(*args, **kwargs):
    print("type q to quit the game.")
    while((res := await play_game()) != -1):
        print(res)
    print("Adios from the game loop!")
    return "Game loop completed"

def usage():
    print("USAGE: Please specify which method with which to run the game.")
    print("\"-w\" to use Workflow Manager, or \"-h\" to run directly on the historian.")
    print("Specifying \"-r\" with any other flag, or none, will refresh the history.")
    exit(1)

async def main():
    game_state = Path("game-state")
    options = ["-w", "-h", "-r"]
    args = sys.argv

    if len(args) < 2:
        usage()
    elif args[1] not in options:
        usage()

    if "-r" in args:
        shutil.rmtree(game_state, ignore_errors=True)
        print("Previous JSON files have been removed.")

    if(args[1] == "-h"):
        workflow_id = "the_guessing_game"
        historian = create_filesystem_historian(game_state, "Guessing_Game", game_loop)
        task = historian.run()
        await task
        print(f'RESULT: {task.result()}')

    elif(args[1] == "-w"):
        storage = LocalFileSystemBlobStorage(game_state)
        histories = {}

        def create_history(wid: str):
            if wid not in histories:
                histories[wid] = PersistentHistory(wid, LocalFileSystemBlobStorage(game_state))
            return histories[wid]
        
        async with WorkflowManager('number-game', storage, create_history, lambda wkflw: game_loop) as manager:
            await manager.start_workflow('workflow', 'game1', 0)
            print('with statement is finishing')


    # the apparent advantage of using the workflow manager is that completed workflows are forgotten
        # i.e., if I keyboard interrupt and throw a Ctrl+C exception, that terminations the workflow,
        # which means that next time I start the program, I just start fresh on a new copy of the work flow.
    
    
if __name__ == '__main__':
    loop = asyncio.new_event_loop()

    try:
        loop.run_until_complete(main())

    finally:
        loop.stop()
        loop.close()