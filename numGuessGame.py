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

# You will need a launch.json file to start the program with the correct arguments. For running, a CLI example is as follows:
    # python ./numGuessGame.py -r -w
    # When you debug, you'll need to make sure there are the correct command line arguments. This should be pretty easy in a Jetbrains
        # IDE, but you'll need to provide a launch.json file for VS Code.

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
    return "Game loop successfully completed"

def usage():
    print("\nUSAGE: Please specify which method with which to run the game.")
    print("\n\t\"-w\" to use Workflow Manager, or \"-h\" to run directly on the historian.")
    print("\n\tSpecifying \"-r\" with any other flag, or none, will refresh the history.\n")
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
        args.remove("-r")
        if len(args) < 2:
            exit(0)

    result = "<PROGRAM RESULT>"

    if(args[1] == "-h"):
        historian = create_filesystem_historian(game_state, "Guessing_Game", game_loop)
        task = historian.run()
        await task
        result = task.result()

    elif(args[1] == "-w"):
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

if __name__ == '__main__':
    loop = asyncio.new_event_loop()

    try:
        result = loop.run_until_complete(main())
        print(f"Result returned by the program was:\n{result}")

    finally:
        loop.close()