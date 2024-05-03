import random
import signal
from threading import get_ident
from src.quest import step

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
    if int(guess) == 77: signal.pthread_kill(get_ident(), signal.SIGINT)
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
