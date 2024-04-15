import random
from src.quest import step
from src.quest.external import state, queue

# This version of multi guess is only different in that it accepts queue input,
    # rather than input from the blocking console.
printing = True

@step
async def getGuess(*args):
    global printing
    if printing: print(f"{args[0]}: Enter your guess:")
    async with queue('guess', None) as input:
        guess = await input.get()
    if printing: print(f"{args[0]}: Guess was {guess}")
    
    if(guess == "q"):
        return -1
    return int(guess)

@step
async def getNum():
    num = random.randint(1, 50)
    return num

@step
async def play_game(workflow_name):
    global printing
    rNum = await getNum()
    async with state('valid-guess', None, rNum), state('current-guess', None, None) as current_guess:
        guess = await getGuess(workflow_name)
        await current_guess.set(guess)
        import time
        # time.sleep(2)
        while(guess != rNum and guess != -1):
            response = f'{workflow_name}: lower than {guess}' if guess > rNum else f'{workflow_name}: higher than {guess}'
            if printing: print(response)
            # time.sleep(2)
            guess = await getGuess(workflow_name)
            # time.sleep(2)
            await current_guess.set(guess)

        if(guess == -1):
            return -1
        else:
            message = f'You guessed it! The number was {rNum}'
            return message

async def game_loop(*args, **kwargs):
    workflow_name = args[0]
    global printing
    printing = args[1]
    if printing: print(f"{workflow_name}: type q to quit the game.")
    while((res := await play_game(workflow_name)) != -1):
        print(res)
    if printing: print(f"{workflow_name}: Adios from the game loop!")
    return f"{workflow_name}: Game loop successfully completed"
