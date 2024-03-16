import random
from src.quest import step
from src.quest.external import state, queue

# This version of multi guess is only different in that it accepts queue input,
    # rather than input from the blocking console

@step
async def getGuess(*args):
    print(f"{args[0]}: Enter your guess:")
    async with state('guess-prompt', None, "Enter your guess:"), queue('guess', None) as input:
        guess = await input.get()
    print(f"{args[0]}: Guess was {guess}")
    
    if(guess == "q"):
        return -1
    return int(guess)

@step
async def getNum():
    return random.randint(1, 50)

@step
async def play_game(workflow_name):
    rNum = await getNum()
    guess = await getGuess(workflow_name)
    while(guess != rNum and guess != -1):
        response = f'{workflow_name}: lower than {guess}' if guess > rNum else f'{workflow_name}: higher than {guess}'
        print(response)
        guess = await getGuess(workflow_name)

    if(guess == -1):
        return -1
    else:
        message = f'You guessed it! The number was {rNum}'
        return message

async def game_loop(*args, **kwargs):
    # workflow_name = kwargs['workflow_name']
    workflow_name = args[0]
    print(f"{workflow_name}: type q to quit the game.")
    while((res := await play_game(workflow_name)) != -1):
        print(res)
    print(f"{workflow_name}: Adios from the game loop!")
    return f"{workflow_name}: Game loop successfully completed"
