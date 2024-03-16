import random
from src.quest import step
from src.quest.external import state, queue

# This version of multi guess is only different in that it accepts queue input,
    # rather than input from the blocking console

@step
async def getGuess():
    print("Enter your guess:")
    async with state('guess-prompt', None, "Enter your guess:"), queue('guess', None) as input:
        guess = await input.get()
        
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
