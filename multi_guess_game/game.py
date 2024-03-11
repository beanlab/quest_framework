import asyncio
import random
from pathlib import Path

from src.quest import step, create_filesystem_historian


@step
async def get_target():
    return random.randint(1, 100)


@step
async def guess_loop(target: int):
    while True:
        guess_string = input("Enter guess: ")
        try:
            guess = int(guess_string)
            if guess == target:
                print("Congratulations! You guessed the correct number")
                break
            elif guess < target:
                print("Too low")
            elif guess > target:
                print("Too high")
        except ValueError:
            print("Invalid guess. Please provide a valid integer")

async def game_loop():
    print("Welcome to Multi-Guess")
    while True:
        target = await get_target()
        print("Guess a number between 1 and 100")
        await guess_loop(target)
        user_input = input("Do you wish to play again? y or n: ")
        user_input.lower()
        if user_input == 'n':
            break
    print("Thanks for playing!")


async def main():
    saved_state = Path("game_state")

    historian = create_filesystem_historian(
        saved_state, 'multi_guess_game', game_loop
    )

    historian.run()


if __name__ == '__main__':
    asyncio.run(main())