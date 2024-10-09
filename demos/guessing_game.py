import asyncio
import random
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent / 'src'))

from quest import step, create_filesystem_manager, ainput


@step
async def pick_number(lower, upper):
    return random.randint(lower, upper)


@step
async def display(*args, **kwargs):
    print(*args, **kwargs)


@step
async def get_input(*args):
    return await ainput(*args)


async def guessing_game():
    secret = await pick_number(1, 100)
    await display("I have a number between 1 and 100.")
    while True:
        guess = int(await get_input('Guess: '))
        if guess < secret:
            await display('Higher!')
        elif guess > secret:
            await display('Lower!')
        else:
            break
    await display('You got it!')


async def main():
    # TODO: Make a SQL backend for Blobstorage
    async with create_filesystem_manager(
            Path('state'),
            'guess_game_demo',
            lambda wid: guessing_game
    ) as manager:
        manager.start_workflow(
            '',
            'demo'
        )
        await manager.wait_for_completion('demo', None)


if __name__ == '__main__':
    asyncio.run(main())
