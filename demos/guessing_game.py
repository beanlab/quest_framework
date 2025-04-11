import asyncio
import random
import selectors
import sys
from pathlib import Path

from history import step, create_filesystem_historian, ainput

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
    state = Path('state')
    # state.rmdir()
    async with create_filesystem_historian(
            state,
            'guess_game_demo',
            lambda wid: guessing_game
    ) as manager:
        if not manager.has('demo'):
            manager.start_soon(
                '',
                f'demo'
            )
        await manager.get_workflow('demo')


if __name__ == '__main__':
    asyncio.run(main())
