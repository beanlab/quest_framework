import asyncio
import random
import selectors
import sys
from pathlib import Path

from quest import step, create_filesystem_manager, ainput

# sel = selectors.DefaultSelector()
# sel.register(sys.stdin, selectors.EVENT_READ)
#
#
# def stdin_has_data():
#     return sel.select(timeout=0)


# async def ainput(prompt: str) -> str:
#     print(prompt, end='', flush=True)
#     line = []
#     while True:
#         if not stdin_has_data():
#             await asyncio.sleep(0.01)
#         else:
#             for i in range(sys.stdin.buffer.tell()):
#                 char = sys.stdin.read(1)
#                 if char == '\n':
#                     return ''.join(line)
#                 else:
#                     line.append(char)


# async def ainput(prompt: str) -> str:
#     print(prompt, end='', flush=True)
#     line = []
#     while len(line) < 5:
#         await asyncio.sleep(1)
#         line.append(str(len(line)))
#     return ''.join(line)


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
    async with create_filesystem_manager(
            state,
            'guess_game_demo',
            lambda wid: guessing_game
    ) as manager:
        manager.start_workflow(
            '',
            f'demo'
        )
        await manager.get_workflow('demo')


if __name__ == '__main__':
    asyncio.run(main())
