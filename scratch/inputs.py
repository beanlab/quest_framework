import asyncio
import random
import uuid
from pathlib import Path

from history import task, create_filesystem_historian


@task
async def ainput(*args):
    return await asyncio.get_event_loop().run_in_executor(None, input, *args)


@task
async def random_stuff(ident, quit: asyncio.Event):
    while not quit.is_set():
        await asyncio.sleep(random.random() * 3)
        print('random', ident)


async def do_stuff():
    quit = asyncio.Event()
    msg_task = ainput('Message: ')
    for i in range(3):
        random_stuff(i, quit)
    print('The message:', await msg_task)
    quit.set()


async def main():
    wid = 'input-' + str(uuid.uuid1())

    async with create_filesystem_historian(
            Path('state'),
            'guess_game_demo',
            lambda wid: do_stuff
    ) as manager:
        manager.start_soon(
            '',
            wid
        )
        await manager.wait_for_completion(wid, None)


if __name__ == '__main__':
    asyncio.run(main())
