import asyncio

from quest import Server, create_filesystem_manager, state, queue
from test_basic import workflow


async def workflow():
    async with state('message', None, '') as message            :
        await state.set('Enter a number: ')

        async with queue('response', None) as response:
            resp = await response.get()

        await state.set('You said: ' + resp)


async def main():
    async with create_filesystem_manager('state', 'simple', {
        'workflow': workflow
    }) as manager, Server(manager, 'localhost', '1234'):
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
