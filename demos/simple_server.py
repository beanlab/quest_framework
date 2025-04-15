import asyncio
import shutil
from pathlib import Path

from quest import Server, create_filesystem_manager, state, queue


async def workflow():
    async with state('message', None, 'Test') as message:
        print('Setting message')
        await message.set('Say something: ')
        print('State set')

        async with queue('response', None) as response:
            resp = await response.get()
            print('Response received: ' + str(resp))

        print('Setting message with user response')
        await message.set('You said: ' + resp)
        print("State set")
    print('Workflow finished')


async def main():
    state_folder = Path('state')
    shutil.rmtree(state_folder, ignore_errors=True)
    async with (create_filesystem_manager(state_folder, 'simple', lambda wid: workflow) as manager,
                Server(manager, 'localhost', 8800)):
        print("Server started at ws://localhost:8800")
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
