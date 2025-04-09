import asyncio
from pathlib import Path

from quest import Server, create_filesystem_manager, state, queue

def authorizer(headers: dict[str, str]) -> bool:
    return True

async def workflow():
    async with state('message', None, 'Test') as message:
        while True:
            print('Setting message')
            await message.set('Enter a number: ')
            print('State set')

            async with queue('response', None) as response:
                resp = await response.get()
                print('Response received: ' + str(resp))

            print('Setting message with user response')
            await message.set('You said: ' + resp)



async def main():
    state_folder = Path('state')
    async with create_filesystem_manager(state_folder, 'simple', lambda wid: workflow) as manager, Server(manager, 'localhost', 8800, authorizer):
        print("Server started at ws://localhost:8800")
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
