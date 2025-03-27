from quest import Server, create_filesystem_manager, queue, state
from pathlib import Path
import asyncio

async def start_server(manager):
    async with Server(manager, 'localhost', 8000, authorizer=lambda x: True):
        await asyncio.Future()

async def register():
    async with state('phrase', None, '') as phrase:
        phrase.set('Hello world!')
        async with queue('registration_queue', None) as registration:
            message = await registration.get()
            await phrase.set(message)

async def main():
    async with create_filesystem_manager(
        Path('state'),
        'registration',
        lambda wid: register
    ) as manager:
        manager.start_workflow('', 'registration')
        await asyncio.create_task(start_server(manager))
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())
