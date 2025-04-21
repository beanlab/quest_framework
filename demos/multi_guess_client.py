import asyncio
import logging

from quest import ainput
from quest.client import Client
from quest.utils import quest_logger

WORKFLOW_ID = 'demo'


async def get_input(*args):
    return await ainput(*args)


async def display(*args, **kwargs):
    print(*args, **kwargs)


async def initialize(client: Client):
    while True:
        choice = await get_input('Enter \"c\" to create game or \"j\" to join existing: ')
        match choice:
            case 'c':
                name = await get_input('Enter your name: ')
                ident = await get_input('Enter your ident: ')
                await client.start_workflow('', WORKFLOW_ID, ident, name)
                quest_logger.debug('started workflow')
                await get_input('Press enter when everyone has joined.')
                async for resources in client.stream_resources(WORKFLOW_ID, ident):
                    if ('start_game', ident) in resources:
                        await client.send_event(WORKFLOW_ID, 'start_game', ident, 'set')
                        break
                return ident
            case 'j':
                name = await get_input('Enter your name: ')
                await display('Waiting for game to start...')
                ident = None
                async for resources in client.stream_resources(WORKFLOW_ID, None):
                    if ('register', None) in resources:
                        ident, _ = await client.send_event(WORKFLOW_ID, 'registration', None, 'put', name)
                        break
                return ident
            case _:
                await display('Incorrect input.')


async def register(registration_queue):
    name = await get_input('Name: ')
    ident, _ = await registration_queue.put(name)
    return ident

async def display_messages(client, ident):
    async for resources in client.stream_resources(WORKFLOW_ID, ident):
        if ('message', None) in resources:
            message = await client.send_event(WORKFLOW_ID, 'message', None, 'get')
            await display(message)


async def respond(client: Client, ident):
    async for resources in client.stream_resources(WORKFLOW_ID, ident):
        # If the 'guess' queue is present, provide a guess
        if ('guess', ident) in resources:
            while True:
                guess = await get_input('Guess: ')
                try:
                    guess = int(guess)  # Convert string to integer
                    await client.send_event(WORKFLOW_ID, 'guess', ident, 'put', guess)
                    break
                except ValueError:
                    await display('Please enter a valid integer.')


async def main():
    async with Client('ws://localhost:8765', {}) as client:
        ident = await initialize(client)

        # Now that we have an established identity
        # we will listen for those resources events
        await asyncio.gather(display_messages(client, ident), respond(client, ident))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
