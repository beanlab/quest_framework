import asyncio
from quest import state, queue, ainput
from quest.client import Client
from quest.server import Server
from quest_test.utils import create_in_memory_workflow_manager


def authorize(headers: dict[str, str]) -> bool:
    if 'Authorization' not in headers:
        return False
    if headers['Authorization'] == "C@n'tT0uchThis!":
        return True
    return False


async def serve(manager):
    async with Server(manager, 'localhost', 8000, authorizer=authorize):
        await asyncio.sleep(1)


async def connect():
    async with Client('ws://localhost:8000', {'Authorization': "C@n'tT0uchThis!"}) as client:
        messages_seen = False
        async for resources in client.stream_resources('workflow1', None):
            print("Resources:", resources)
            if ('messages', None) in resources and not messages_seen:
                messages_seen = True
                message_to_send = await ainput("Enter a message to send: ")
                response = await client.send_event('workflow1', 'messages', None, 'put', [message_to_send])
                print("Response:", response)



async def main():
    async def workflow():
        async with state('phrase', None, '') as phrase:
            async with queue('messages', None) as messages:
                messages = await messages.get()
                await phrase.set(messages)
                print(f'Phrase: {await phrase.get()}')

    manager = create_in_memory_workflow_manager({'workflow': workflow})
    manager.start_workflow('workflow', 'workflow1')
    await asyncio.gather(serve(manager), connect())

if __name__ == '__main__':
    asyncio.run(main())