import asyncio
from quest import Client

async def render(resources):
    if 'message' in resources:
        message = as_state(resources['message'])
        print(message.get())

    if 'response' in resources:


async def main():
    async with Client('localhost', '1234') as client:
        await client.start_workflow('workflow', 'wid')
        async for resources in client.stream_resources('wid', None):
            await render(resources)


if __name__ == '__main__':
    asyncio.run(main())
