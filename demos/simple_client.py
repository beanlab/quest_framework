import asyncio
from quest import Client, ainput

async def render(resources):
    print('Rendering resources')
    if ('message', None) in resources:
        message = resources[('message', None)]
        print(message.get())

async def main():
    wid = 'wid'
    async with Client('ws://localhost:8800', {'Foo': 'Bar'}) as client:
        print('Client starting workflow')
        await client.start_workflow('workflow', wid)
        print('Client started workflow')
        async for resources in client.stream_resources(wid, None):
            print('Client received resource event')
            if ('message', None) in resources:
                message = await client.send_event(wid, 'message', None, 'get')
                print(message)
            if ('response', None) in resources:
                user_response = await ainput()
                await client.send_event(wid, 'response', None, 'put', user_response)


if __name__ == '__main__':
    asyncio.run(main())
