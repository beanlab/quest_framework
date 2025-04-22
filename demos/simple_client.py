import asyncio
from quest import Client, ainput

# TODO: This doesn't currently work the way we want it to. The client's resource stream should
#  be closed by the workflow finishing up and closing the resource stream on the server. Instead,
#  without the break statement the client tries to prompt the user for a second time and hangs up.

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
            print('Client received resource event', resources)
            if ('message', None) in resources:
                message = await client.send_event(wid, 'message', None, 'get')
                print(message, end='')
            if ('response', None) in resources:
                user_response = await ainput('prompt: ')  # The text here is to make it clear when the client prompts.
                await client.send_event(wid, 'response', None, 'put', user_response)
                # Uncomment the break statement to see the broken behavior. The client will hang when it should
                #  close automatically after the workflow finishes up
                # break
        print('Stream finished')


if __name__ == '__main__':
    asyncio.run(main())
