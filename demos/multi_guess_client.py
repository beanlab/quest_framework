import asyncio

from quest import step


# TODO - write a websocket client
# this is essentially the interface of WorkflowManager

@step
async def get_input(*args, **kwargs):
    # TODO - replace this with non-blocking input
    return input(*args, **kwargs)


@step
async def display(*args, **kwargs):
    print(*args, **kwargs)


@step
async def register(registration_queue):
    name = await get_input('Name: ')
    ident, _ = await registration_queue.put(name)
    return ident


@step
async def respond(resources):
    await display('\n\n')

    # If the 'message' state is present, display it
    if 'message' in resources:
        await display(resources['status'].get())

    # If the 'guess' queue is present, provide a guess
    if 'guess' in resources:
        guess = await get_input('Guess: ')
        await resources['guess'].put(guess)


async def main():
    async with client('localhost', '12345') as client:

        # TODO - make sure multiple users can listen on the
        # same identity (e.g. None) and still get all the events

        # TODO - make resource_streams a context
        # so you can close the stream
        # right now, if you stop iterating the stream,
        # the whole system blocks.
        # We need a way for the user to listen to a stream
        # until they are no longer interested.

        async with client.get_resource_stream('demo', None) as resource_stream:
            async for resources in resource_stream:
                # We're expecting an identity queue named "registration"
                if 'registration' in resources:
                    ident = await register(resources['registration'])
                    break  # See TODO above about leaving resource streams

        # Now that we have an established identity
        # we will listen for those resources events
        async for resources in client.get_resource_stream(ident):
            await respond(resources)


if __name__ == '__main__':
    asyncio.run(main())
