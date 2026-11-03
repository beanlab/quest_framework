import asyncio

from quest import State, Workflow


async def workflow():
    async with State(None, 'message', 'foo'):
        await asyncio.sleep(1)

    async with State(None, 'message', 'bar'):
        await asyncio.sleep(1)

    async with State(None, 'message', 'baz'):
        await asyncio.sleep(1)


async def main():
    async with Workflow('demo', workflow) as work:
        async with work.resource_stream([None]) as stream:
            async for resources in stream:
                print(resources)


if __name__ == '__main__':
    asyncio.run(main())
