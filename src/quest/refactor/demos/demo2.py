import asyncio

from quest.refactor.resources import State
from quest.refactor.workflow import Workflow


async def workflow():
    async with State(None, 'message', 'foo'):
        await asyncio.sleep(1)

    async with State(None, 'message', 'bar'):
        await asyncio.sleep(1)

    async with State(None, 'message', 'baz'):
        await asyncio.sleep(1)


async def main():
    async with Workflow('demo', workflow) as work:
        async for resources in work.resource_stream([None]):
            print(resources)


if __name__ == '__main__':
    asyncio.run(main())
