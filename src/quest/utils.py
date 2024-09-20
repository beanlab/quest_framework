import asyncio


async def ainput(*args):
    return await asyncio.get_event_loop().run_in_executor(None, input, *args)

