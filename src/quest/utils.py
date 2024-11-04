import asyncio

async def ainput(*args):
    return await asyncio.to_thread(input, *args)

