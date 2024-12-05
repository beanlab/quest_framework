import asyncio
import logging

async def ainput(*args):
    return await asyncio.to_thread(input, *args)

quest_logger = logging.getLogger('quest')