import asyncio
import logging

async def ainput(*args):
    return await asyncio.to_thread(input, *args)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s : %(levelname)s : %(message)s',
    handlers=[logging.StreamHandler()],
)

quest_logger = logging.getLogger('quest')