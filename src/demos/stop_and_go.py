import asyncio

from quest import step, Workflow
from quest.logs import quest_logger


async def main():
    pause = asyncio.Event()

    @step
    async def noisy():
        quest_logger.info('Noisy')

    async def workflow():
        quest_logger.info('Start')
        await noisy()
        quest_logger.info('Before pause')
        await pause.wait()
        quest_logger.info('After pause')
        await noisy()
        quest_logger.info('End')

    storage = []
    async with Workflow('demo', workflow, storage=storage) as work:
        await asyncio.sleep(0.1)  # i.e. yield to the workflow

    # Leave context -> suspend the workflow

    quest_logger.info(storage)

    pause.set()
    async with Workflow('demo', workflow, storage=storage) as work:
        await work


if __name__ == '__main__':
    from quest.logs import enable_debug_logging, set_suggested_log_format, enable_trace_logging

    set_suggested_log_format()
    # enable_debug_logging()
    # enable_trace_logging()
    asyncio.run(main())
