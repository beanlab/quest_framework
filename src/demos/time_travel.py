import asyncio
import logging

from quest import Workflow, when, get_current_timestamp, step
from quest.logs import quest_logger, set_suggested_log_format


def assert_monotonic_timestamps(history):
    for prev, nxt in zip(history[:-1], history[1:]):
        assert prev['timestamp'] <= nxt['timestamp'], (prev, nxt)


@step
async def do_some_work(msg: str):
    # Why do we need this function?
    # The when() timestamps are anchored to quest events, like steps, tasks, and resource creation/action.
    # So, new workflow logic must
    quest_logger.info('DOING WORK %s', msg)


async def main():
    first_pause = asyncio.Event()
    second_pause = asyncio.Event()

    # A workflow is written
    async def workflow():
        quest_logger.info('A')
        await do_some_work('A')

        await first_pause.wait()

        quest_logger.info('B')
        await do_some_work('B')

        await second_pause.wait()

        quest_logger.info('C')
        await do_some_work('C')

    # Run the workflow
    first_history = []
    async with Workflow('A', workflow, storage=first_history):
        await asyncio.sleep(0.1)
        # This workflow run made it to the first pause

    first_pause.set()

    second_history = []
    async with Workflow('AB', workflow, storage=second_history):
        await asyncio.sleep(0.1)
        # And this workflow run made it to the second pause

    # And now suspends because we are rebooting with a new version of the code
    new_version_released = get_current_timestamp()
    quest_logger.info('New version of workflow released %s', new_version_released)
    await asyncio.sleep(0.1)  # give enough time for the timestamps to actually be different...

    second_pause.set()

    async def workflow():
        # this is the new version of the code that we are now deploying
        quest_logger.info('A')
        await do_some_work('A')
        await first_pause.wait()

        if when() < new_version_released:
            # i.e. if right now happened before the new code
            # then we should run the old code
            quest_logger.info('B')
            await do_some_work('B')
        else:
            # We run the new version of the code
            quest_logger.info('NEW B')
            await do_some_work('NEW B')

        await second_pause.wait()

        if when() < new_version_released:
            # i.e. if right now happened before the new code
            # then we should run the old code
            quest_logger.info('C')
            await do_some_work('C')

        else:
            # We run the new version of the code
            quest_logger.info('NEW C')
            await do_some_work('NEW C')

    # We now resume the workflows to let them finish
    async with Workflow('A', workflow, storage=first_history) as work:
        await work

    async with Workflow('AB', workflow, storage=second_history) as work:
        await work


if __name__ == '__main__':
    set_suggested_log_format()

    quest_logger.setLevel(logging.INFO)
    # quest_logger.setLevel(logging.DEBUG)
    asyncio.run(main())
