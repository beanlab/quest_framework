import asyncio
import logging
import random

from quest import WorkflowManager, step
from quest.core.serializer import NoOpSerializer
from quest.logs import quest_logger
from quest.persistence import InMemoryBlobStorage, PersistentHistory


def build_workflow_manager(namespace: str, workflows: dict, storage=None, histories=None, serializer=None) -> WorkflowManager:
    storage = InMemoryBlobStorage() if storage is None else storage
    histories = {} if histories is None else histories

    def create_history(wid: str):
        if wid not in histories:
            histories[wid] = PersistentHistory(wid, storage)
        return histories[wid]

    def create_workflow(wtype: str):
        return workflows[wtype]

    if serializer is None:
        serializer = NoOpSerializer()

    return WorkflowManager(namespace, storage, create_history, create_workflow, serializer)


@step
async def randnum():
    return random.randint(1, 10)


@step
async def display(wid: str, *msg):
    print(wid, *msg)


async def main():
    pause = asyncio.Event()

    async def workflow_a(wid: str):
        a = await randnum()
        await display(wid, 'a', a)
        b = await randnum()
        await display(wid, 'b', b)
        await pause.wait()
        await display(wid, 'a+b', a + b)

    async def workflow_b(wid: str):
        a = await randnum()
        await display(wid, 'a', a)
        await pause.wait()
        b = await randnum()
        await display(wid, 'b', b)
        await display(wid, 'a+b', a + b)

    workflows = {
        'wa': workflow_a,
        'wb': workflow_b
    }

    storage = InMemoryBlobStorage()
    histories = {}

    manager = build_workflow_manager('demo', workflows=workflows, storage=storage, histories=histories)
    async with manager:
        await manager.start_workflow('wa', 'wa1', 'wa1')
        await manager.start_workflow('wb', 'wb1', 'wb1')
        await manager.start_workflow('wa', 'wa2', 'wa2')
        await asyncio.sleep(0.1)  # i.e. yield to give the workflows a chance to run

    # The server restarts
    print('RESTART')
    pause.set()

    async with manager:
        await asyncio.sleep(0.1)  # i.e. yield to give the workflows a chance to run


if __name__ == '__main__':
    from quest.logs import set_suggested_log_format, enable_debug_logging, enable_trace_logging

    set_suggested_log_format()
    # enable_debug_logging()
    # enable_trace_logging()
    asyncio.run(main())
