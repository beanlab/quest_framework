import asyncio
import random

from .context import these
from .decorators import step, task
from .resources import State, Action, IdentityQueue, Queue
from .workflow import Workflow


@task
async def play(identity: str, target_score: int):
    score_changed = asyncio.Event()
    score_changed.clear()

    score = State(identity, 'score', 0)

    async def increment_score():
        await score.set(await score.get() + 1)
        score_changed.set()

    async with score, Action(identity, 'increment', increment_score), \
            State(identity, 'message', 'Increment your score!') as message:
        while True:
            await score_changed.wait()
            score_changed.clear()
            if await score.get() >= target_score:
                break
            else:
                await message.set('Keep going!')

    return await score.get()


@step
async def get_target_score():
    return random.randint(3, 6)


async def workflow():
    users = {}
    async with (
        State(None, 'message', 'Enter your name'),
        IdentityQueue(None, 'name') as name_queue
    ):
        while len(users) < 3:
            identity, name = await name_queue.get()
            users[identity] = name

    plays = [
        play(ident, await get_target_score())
        for ident in users
    ]
    scores = {
        identity: score
        for identity, score in zip(users, await asyncio.gather(*plays))
    }
    final_message = '\n'.join(
        f'{users[identity]} has {scores[identity]}'
        for identity in users
    )

    async with State(None, 'message', final_message), \
            these({identity: Queue(identity, 'confirm') for identity in users}) as confirm_queues:
        for q in confirm_queues.values():
            await q.get()


async def main():
    async with Workflow('demo', workflow) as work:
        async for resources in work.resource_stream([None]):
            print(resources)
            if any(name == 'name.put' for _, name in resources.actions):
                name = input('Name: ')
                await work.invoke_action(None, 'name.put', name)
            

if __name__ == '__main__':
    from quest.refactor.logs import enable_trace_logging, set_suggested_log_format
    set_suggested_log_format()
    enable_trace_logging()
    asyncio.run(main())
