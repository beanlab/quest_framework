import asyncio
import random

from quest import step, task, action, State, IdentityQueue, Workflow, Resource, IDENTITY


class Score(Resource):
    def __init__(self, identity: IDENTITY):
        super().__init__(identity, 'Score')
        self._score = State(identity, 'score', 0)
        self._score_changed = asyncio.Event()
        self._score_changed.clear()

    @action
    async def increment(self):
        await self._score.set(await self._score.get() + 1)
        self._score_changed.set()

    async def get(self):
        return await self._score.get()

    def __await__(self):
        waiter = self._score_changed.wait().__await__()
        self._score_changed.clear()
        return waiter


@task
async def play(identity: str, target_score: int):
    quest_logger.debug('Starting play %s %s', identity, target_score)

    async with (
        Score(identity) as score,
        State(identity, 'message', 'Increment your score!') as message
    ):
        while True:
            await score
            if await score.get() >= target_score:
                break
            else:
                await message.set('Keep going!')

        return await score.get()


@step
async def get_target_score():
    return random.randint(3, 6)


class Confirmation(Resource):
    def __init__(self, identity):
        super().__init__(identity, 'Confirm')
        self._event = asyncio.Event()

    @action
    async def confirm(self):
        self._event.set()

    @action(external=False)
    async def wait(self):
        await self._event.wait()


@task
async def get_confirmation(identity, message):
    async with (
        State(identity, 'message', message),
        Confirmation(identity) as confirmation
    ):
        await confirmation.wait()


async def workflow():
    users = {}
    async with (
        State(None, 'message', 'Enter your name'),
        IdentityQueue(None, 'name') as name_queue
    ):
        while len(users) < 3:
            quest_logger.debug('users %s', users)
            identity, name = await name_queue.get()
            users[identity] = name

    quest_logger.debug('Players identified: %s', users)

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

    await asyncio.gather(*[
        get_confirmation(ident, final_message)
        for ident in users
    ])
    

class NewIdentity(BaseException):
    def __init__(self, ident):
        self.identity = ident
        super().__init__(ident)


async def main():
    async with Workflow('demo', workflow) as work:
        identities = [None]
        while True:
            try:
                async with work.resource_stream(identities) as stream:
                    async for resources in stream:
                        print('STATE')
                        for ident, name, value in resources.state:
                            print(ident, name, value)
                        print()

                        print('ACTIONS')
                        for index, (ident, name) in enumerate(resources.actions):
                            print(index, ident, name)
                        print()

                        choice = input('Action: ')
                        if not choice:
                            # quest_logger.debug('Next')
                            continue
                        index, *args = choice.split()
                        ident, name = resources.actions[int(index)]
                        result = await work.invoke_action(ident, name, *args)
                        if name == 'name.put' and result not in identities:  # identity queue
                            raise NewIdentity(result)
                break

            except NewIdentity as new_id:
                identities.append(new_id.identity)


if __name__ == '__main__':
    from quest.logs import enable_trace_logging, enable_debug_logging, set_suggested_log_format, quest_logger

    set_suggested_log_format()
    # enable_debug_logging()
    # enable_trace_logging()
    asyncio.run(main())
