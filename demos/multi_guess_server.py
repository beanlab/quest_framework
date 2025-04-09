import asyncio
import random
from pathlib import Path

from quest import (step, queue, state, identity_queue,
                   create_filesystem_manager, these)
from scratch.websocket_scratch.server import serve
from quest.external import Queue
from quest.historian import find_historian


# Multiple response per user
class MultiQueue:
    def __init__(self, queues: dict[str, Queue], single_response: bool = False):
        self.queues = queues
        self.single_response = single_response
        self.task_to_ident: dict[asyncio.Task, str] = {}
        self.ident_to_task: dict[str, asyncio.Task] = {}

    def _add_task(self, ident: str, q: Queue):
        historian = find_historian()
        task = historian.start_task(
            q.get,
            name=f"mq-get-{ident}"
        )

        self.task_to_ident[task] = ident
        self.ident_to_task[ident] = task

    async def __aenter__(self):
        # Listen on all queues -> create a task for each queue.get()
        for ident, q in self.queues.items():
            self._add_task(ident, q)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cancel all pending tasks - context exits
        for task in self.task_to_ident:
            task.cancel()

    def remove(self, ident: str):
        # Stop listening to this identity queue
        if ident not in self.ident_to_task:
            raise KeyError(f"Identity '{ident}' does not exist in MultiQueue.")

        task = self.ident_to_task.pop(ident)
        self.task_to_ident.pop(task)
        task.cancel()

    async def __aiter__(self):
        while self.task_to_ident:
            # Wait until any of the current task is done
            done, _ = await asyncio.wait(self.task_to_ident.keys(), return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                ident = self.task_to_ident.pop(task)
                # Stop listening to this identity
                del self.ident_to_task[ident]

                try:
                    result = await task
                    yield ident, result

                    # Start listening again
                    if not self.single_response:
                        self._add_task(ident, self.queues[ident])

                except asyncio.CancelledError:
                    continue


class SingleResponseMultiQueue:
    def __init__(self, queues: dict[str, Queue]):
        self._mq = MultiQueue(queues, single_response=True)
        self._received: set[str] = set()
        self._total = len(queues)

    async def __aenter__(self):
        await self._mq.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._mq.__aexit__(exc_type, exc_val, exc_tb)

    async def __aiter__(self):
        async for ident, item in self._mq:
            # Skip identities that already gave one response
            if ident in self._received:
                continue
            self._received.add(ident)
            yield ident, item

            if len(self._received) == self._total:
                return


# TODO - write a websocket server that wraps
# an existing workflow manager

@step
async def get_players():
    players = {}

    # The identity_queue is visible to everyone (None identity)
    #  until it is taken down (i.e. we have 3 players)
    # When someone puts data in an identity queue,
    #  that action is fingerprinted with an identity
    #  (i.e. the identity of the user that put the data is established)
    # The `put` command returns that identity
    # The `get` command returns the identity and the `put` value
    # In essence, users are identified by the data they provided
    #  (i.e. when I say ID 12345, I mean "whoever gave me 'John' in the queue")
    async with identity_queue('register') as register:
        while len(players) < 3:
            ident, name = await register.get()
            players[ident] = name
    return players


@step
async def get_secret():
    return random.randint(1, 100)


@step
async def get_guesses(players: dict[str, str], message) -> dict[str, int]:
    guesses = {}
    status_message = []

    queues: dict[str, Queue] = {ident: queue('guess', ident) for ident in players}

    # TODO - the following code sequence is a little verbose
    # We need to:
    # - create a queue for each player
    # - listen on all queues
    # - take one input at a time
    # - have the option to remove a queue (e.g. after getting input)
    # This pattern should be common enough we should make
    # it easy and clear

    # Iterate guesses one at a time
    async with SingleResponseMultiQueue(queues) as mq:
        async for ident, guess in mq:
            guesses[ident] = guess

            # Status message
            name = players[ident]
            status_message.append(f'{name} guessed {guess}')
            message.set('\n'.join(status_message))

    return guesses


@step
async def play_game(players: dict[str, str]):
    secret = await get_secret()

    async with state('message', None, '') as message:
        while True:
            guesses = await get_guesses(players, message)
            closest_ident, guess = min(guesses.items(), key=lambda x: abs(x[1] - secret))
            if guess == secret:
                break
            closest = players[closest_ident]
            message.set(f'{closest} was closest: {guess}')

    return secret
    # TODO - have the return value of the workflow appear
    # as the final resource in the resource stream
    # Some design needed (raw value? special resource?)


async def multi_guess():
    players = await get_players()
    await play_game(players)


# TODO: Rewrite this function to import and use serve from server.py
async def main():
    async with (
        create_filesystem_manager(
            Path('state'),
            'multi_guess',
            lambda wid: multi_guess
        ) as manager,
        serve(
            manager,
            'localhost',
            8765
        ) as server
    ):
        # TODO: Add ability to start workflows to server.py
        # Start the game
        manager.start_workflow('', 'demo')

        # Wait for it to finish
        await manager.get_workflow('demo')
