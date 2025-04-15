import asyncio
import logging
import random
import shutil
from pathlib import Path

from quest import (step, queue, state, create_filesystem_manager, these, event, identity_queue)
from quest.server import Server
from quest.utils import quest_logger


@step
async def initialize_game(host_ident, host_name) -> dict[str, str]:
    players = {host_ident: host_name}

    async with (event('start_game', host_ident) as start_game,
                identity_queue('registration') as register):
        while True:
            register_task = asyncio.create_task(register.get())
            start_game_task = asyncio.create_task(start_game.wait())

            done, pending = await asyncio.wait(
                [register_task, start_game_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            if start_game_task in done:
                break

            ident, name = done.pop().result()
            players[ident] = name

            for task in pending:
                task.cancel()

    return players


@step
async def get_secret():
    return random.randint(1, 100)


@step
async def get_guesses(players: [str], message) -> dict[str, int]:
    guesses = {}
    status_message = []

    # TODO - the following code sequence is a little verbose
    # We need to:
    # - create a queue for each player
    # - listen on all queues
    # - take one input at a time
    # - have the option to remove a queue (e.g. after getting input)
    # This pattern should be common enough we should make
    # it easy and clear

    async with (
        # Create a guess queue for each player
        these({
            ident: queue('guess', ident)
            for ident in players
        }) as guess_queues
    ):
        # Wait for guesses to come in.
        # As they do, remove their queue so they can't guess again.
        guess_gets = {q.get(): ident for ident, q in guess_queues.items()}
        for guess_get in asyncio.as_completed(guess_gets):
            guess = await guess_get
            # TODO: This is a problem.
            ident = guess_gets[guess]
            guesses[ident] = guess

            # Update the status
            status_message.append(f'{ident} guessed {guess}')
            message.set('\n'.join(status_message))

            # Remove the queue
            # The user will no longer see it
            guess_queues.remove(ident)

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
            message.set(f'{closest_ident} was closest: {guess}')

    return secret
    # TODO - have the return value of the workflow appear
    #  as the final resource in the resource stream
    #  Some design needed (raw value? special resource?)


async def multi_guess(host_ident, host_name):
    quest_logger.debug('Multi-Guess started.')
    players = await initialize_game(host_ident, host_name)
    await play_game(players)


async def main():
    f_path = Path('state')
    shutil.rmtree(f_path, ignore_errors=True)
    async with (
        create_filesystem_manager(f_path, 'multi_guess', lambda wtype: multi_guess) as manager,
        Server(manager, 'localhost', 8765)
    ):
        await asyncio.Future()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
