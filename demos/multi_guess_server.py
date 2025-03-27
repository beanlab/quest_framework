import asyncio
import random
from pathlib import Path

from quest import (step, queue, state, identity_queue,
                   create_filesystem_manager, these)
from quest.extras.sockets.server import Server


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
            ident = guess_gets[guess]
            guesses[ident] = guess

            # Update the status
            name = players[ident]
            status_message.append(f'{name} guessed {guess}')
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
            closest = players[closest_ident]
            message.set(f'{closest} was closest: {guess}')

    return secret
    # TODO - have the return value of the workflow appear
    # as the final resource in the resource stream
    # Some design needed (raw value? special resource?)


async def multi_guess():
    players = await get_players()
    await play_game(players)


async def main():
    async with (
        create_filesystem_manager(
            Path('state'),
            'multi_guess',
            lambda wid: multi_guess
        ) as manager,
        Server.serve(
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
