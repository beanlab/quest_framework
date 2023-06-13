import asyncio
from typing import Optional, Any, Callable, TypeVar

from .workflow import event, signal


@signal
async def await_new_player(): ...


@signal
async def get_player_name(pid): ...


@event
async def add_player() -> dict:  # player info
    player_id = await await_new_player()
    return await get_player_name(player_id)


@event
async def get_players():
    players = []
    try:
        while True:
            players.append(add_player())
    except GameReadyToStart as game_ready:
        return players


@signal
def get_guess(pid: str, feedback: Optional[str]): ...


RT = TypeVar('RT')
CRT = TypeVar('CRT')


class Promise:
    NO_RESULT = object()

    signal_name: str
    args: list[Any]
    kwargs: dict[str, Any]
    callback: Optional[Callable[[RT], CRT]]

    async def join(self) -> CRT:
        # If I have the payload for this signal, return it
        # else raise
        await find_workflow().async_handle_signal(func_or_name, *args, **kwargs)

    async def try_join(self):
        try:
            return self.join()
        except WorkflowSuspended as ws:
            return Promise.NO_RESULT


async def any_promise(*promises: Promise):
    await find_workflow().async_handle_signals(*promises)


    while True:
        for promise in promises:
            result = promise.try_join()
            if result is not Promise.NO_RESULT:
                return promise, result
        await asyncio.sleep(1)

async def host_wait(afunc):
    host_promise =

@event
def get_players():
    players = []
    game_start_promise = host_says_game_ready()
    while True:
        promise, result = any_promise(game_start_promise, get_next_player())
        if promise is game_start_promise:
            return players
        players.append(result.payload)



def promised_signal(func) -> Callable:
    def new_func(*args, **kwargs):
        return await find_workflow().async_start_signal(func.__name__, *args, **kwargs)
    return new_func

@promised_signal
def get_guess(pid: str, feedback: Optional[str]): ...

@event
def sports_commentary_on_guess(pid: str, guess: str):
    ...
    # compare guess to history
    # provide silly fact about number
    # provide a joke


async def two_player_game():
    players = await get_players()
    secret = await generate_secret_number()
    feedback = None
    while True:
        guess_promises = [
            await get_guess(pid, feedback)
            for pid in players
        ]
        # find the closest guess
        guesses = [await prom.join() for prom in guess_promises]
        feedback = players[0]

        if any(g == secret for g in guesses):
            break



