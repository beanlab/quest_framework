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
    signal_name: str
    args: list[Any]
    kwargs: dict[str, Any]
    callback: Optional[Callable[[RT], CRT]]

    async def join(self) -> CRT:
        # If I have the payload for this signal, return it
        # else raise
        await find_workflow().async_handle_signal(func_or_name, *args, **kwargs)
        raise WorkflowSuspended(signal_name)


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
