import asyncio
from typing import Optional, Any, Callable, TypeVar

from .workflow import event, signal, promised_signal, any_promise


@promised_signal
async def host_says_game_ready(): ...


@promised_signal(many=True)
async def get_next_player() -> dict:
    """Returns the player {id: <>, name: <>}"""


@event
def get_players() -> dict[str, dict]:
    host_id = gen_player_id()
    players = {host_id: await get_next_player(host_id)}
    game_start_promise = host_says_game_ready(host_id)
    while True:
        promise, result = any_promise(game_start_promise, get_next_player(gen_player_id()))
        if promise is game_start_promise:
            return players
        players[result.playload['id']] = result.playload['name']


# requests.get('https://url.com/<workflow_id>/<player_id>/get-signal
@promised_signal(ids=['player_id'])
async def get_next_player(player_id: str) -> dict:
    """Returns the player {id: <>, name: <>}"""

@promised_signal(ids=['player_id'])
async def notify_player(player_id: str, name: str): ...

@event
async def get_players_host_adds() -> tuple[str, str, dict[str, dict]]:
    host_id = gen_player_id()
    host_name = await get_host_player()
    players = {}
    while True:
        try:
            result = host_adds_player(players)
            player_id = gen_player_id()
            players[player_id] = result['name']
            notify_player(player_id, name)
        except SignalException as se:
            if se.name == 'GameReady':
                return host_id, host_name, players
            else:
                raise


@promised_signal(ids=['player_id'])
async def get_player_guess(player_id: str, feedback: str): ...

async def game():
    host_id, host_name, players = await get_players_host_adds()
    secret = await get_secret_number()
    feedback = 'Initial guess'
    while True:
        promised_guesses = {pid: get_player_guess(pid, feedback) for pid in players}
        promised_guesses[host_id] = get_host_guess(feedback)
        guesses = {pid: promise.join() for pid, promise in promised_guesses.items()}
        closest = await calc_closest(guesses, secret)
        if closest == secret:
            break
        feedback = await make_feedback(guesses, closest)

    for pid in players:
        announce_game_complete(pid, guesses, secret)

    return guesses, secret  # announces to host that game is complete


@promised_event
async def host_join():
    name = await host_join_game()  # scene for the initial player to join game
    while True:
        players = yield name
        if await game_ready(players):  # host indicates enough players have joined
            break
    return name


# https://url.com/join
#  -> WM.start_workflow(player_id, player_name)

@signal_queue
async def player_join() -> list[str]:  #player_id
    player_id = yield
    notify_player_has_joined(player_id)


def game():
    host = host_game()
    host_name = host.send(None)
    player_queue = player_join(host_name)
    players = []
    try:
        while True:
            players.extend(player_queue.accept())
            host.send(players)
    except StopIteration as si:
        pass


    host_name = host.join()
    players = [prom.join() for prom in players]


async def get_feedback(game_id, player_id, guess):
    wm.send_signal(game_id, player_id, guess)
    while True:
        status = wm.status(game_id, player_id)
        if status is not None:
            return status
        await sleep(1)

@thing
async def submit_guess(game_id, player_id, feedback):
    guess = await get_player_guess(feedback)
    feedback = await get_feedback(game_id, player_id, guess)
    return feedback

async def regular_player():
    game_id, player_id = await join_game()
    feedback = await host_starts_game(game_id)
    while True:
        feedback = submit_guess(game_id, player_id, feedback)
        if feedback is None:
            break
    return await get_final_info(game_id)


# Another approach
async def player_worklow():
    """Responsible for defining the sequence of scenes the player encounters"""
    name = await get_name()
    instructions = await wait_for_first_turn(name)
    try:
        while True:
            guess = await provide_guess()
            instructions = await wait_for_turn(guess)
    except SignalException as se:
        if se.name == 'GameComplete':
            return se.game_stats
        else:
            raise


@event
async def wait_for_turn(guess):
    """Provides scene updates to UI while querying parent for info"""
    status = provide_guess(guess)  # this calls the parent

    while status.waiting:
        provide_status_update(status)  # this updates the signal
        status = await get_status()    # this calls the parent

    return status.instructions