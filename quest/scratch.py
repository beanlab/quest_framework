import asyncio
from typing import Optional, Any, Callable, TypeVar

from .workflow import event, signal, promised_signal, any_promise


@promised_signal
async def host_says_game_ready(): ...


@promised_signal
async def get_next_player(): ...


@event
def get_players():
    players = []
    game_start_promise = host_says_game_ready()
    while True:
        promise, result = any_promise(game_start_promise, get_next_player())
        if promise is game_start_promise:
            return players
        players.append(result.payload)
