import asyncio

from demos.demo_tools import admin_view
from quest import step, State, IdentityQueue, task, Queue, Workflow
from quest.logs import quest_logger


@step
async def register_two_players():
    users = []
    async with (
        State(None, 'message', 'Enter your name'),
        IdentityQueue(None, 'name') as name_queue
    ):
        while len(users) < 2:
            quest_logger.debug('users %s', users)
            identity, name = await name_queue.get()
            users.append((identity, name))

    quest_logger.debug('Players identified: %s', users)
    return users


@task
@step
async def get_user_play(user):
    async with (
        State(user, 'message', 'Rock, paper, or scissors?'),
        Queue(user, 'action') as play
    ):
        return await play.get()


@step
async def determine_game(name1, name2, play1, play2):
    if play1 == 'rock' and play2 == 'rock':
        print('Tie!')
    elif play1 == 'rock' and play2 == 'paper':
        print(f'{name2} wins!')
    elif play1 == 'rock' and play2 == 'scissors':
        print(f'{name1} wins!')
    elif play1 == 'paper' and play2 == 'rock':
        print(f'{name1} wins!')
    elif play1 == 'paper' and play2 == 'paper':
        print('Tie!')
    elif play1 == 'paper' and play2 == 'scissors':
        print(f'{name2} wins!')
    elif play1 == 'scissors' and play2 == 'rock':
        print(f'{name2} wins!')
    elif play1 == 'scissors' and play2 == 'paper':
        print(f'{name1} wins!')
    elif play1 == 'scissors' and play2 == 'scissors':
        print('Tie!')


@step
async def play_again(user) -> str:
    async with (
        State(user, 'message', 'Play again? [y/n]'),
        Queue(user, 'action') as again
    ):
        choice = await again.get()
        return choice.lower().startswith('y')


async def workflow():
    (user1, name1), (user2, name2) = await register_two_players()

    while True:
        play1, play2 = await asyncio.gather(get_user_play(user1), get_user_play(user2))

        await determine_game(name1, name2, play1, play2)

        if not await play_again(user1):
            break


async def main():
    async with Workflow('rps', workflow) as work:
        await admin_view(work)


if __name__ == '__main__':
    asyncio.run(main())
