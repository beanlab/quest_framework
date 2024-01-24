import random
import uuid
from functools import wraps
from typing import TypedDict, Any, Callable


class SetState:
    def __init__(self, name: str, initial_value, identity):
        self.state_id = find_workflow().create_state(name, initial_value, identity)

    def __call__(self, value):
        # Make invoke state also act like an event
        return find_workflow().set_state(self.state_id, value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not isinstance(exc_type, WorkflowSuspended):
            find_workflow().remove_state(self.state_id)


def state(name, initial_value=None, identity=None) -> SetState:
    return SetState(name, initial_value, identity)


class StepEntry(TypedDict):
    step_id: str
    name: str
    value: Any


class StateEntry(TypedDict):
    state_id: str
    name: str
    value: Any
    identity: str


class QueueEntry(TypedDict):
    queue_id: str
    name: str
    values: list
    identity: str
    assign_identity: bool


def _step(func):
    @wraps(func)
    def new_func(self, *args, **kwargs):
        return await self.handle_step(func.__name__, func, *args, **kwargs)

    return new_func


class Workflow:
    state: EventManager[StateEntry] #  dict[str, StateEntry]
    queues: EventManager[QueueEntry] # dict[str, QueueEntry]
    steps: EventManager[StepEntry] # dict[str, EventEntry]
    unique_ids: dict[str, UniqueEvent]

    # TODO: rename UniqueEvent to UniqueId, etc.

    def _get_unique_id(self, event_name: str) -> str:
        prefixed_name = self._get_prefixed_name(event_name)
        if prefixed_name not in self.unique_ids:
            self.unique_ids[prefixed_name] = UniqueEvent(prefixed_name)
            self._replay_events.append(self.unique_ids[prefixed_name])
        return next(self.unique_ids[prefixed_name])

    async def handle_step(self, step_name: str, func: Callable, *args, **kwargs):
        """This is called by the @event decorator"""
        step_id = self._get_unique_id(step_name)

        if step_id in self.steps:
            return self.steps[step_id]['value']
        else:
            self.prefix.append(step_name)
            payload = await func(*args, **kwargs)
            self.prefix.pop(-1)
            self.steps[step_id] = StepEntry(
                step_id=step_id,
                name=step_name,
                value=payload
            )
            return payload

    @_step
    def create_state(self, name, initial_value, identity):
        state_id = self._get_unique_id(name)
        self.state[state_id] = StateEntry(
            state_id=state_id,
            name=name,
            value=initial_value,
            identity=identity
        )
        return state_id

    @_step
    def remove_state(self, state_id):
        del self.state[state_id]

    def get_state(self, state_id):
        # Called by workflow manager
        return self.state[state_id]['value']

    @_step
    def set_state(self, state_id, value):
        self.state[state_id]['value'] = value

    @_step
    def create_queue(self, name, identity, assign_identity):
        queue_id = self._get_unique_id(name)
        self.queues[queue_id] = QueueEntry(
            queue_id=queue_id,
            name=name,
            values=[],
            identity=identity,
            assign_identity=assign_identity
        )
        return queue_id

    @_step
    async def push_queue(self, queue_id, value) -> None | str:
        # Called by Workflow Manager
        self.queues[queue_id]['values'].append(value)
        identity = str(uuid.uuid4()) if self.queues[queue_id]['assign_identity'] else None
        return identity

    @_step
    def pop_queue(self, queue_id):
        if self.queues[queue_id]['values']:
            return self.queues[queue_id]['values'].pop(0)
        else:
            raise WorkflowSuspended(...)

    @_step
    def check_queue(self, queue_id) -> bool:
        return bool(self.queues[queue_id]['values'])

    @_step
    def remove_queue(self, queue_id):
        del self.queues[queue_id]

    def start(self, *args, **kwargs):
        args = self.handle_step('args', lambda: args)
        kwargs = self.handle_step('kwargs', lambda: kwargs)
        result = await self._func(*args, **kwargs)
        self.create_state('return', result)
        return result


def assign_id():
    return str(uuid.uuid4())

class Queue:
    def __init__(self, name: str, *args, **kwargs):
        self.queue_id = find_workflow().create_queue(name, *args, **kwargs)

    def check(self):
        return find_workflow().check_queue(self.queue_id)

    def pop(self):
        return find_workflow().pop_queue(self.queue_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(exc_type, WorkflowSuspended):
            find_workflow().remove_queue(self.queue_id)


def queue(name, identity=None, assign_identity=None) -> Queue:
    return Queue(name, identity, assign_identity)


def scene(name, *queues):
    return these(scene('scene', name), *queues)


async def game_workflow():
    players = {}

    with queue('ready') as ready, \
            queue('join', assign_identity=True) as new_player, \
            state('waiting_for_players') as waiting_scene:

        while not await ready.check():
            await waiting_scene({'players': players})
            player_id, player_name = await new_player.pop()
            players[player_id] = player_name

    secret = await step(lambda: random.randint(1, 100))

    feedback = None
    with cdict({player: state('scene', 'guess', identity=player) for player in players}) as player_scenes, \
            cdict({player: queue('guess', identity=player) for player in players}) as get_guesses:

        while True:
            for player in players:
                player_scenes[player](feedback)

            guesses = {players[player]: await get_guess.pop() for player, get_guess in get_guesses.items()}

            closest = min(guesses.items(), key=lambda x: abs(x - secret))
            if closest == secret:
                break
            feedback = {'closest': closest, 'guesses': guesses}

        return {'game_over': True, 'secret': secret, 'closest': closest, 'guesses': guesses}
