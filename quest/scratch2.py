from typing import Callable


def make_state(state_name: str, **kwargs) -> tuple[Callable, Callable]:
    ...


def game_flow():
    player_scenes = {}
    with make_state('players') as (get_players, set_players), make_state('game_ready') as (
            get_game_ready, set_game_ready):
        if not get_game_ready():
            players = get_players()
            for player in players:
                if player not in player_scenes:
                    player_scenes[player] = make_state('scene', identity=player)[0]
                player_scenes[player]('wait', players)
            return
        players = get_players()

    for player in players:
        if player not in player_scenes:
            player_scenes[player] = make_state('scene', identity=player)[0]

    secret = await get_secret()
    closest = None
    while True:
        for player, set_scene in player_scenes.items():
            set_scene('provide_guess', closest)
        player_guesses = {player: make_state('guess', identity=player, accept=1, required=True)[0] for player in
                          players}
        guesses = {player: get_guess() for player, get_guess in player_guesses.items()}
        closest = calc_closest(guesses, secret)
        if closest == secret:
            break
    for player, set_scene in player_scenes.items():
        set_scene('game_done', secret)


def assign_id():
    return str(uuid.uuid4())


async def game_workflow():
    players = {}
    # All workflow state must be defined local to the workflow function
    # This is so the state is rebuilt each time the workflow runs
    # This ensures no side-effects in state and ensures that each replay of the workflow
    # is identical.

    with state('ready', default=False, writable=True) as ready, \
            queue('join', assign_id=assign_id) as new_player, \
            state('scene') as waiting_scene:
        # With a default, when I call ready() before the state has been supplied
        # the event returns the default value (the default is None).
        # Each call to ready() is evented and recorded in the history with a unique id
        # Before an external source supplies the value, I get the default
        # After the value is supplied, I get that value.
        # When the context exits, that value can no longer be changed (nor is it visible in the status)
        #
        # 'writable' means the external client can post to the state and change its value. Default = False.
        #
        # A queue is another kind of state. External clients can post an item to the queue.
        # When I call new_player, I get the next item in the queue.
        # When there are no items in the queue, the workflow suspends.
        # Each retrieval is evented. The workflow must store the current state of the queue.
        # I only get one item per call to new_player.
        #
        # Note: the state context controls when state endpoints are visible in the workflow status
        # When you enter the context the first time (i.e. not on a replay) the state becomes visible
        # When you exit the context cleanly (i.e. not on a suspend) then the state information is removed
        # from the workflow status.
        # Entering the state on a replay is ignored.
        # Exiting the state via raising a suspended exception is also ignored.
        #
        # If no namespace is provided (to state or queue), then the information goes under the
        # 'global' identifier in the workflow status state section.
        #
        # 'assign_id' is a function that generates a unique ID to be used by the client.
        # When the client posts to the associated feature (state or queue),
        # the ID is immediately returned to the client. The client should include that argument in all future invocations.
        # A client can be associated with multiple IDs and should keep track of all of them for a given workflow.
        # State handlers built with 'assign_id' always return a tuple of (ID, value) instead of just the value
        # so the workflow knows what ID is associated with the value.
        while not await ready():
            await waiting_scene({'name': 'waiting_for_join', 'args': players, 'kwargs': {}})
            # State functions set the value when a value is provided.
            # When no value is provided, they don't change anything.
            # They always return the current value of the state (though the workflow may ignore it)
            player_id, player_name = await new_player()
            players[player_id] = player_name

    secret = await get_secret()  # regular event
    feedback = None
    with {player: state('scene', namespace=player) for player in players} as player_scenes, \
            {player: state('guess', namespace=player) for player in players} as get_guesses:
        # I'm pretty sure I just made up that syntax (with dict).
        # We may need a simple wrapper for giving a dictionary of contexts
        # the ability to be a context. I'm sure the same for lists and tuples will come in handy.
        #
        # 'namespace' means the state is intended for a specific identity. It will be the HTTP layer's job to control this.
        while True:
            for player in players:
                player_scenes[player]({'name': 'provide_guess', 'args': [feedback], 'kwargs': {'responses': ['guess']}})
                # The actual content and structure of args, kwargs, etc. is negotiable

            guesses = {players[player]: await get_guess() for player, get_guess in get_guesses.items()}
            # When you try to get the state and it doesn't exist (no default or value), the workflow suspends
            # When someone sets state, the workflow is triggered and replays
            # The player IDs are secure identifiers and should not be published outside the workflow
            # I use the player names here because the guesses are published back to the players

            closest = min(guesses.items(), key=lambda x: abs(x - secret))
            if closest == secret:
                break
            feedback = {'closest': closest, 'guesses': guesses}

        for player, scene in player_scenes.items():
            scene({'name': 'conclusion', 'args': [secret, guesses], 'kwargs': {}})
            # One problem I see here is that the scene context will exit and 'scene' will no longer be available
            # We probably need a way to return a final, persistent state from the workflow.
            # Or, perhaps we add a parameter to the state function that tells it to persist in read-only mode after
            # the workflow completes (I think I like this idea the most, but I'm open to better options).


# One thing that impresses me about the 'with' contexts controlling state, is that it gives clear temporal control
# over state: when is it visible, editable, etc.
# It makes it clear what information is available and/or mutable at each phase of the workflow.
#
# Another guiding principal: all workflow state must be in the workflow's local namespace (not class state!)
# Thus, at any point in "time", you can see what the workflow's state is. The complete history of that state is
# recorded in the workflow event history. This is valuable.
#
# The workflow manager should enforce identities. There should be a kwarg "identity" in which the caller provides
# a string or iterable of strings. If None it assumes 'global'. Only the state pertaining to that identity is returned.
# And the caller must provide the associated identity in order to post state to the workflow.
# Secure workflows must be a feature from the ground up.
#
# All applications have to keep track of who is allowed to do what and when. A program cannot be any simpler than
# the complexity that defines that information. What is powerful about this proposed syntax is that it is explicitly
# clear in the code: who (identity) is allowed to do what (state/queue) and when (with context).

class NoValue:
    ...


def state(name: str, **kwargs):
    new_state = State(name, **kwargs)
    find_workflow().add_to_state(new_state)
    return new_state


class State:
    def __init__(self, namespace: str, **kwargs):
        # State declaration
        self.value = NoValue
        # Other variables
        self.replayed = False
        self.visible = False
        self.default = True
        self.writeable = False
        self.namespace = "global"
        self.assign_id = None
        # This takes care of the kw options
        self.options = {"default", "writeable", "assign_id", "namespace"}
        for kw_name, kw_value in kwargs.items():
            if kw_name in self.options:
                setattr(self, kw_name, kw_value)
        # handle kw option changes
        if self.default:
            self.value = None

    def __enter__(self):
        if not self.replayed:
            self.visible = True

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.visible = False

    def __call__(self, *args, **kwargs):
        if len(args) >= 1:
            self.set_state(*args, **kwargs)
        return self.get_state()

    def set_state(self, value):
        self.value = value

    def get_state(self):
        return self.value


