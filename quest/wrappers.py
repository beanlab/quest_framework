import inspect
from functools import wraps

from quest.workflow import Workflow, WorkflowSuspended


class WorkflowNotFoundException(Exception):
    pass


def _find_workflow() -> Workflow:
    outer_frame = inspect.currentframe()
    is_workflow = False
    while not is_workflow:
        outer_frame = outer_frame.f_back
        if outer_frame is None:
            raise WorkflowNotFoundException("Workflow object not found in event stack")
        is_workflow = isinstance(outer_frame.f_locals.get('self'), Workflow)
    return outer_frame.f_locals.get('self')


class SetState:
    def __init__(self):
        self.state_id = None

    async def async_init(self, name: str, initial_value, identity):
        self.state_id = await _find_workflow().create_state(name, initial_value, identity)
        return self

    async def __call__(self, value):
        # Make invoke state also act like an event
        return await _find_workflow().set_state(self.state_id, value)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not isinstance(exc_type, WorkflowSuspended):
            await _find_workflow().remove_state(self.state_id)


async def state(name, initial_value=None, identity=None) -> SetState:
    return await SetState().async_init(name, initial_value, identity)


class Queue:
    def __init__(self):
        self.queue_id = None

    async def _async_init(self, name: str, *args, **kwargs) -> 'Queue':
        self.queue_id = await _find_workflow().create_queue(name, *args, **kwargs)
        return self

    async def check(self):
        return await _find_workflow().check_queue(self.queue_id)

    async def pop(self):
        return await _find_workflow().pop_queue(self.queue_id)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if isinstance(exc_type, WorkflowSuspended):
            await _find_workflow().remove_queue(self.queue_id)


async def queue(name, identity=None) -> Queue:
    return await Queue()._async_init(name, identity)


def step(func):
    @wraps(func)
    async def new_func(*args, **kwargs):
        return await _find_workflow().handle_step(func.__name__, func, *args, **kwargs)

    return new_func
