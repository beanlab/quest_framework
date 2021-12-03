import logging
from typing import TypeVar, Callable

from caching import CacheTool, RetryCheckpoint, CheckpointFailed, Q

T = TypeVar('T')
Action = Callable[[], T]


class Checkpoints:
    namespace: str
    cache_tool: CacheTool

    def __init__(self, namespace, cache_tool: CacheTool):
        self.namespace = namespace
        self.cache_tool = cache_tool
        self.MAX_ATTEMPTS = 3

    def _scope_name(self, name: str) -> str:
        return f"{self.namespace} - {name}"

    def _do_cached_action(self, name: str, action: Action) -> T:
        try:
            logging.debug(f"Evaluating step {self._scope_name(name)}")
            if self.cache_tool.exists(self._scope_name(name)):
                logging.debug(f"Loading step {self._scope_name(name)}")
                return self.cache_tool.load(self._scope_name(name))
            else:
                logging.debug(f"Running step {self._scope_name(name)}")
                return self.cache_tool.save(self._scope_name(name), action())

        except Exception as ex:
            logging.warning(f"Step {self._scope_name(name)} failed with {ex}")
            raise ex

    def _wrap_in_retry(self, name: str, action: Action) -> T:
        for attempt in range(self.MAX_ATTEMPTS):
            try:
                return action()

            except RetryCheckpoint as retry:
                logging.debug(f"Will retry {self._scope_name(name)} (attempt {attempt}) because of {retry}")

        raise CheckpointFailed(f"{self._scope_name(name)} exceeded max attempts")

    def do(self, name: str, action: Action) -> T:
        try:
            return self._do_cached_action(name, action)
        except RetryCheckpoint as retry:
            logging.error(f"Checkpoints.retry() should not be called in a 'do' or 'step' action: {retry}")
            raise Exception("Invalid use of Checkpoints.retry", retry)

    def step(self, name):
        def step_dec(action: Action):
            """Invoke the action and return the result"""
            return self._do_cached_action(name, action)

        return step_dec

    def quest(self, name):
        def quest_dec(quest_action: Callable[[Checkpoints], Q]):
            """Invoke the action and return the result"""
            return self._do_cached_action(name, lambda: self._wrap_in_retry(
                name,
                lambda: quest_action(Checkpoints(self._scope_name(name), self.cache_tool))
            ))

        return quest_dec

    def retry(self):
        logging.debug(f"Retry requested for {self.namespace}")
        self.cache_tool.invalidate(self.namespace)
        raise RetryCheckpoint(self.namespace)