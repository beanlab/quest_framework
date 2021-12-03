import os
import pickle
import random
import re
from pathlib import Path
from typing import TypeVar, Callable, Protocol
import logging

logging.basicConfig(level=logging.INFO)

T = TypeVar('T')
Q = TypeVar('Q')
P = TypeVar('P')

Action = Callable[[], T]


class RetryCheckpoint(Exception):
    pass


class CheckpointFailed(Exception):
    pass


class CacheTool(Protocol):
    def exists(self, name: str) -> bool:
        pass

    def save(self, name: str, payload: P) -> P:
        pass

    def load(self, name: str):
        pass

    def invalidate(self, prefix: str):
        pass


class PickleCacheTool(CacheTool):
    cache_path: Path

    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self.PRESERVE_INVALID_CACHE = False

    def _safe_name(self, name: str) -> str:
        return re.sub(r'\W+', "_", name)

    def _get_cache_path(self, name: str) -> Path:
        return self.cache_path / (self._safe_name(name) + '.pickle')

    def exists(self, name: str) -> bool:
        return self._get_cache_path(name).exists()

    def save(self, name: str, payload: P) -> P:
        self._get_cache_path(name).parent.mkdir(exist_ok=True, parents=True)
        with open(self._get_cache_path(name), 'wb') as f:
            pickle.dump(payload, f)
        return payload

    def load(self, name: str):
        with open(self._get_cache_path(name), 'rb') as f:
            return pickle.load(f)

    def invalidate(self, prefix: str):
        logging.debug(f"Deleting cache for {prefix}")
        parent = self.cache_path if self.cache_path.is_dir() else self.cache_path.parent
        for dir_path, _, files in os.walk(parent):
            for file in files:
                full_path = os.path.join(dir_path, file)
                if full_path.startswith(prefix):
                    if self.PRESERVE_INVALID_CACHE:
                        new_path = full_path.replace(".pickle", ".invalid.pickle")
                        logging.debug(f"Moving {full_path} to {new_path}")
                        os.rename(full_path, new_path)
                    else:
                        logging.debug(f"Deleting {full_path}")
                        os.remove(full_path)


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


def go_on_quest(ch, payload):
    config = ch.do("Load Config", lambda: print("Loading config") or {"config": "stuff"})

    # Make stuff up
    @ch.step("Random")
    def random_number():
        return random.randint(0, 10)

    print(random_number)

    # Get user information
    @ch.quest("User Info")
    def user_info(qch):
        name = qch.do("User Name", lambda: input("Enter name: "))
        number = qch.do("User Number", lambda: int(input("Enter a number: ")))

        if number < 7:
            logging.warning(f"The number {number} is less than 7. Try again!")
            qch.retry()
        else:
            return {"name": name, "number": number}

    print(user_info)

    # TODO - implement the following:

    @ch.quest("Get user feedback")
    def feedback(qch):
        @qch.async_step("Send feedback email")
        def send_email(token):
            # Send an email
            print(f"Sending email containing {token}")

        @qch.wait("Wait for feedback", send_email)
        def user_feedback(response):
            # Wait for user to respond
            return response['feedback']

        return user_feedback

    rch1 = ch.start_quest("Remote task 1")
    task1 = rch1.do("Launch task 1", lambda: print("Launching task 1") or "task-id-123456")

    rch2 = ch.start_quest("Remote task 2")
    task2 = rch2.do("Launch task 2", lambda: print("Launching task 2") or "task-id-abcdef")

    result1 = rch1.do("Wait on task 1", lambda: print(f"Waiting on task {task1}") or {"result": "foobar"})
    rch1.complete_quest(result1)

    result2 = rch2.do("Wait on task 2", lambda: print(f"Waiting on task {task2}") or {"result": "bazquux"})
    rch2.complete_quest(result2)


if __name__ == '__main__':
    go_on_quest(Checkpoints("root", PickleCacheTool(Path("./step_cache"))), {})

# TODO - eventing: how do we wait for an event to come in?
