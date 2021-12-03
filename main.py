import random
from pathlib import Path
import logging

from caching import PickleCacheTool
from checkpointing import Checkpoints

logging.basicConfig(level=logging.INFO)


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
# TODO - tests: basic do, basic step, basic quest, retry quest, interleaved quests (with retry), signals
# TODO - Orchestrator + signals + manual resumes
# TODO - command-line interface
# TODO - switch to JSON payloads (and use json instead of pickle) - use TypedDict for payload types
