import asyncio

import pytest

from quest import Workflow, Queue, step, task


async def wait_for(event):
    await asyncio.wait_for(event.wait(), timeout=2)


class Gate:
    """A test-host checkpoint for placing interruption at a known boundary."""

    def __init__(self):
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def wait(self):
        self.reached.set()
        await self.release.wait()


@pytest.mark.asyncio
async def test_managed_task_arguments_and_result():
    # Scenario TK01: a managed task accepts positional/keyword arguments and
    # returns its value to the workflow that awaits it.
    @task
    async def combine(prefix, value=2, *, suffix="!"):
        return f"{prefix}:{value}{suffix}"

    observed = []

    async def workflow():
        # Starting a task returns an awaitable managed task; await its result.
        observed.append(await combine("item", suffix="?"))
        observed.append(await combine("next", 5))

    # Run both task calls in order and await workflow completion before checking
    # the values delivered back to ordinary workflow code.
    async with Workflow("task-tk01", workflow) as running:
        await asyncio.wait_for(running, timeout=2)

    # The first call uses the default value; the second overrides it.
    assert observed == ["item:2?", "next:5!"]


@pytest.mark.asyncio
async def test_managed_tasks_reach_independent_waits_concurrently():
    # Scenario TK02: both tasks reach their own waiting point before either is
    # released, demonstrating actual concurrency rather than sequential calls.
    left_gate, right_gate = Gate(), Gate()
    results = []

    @task
    async def worker(label, gate):
        await gate.wait()
        return label.upper()

    async def workflow():
        left = worker("left", left_gate)
        right = worker("right", right_gate)
        # Wait for independent body-entry signals before releasing either task;
        # this proves both are concurrently waiting rather than merely created.
        await wait_for(left_gate.reached)
        await wait_for(right_gate.reached)
        # Neither task can finish until the test/workflow releases its gate.
        assert not left.done()
        assert not right.done()
        left_gate.release.set()
        right_gate.release.set()
        results.extend([await left, await right])

    # Let workflow code coordinate both children, release them, and await results.
    async with Workflow("task-tk02", workflow) as running:
        await asyncio.wait_for(running, timeout=3)

    # Each handle delivers the result produced by its own worker.
    assert results == ["LEFT", "RIGHT"]


@pytest.mark.asyncio
async def test_partial_managed_task_replay_reconstructs_bodies_and_step_outcomes():
    # Scenario TK03: sibling task bodies reconstruct after interruption. A
    # completed step supplies its recorded result, while an interrupted step
    # starts again and completes; both task results reach their awaiters.
    history = []
    executions = []
    ordinary_runs = []
    incomplete_gate = Gate()

    @step
    async def completed_work(value):
        executions.append(("complete", value))
        return value * 10

    @step
    async def interrupted_work(value):
        executions.append(("retry", value))
        await incomplete_gate.wait()
        return value + 1

    @task
    async def completed_child(value):
        ordinary_runs.append(("completed-child", value))
        return await completed_work(value)

    @task
    async def incomplete_child(value):
        ordinary_runs.append(("incomplete-child", value))
        return await interrupted_work(value)

    received = []
    completed_child_finished = asyncio.Event()

    async def workflow():
        first = completed_child(4)
        second = incomplete_child(8)
        first_result = await first
        # Signal only after awaiting the sibling, proving task completion rather
        # than merely reaching the end of its step body.
        completed_child_finished.set()
        received.append((first_result, await second))

    # First let the workflow receive the completed sibling's result, then wait
    # until the other child's step is blocked; exit with that step still incomplete.
    async with Workflow("task-tk03", workflow, storage=history):
        await wait_for(completed_child_finished)
        await wait_for(incomplete_gate.reached)
    # The first attempt ran each step body once, and the first child was awaited.
    assert completed_child_finished.is_set()
    assert executions == [("complete", 4), ("retry", 8)]

    # Task bodies run again from their beginnings. Replace only the host gate;
    # the authored workflow and its task/step sequence stay unchanged.
    retry_gate = Gate()
    incomplete_gate = retry_gate
    completed_child_finished = asyncio.Event()
    async with Workflow("task-tk03", workflow, storage=history) as resumed:
        # The reconstructed child reaches the fresh gate; releasing it lets the
        # incomplete step finish and both awaited task outcomes reach workflow.
        await wait_for(retry_gate.reached)
        retry_gate.release.set()
        await asyncio.wait_for(resumed, timeout=3)

    # Sibling scheduling is not the contract; each body reconstructs once.
    assert ordinary_runs.count(("completed-child", 4)) == 2
    assert ordinary_runs.count(("incomplete-child", 8)) == 2
    assert len(ordinary_runs) == 4
    assert executions == [("complete", 4), ("retry", 8), ("retry", 8)]
    assert received == [(40, 9)]


@pytest.mark.asyncio
async def test_controlled_task_interleaving_replays_queue_operation_order():
    # Scenario TK04: task operations on a queue have deliberately controlled
    # ordering. Replay uses the same recorded task/queue creation sequence but
    # reverses host release order; public Quest replay must retain operation
    # order from the original execution.
    history = []
    observed = []

    def fresh_events():
        return {item: asyncio.Event() for item in ("one", "two")}

    ready, go, attempted, put_done = (
        fresh_events(), fresh_events(), fresh_events(), fresh_events()
    )
    gate = Gate()

    @task
    async def producer(queue, item):
        ready[item].set()
        await go[item].wait()
        attempted[item].set()
        await queue.put(item)
        put_done[item].set()

    @task
    async def consumer(queue):
        return await queue.get()

    async def workflow():
        # Resource and task creation follow exactly the same path on both runs.
        async with Queue(None, "ordered") as queue:
            first = consumer(queue)
            second = consumer(queue)
            p1 = producer(queue, "one")
            p2 = producer(queue, "two")
            observed.extend([await first, await second])
            await p1
            await p2
            await gate.wait()

    # Both task bodies must start before either put: replay records task starts
    # too, so creation order alone cannot put producer two before producer one's put.
    async with Workflow("task-tk04", workflow, storage=history):
        await wait_for(ready["one"])
        await wait_for(ready["two"])
        go["one"].set()
        await wait_for(put_done["one"])
        go["two"].set()
        await wait_for(gate.reached)

    # Replace host events as well as the queue reconstructed by workflow code.
    # Replaying under the same workflow path allows only host release timing to vary.
    ready, go, attempted, put_done = (
        fresh_events(), fresh_events(), fresh_events(), fresh_events()
    )
    gate = Gate()
    async with Workflow("task-tk04", workflow, storage=history) as resumed:
        # Replay both recorded task starts before varying releases; otherwise
        # producer two cannot signal an attempted put before producer one's put.
        await wait_for(ready["one"])
        await wait_for(ready["two"])
        # Attempt the second put first. Do NOT wait for it to complete before
        # releasing the first: replay must hold it behind the recorded first put.
        go["two"].set()
        await wait_for(attempted["two"])
        go["one"].set()
        await wait_for(gate.reached)
        gate.release.set()
        await asyncio.wait_for(resumed, timeout=3)
    # Changed host scheduling must not change which consumer receives each item;
    # the two pairs are observations from the initial run and reconstructed run.
    assert observed == ["one", "two", "one", "two"]


@pytest.mark.asyncio
async def test_nested_managed_task_tree_and_repeated_function_calls_replay():
    # Scenario TK05: a task starts and awaits repeated child tasks using the
    # same function; each result remains associated with its own invocation.
    history = []
    bodies = []
    gate = Gate()
    branch_results = []
    branch_results_received = []

    @task
    async def leaf(value):
        bodies.append(("leaf", value))
        return value * 2

    @task
    async def branch(values):
        bodies.append(("branch", tuple(values)))
        results = [await leaf(value) for value in values]
        branch_results.append(tuple(results))
        await gate.wait()
        return results

    async def workflow():
        # Capture the branch's returned value at its actual workflow awaiter.
        branch_results_received.append(await branch([3, 7]))

    # Let the branch and both leaf tasks finish before suspending inside the
    # branch; the parent workflow has not yet received its returned value.
    async with Workflow("task-tk05", workflow, storage=history):
        await wait_for(gate.reached)

    # Reconstruct with the same task call tree but a fresh host checkpoint.
    replay_gate = Gate()
    gate = replay_gate
    async with Workflow("task-tk05", workflow, storage=history) as resumed:
        await wait_for(replay_gate.reached)
        replay_gate.release.set()
        await asyncio.wait_for(resumed, timeout=3)

    # Check both the value computed inside each reconstructed branch and the
    # value received by the workflow awaiting that branch.
    assert branch_results == [(6, 14), (6, 14)]
    # The first parent execution is interrupted inside branch; its reconstructed
    # workflow receives the completed result once during replay.
    assert branch_results_received == [[6, 14]]
    # Task bodies reconstruct; this test makes no task-result caching claim.
    assert bodies == [("branch", (3, 7)), ("leaf", 3), ("leaf", 7)] * 2


@pytest.mark.asyncio
async def test_awaited_task_exception_is_caught_and_replayed_from_step():
    # Scenario TK06: a child task's recorded step exception is replayed, caught
    # by its awaiting parent, and permits workflow continuation. The task body
    # reconstructs, but the completed exceptional step body does not rerun.
    history = []
    attempts = []
    handled = []
    gate = Gate()

    @step
    async def fail():
        attempts.append("step-body")
        raise ValueError("task failure", 23)

    @task
    async def child():
        handled.append("task-body")
        return await fail()

    async def workflow():
        try:
            await child()
        except ValueError as error:
            handled.append((type(error), error.args))
        await gate.wait()
        handled.append("continued")

    # Run the failing step through its handler, then suspend after the caught error.
    async with Workflow("task-tk06-caught", workflow, storage=history):
        await wait_for(gate.reached)

    # Replay reconstructs child code and handler flow; the completed step outcome
    # supplies the exception without executing the step body again.
    replay_gate = Gate()
    gate = replay_gate
    async with Workflow("task-tk06-caught", workflow, storage=history) as resumed:
        await wait_for(replay_gate.reached)
        replay_gate.release.set()
        await asyncio.wait_for(resumed, timeout=3)

    assert attempts == ["step-body"]
    assert handled == [
        "task-body", (ValueError, ("task failure", 23)),
        "task-body", (ValueError, ("task failure", 23)), "continued",
    ]


@pytest.mark.asyncio
async def test_unhandled_awaited_task_exception_fails_workflow():
    # Scenario TK06: when the awaiting workflow does not handle a task error,
    # that exception propagates out and fails the workflow.
    @task
    async def child():
        raise LookupError("unhandled child")

    async def workflow():
        await child()

    # With no handler in workflow code, awaiting the child propagates its error
    # through the public workflow task.
    async with Workflow("task-tk06-unhandled", workflow) as running:
        with pytest.raises(LookupError, match="unhandled child"):
            await asyncio.wait_for(running, timeout=2)


@pytest.mark.asyncio
async def test_workflow_signals_and_awaits_managed_children_before_returning():
    # Scenario TK08: workflow code owns child lifetimes, signals each child to
    # finish, and awaits them before returning rather than relying on detached
    # task durability or implicit cancellation.
    finish = asyncio.Event()
    started = [asyncio.Event(), asyncio.Event()]
    finished = []

    @task
    async def child(number):
        started[number - 1].set()
        await finish.wait()
        finished.append(number)
        return number

    async def workflow():
        children = [child(1), child(2)]
        await wait_for(started[0])
        await wait_for(started[1])
        finish.set()
        return [await task_handle for task_handle in children]

    result_in_workflow = []

    async def capturing_workflow():
        result_in_workflow.append(await workflow())

    # The workflow signals both started children, releases them, and awaits
    # each handle before its own run is allowed to complete.
    async with Workflow("task-tk08", capturing_workflow) as running:
        await asyncio.wait_for(running, timeout=3)

    # Both managed bodies finished and the workflow received both return values.
    assert sorted(finished) == [1, 2]
    assert result_in_workflow == [[1, 2]]
