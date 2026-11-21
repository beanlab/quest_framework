import asyncio

import pytest

from quest import Workflow, step


class Gate:
    """A test-host checkpoint that makes interruption deterministic."""

    def __init__(self):
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def wait(self):
        self.reached.set()
        await self.release.wait()


async def wait_for_gate(gate):
    await asyncio.wait_for(gate.reached.wait(), timeout=2)


@pytest.mark.asyncio
async def test_workflow_task_raises_cancelled_error_after_context_exit():
    # Scenario WF03: leaving the context suspends unfinished execution, and
    # awaiting its public task handle afterward raises CancelledError.
    gate = Gate()

    async def workflow():
        await gate.wait()

    # Start execution and wait until the workflow is blocked at the checkpoint.
    async with Workflow("step-suspension", workflow) as running:
        await wait_for_gate(gate)
        # Retain the public handle so its outcome can be checked after exit.
        workflow_task = running.task

    # Context exit requests suspension without releasing the checkpoint.
    # Wait without cancelling the task on timeout, so only context exit can
    # account for the CancelledError asserted below.
    _, pending = await asyncio.wait({workflow_task}, timeout=2)
    assert not pending, "Workflow task did not finish after context exit"
    # The finished task reports cancellation, not a normal workflow result.
    with pytest.raises(asyncio.CancelledError):
        await workflow_task


@pytest.mark.asyncio
async def test_step_calls_accept_arguments_defaults_and_values():
    # Scenario ST01: ordinary step calls forward positional/keyword arguments,
    # honor defaults, and return representative Python values.
    @step
    async def assemble(label, count=2, *, metadata=None):
        return {"label": label, "count": count, "metadata": metadata}

    @step
    async def no_arguments():
        return None

    observed = []

    async def workflow():
        # Pass a positional label and keyword metadata, leaving count at its
        # default. Also call a step with no arguments and a None result.
        observed.append((
            await assemble("item", metadata={"tags": ["a", {"enabled": True}]}),
            await no_arguments(),
        ))

    # Let both calls complete in a real workflow; no replay is needed here.
    async with Workflow("step-st01", workflow) as running:
        await asyncio.wait_for(running, timeout=2)
    # Check the values received by workflow code, including the default count
    # and the unchanged nested metadata.
    assert observed == [(
        {
            "label": "item",
            "count": 2,
            "metadata": {"tags": ["a", {"enabled": True}]},
        },
        None,
    )]


@pytest.mark.asyncio
async def test_completed_nested_step_is_not_repeated_after_suspension():
    # Scenario ST02 (including ST05): suspend after a completed outer step that
    # contains a completed child; reopening the same history skips both bodies.
    # Keep the recorded history across runs. Track body executions separately
    # from the values observed by ordinary workflow code.
    history = []
    effects = []
    gate = Gate()

    @step
    async def child(value):
        # This marker appears only when the child body actually executes.
        effects.append("child")
        return value * 3

    @step
    async def outer(value):
        # The first run executes the outer body, then its nested child step.
        effects.append("outer")
        return await child(value)

    observed = []

    async def workflow():
        # Unlike a completed step body, this ordinary code runs again on replay.
        observed.append(await outer(7))
        # Pause after the outer step has completed and its result is recorded.
        await gate.wait()

    # Run until both steps have completed and the workflow is waiting at the gate.
    async with Workflow("step-st02", workflow, storage=history):
        await wait_for_gate(gate)
    # Leaving the context requests suspension; reuse the history directly.
    assert effects == ["outer", "child"]

    # Supply a fresh gate for the next execution without changing the workflow's
    # recorded control flow. The original gate remains unreleased.
    replay_gate = Gate()
    gate = replay_gate
    # A fresh Workflow reconstructs execution using the original recorded history.
    async with Workflow("step-st02", workflow, storage=history) as resumed:
        # Replay supplies outer's recorded result (21), skipping its entire body
        # and therefore the nested child call, before reaching the new gate.
        await wait_for_gate(replay_gate)
        # Allow the reconstructed workflow to finish normally this time.
        replay_gate.release.set()
        await asyncio.wait_for(resumed, timeout=2)
    # Ordinary workflow code observed the same result once per execution.
    assert observed == [21, 21]
    # Neither completed step body executed a second time.
    assert effects == ["outer", "child"]


@pytest.mark.asyncio
async def test_incomplete_step_starts_again_from_its_body():
    # Scenario ST03: interruption at an await inside a step leaves it incomplete,
    # so replay begins the step body again and completes the new attempt.
    history = []
    attempts = []
    gate = Gate()

    @step
    async def work():
        # Record entry before blocking, so a retry is distinguishable from
        # resuming at the await or restoring an already completed result.
        attempts.append("started")
        await gate.wait()
        return len(attempts)

    observed = []

    async def workflow():
        observed.append(await work())

    # Interrupt the first attempt inside work(), before it can return a result.
    async with Workflow("step-st03", workflow, storage=history):
        await wait_for_gate(gate)
    assert attempts == ["started"]

    # Give the next attempt a fresh checkpoint and reuse the recorded history.
    replay_gate = Gate()
    gate = replay_gate
    async with Workflow("step-st03", workflow, storage=history) as resumed:
        # The incomplete step must enter its body again to reach this gate.
        await wait_for_gate(replay_gate)
        # This time release the step and let the workflow finish.
        replay_gate.release.set()
        await asyncio.wait_for(resumed, timeout=2)
    # Only the second attempt returned; its result reflects two body entries.
    assert observed == [2]
    assert attempts == ["started", "started"]


@pytest.mark.asyncio
async def test_incomplete_outer_step_replays_only_incomplete_child():
    # Scenario ST04: the outer step is unfinished, while one child is complete
    # and the next is interrupted; replay skips the first child and retries the second.
    history = []
    effects = []
    gate = Gate()

    @step
    async def completed_child():
        effects.append("completed")
        return "kept"

    @step
    async def pending_child():
        effects.append("pending")
        await gate.wait()
        return "done"

    @step
    async def outer():
        # Finish the first child before entering the second child's checkpoint.
        # The outer step itself cannot complete until both children return.
        effects.append("outer")
        first = await completed_child()
        second = await pending_child()
        return first, second

    observed = []

    async def workflow():
        observed.append(await outer())

    # Suspend with completed_child recorded, but pending_child and outer unfinished.
    async with Workflow("step-st04", workflow, storage=history):
        await wait_for_gate(gate)
    assert effects == ["outer", "completed", "pending"]

    # Reconstruct using the same history and a fresh checkpoint for the retry.
    replay_gate = Gate()
    gate = replay_gate
    async with Workflow("step-st04", workflow, storage=history) as resumed:
        # Outer runs again, gets the first child's recorded value, and retries
        # the unfinished second child, which reaches the replacement gate.
        await wait_for_gate(replay_gate)
        # Complete the second child, allowing outer to return both values.
        replay_gate.release.set()
        await asyncio.wait_for(resumed, timeout=2)
    # The workflow receives the combined result only after the successful retry.
    assert observed == [("kept", "done")]
    # The completed child's body ran once; outer and pending_child each ran twice.
    assert effects == ["outer", "completed", "pending", "outer", "pending"]


@pytest.mark.asyncio
async def test_caught_step_exception_is_replayed_without_rerunning_body():
    # Scenario ST06: a recorded, reconstructable step exception is caught on
    # replay with the same type/arguments and workflow handling path.
    history = []
    attempts = []
    handled = []
    gate = Gate()

    @step
    async def fail_once():
        # Witness actual body execution before producing the recorded exception.
        attempts.append("called")
        raise ValueError("expected", 17)

    async def workflow():
        try:
            await fail_once()
        except ValueError as error:
            # Ordinary exception handling runs on both initial execution and replay.
            handled.append((type(error), error.args))
        # Wait after handling the failure, keeping the workflow recoverable.
        await gate.wait()
        handled.append("continued")

    # Run through the failure and its handler, then suspend at the checkpoint.
    async with Workflow("step-st06", workflow, storage=history):
        await wait_for_gate(gate)

    # Replay the failed step's outcome, not its body, using a fresh checkpoint.
    replay_gate = Gate()
    gate = replay_gate
    async with Workflow("step-st06", workflow, storage=history) as resumed:
        # Reaching the gate means the replayed exception was caught successfully.
        await wait_for_gate(replay_gate)
        # Allow only this execution to continue beyond the checkpoint.
        replay_gate.release.set()
        await asyncio.wait_for(resumed, timeout=2)
    # The step body failed once, but the handler saw the same type and arguments
    # twice. Only the resumed workflow reached the final continuation.
    assert attempts == ["called"]
    assert handled == [
        (ValueError, ("expected", 17)),
        (ValueError, ("expected", 17)),
        "continued",
    ]


@pytest.mark.asyncio
async def test_repeated_calls_to_same_step_replay_in_sequence():
    # Scenario ST07: repeated calls to the same decorated function in a loop
    # retain distinct results and replay the completed prefix in order.
    history = []
    effects = []
    gate = Gate()

    @step
    async def record(value):
        # Distinct input/output values make each occurrence observable even
        # though every iteration calls the same decorated function.
        effects.append(value)
        return value * 10

    observed = []

    async def workflow():
        results = []
        for index in range(3):
            results.append(await record(index))
            if index == 0:
                # Interrupt after the first result, before calls 1 and 2 begin.
                await gate.wait()
        observed.append(results)

    # Record the first call and suspend while the loop's local list is partial.
    async with Workflow("step-st07", workflow, storage=history):
        await wait_for_gate(gate)

    # Restart the workflow function with its original history and a fresh gate.
    replay_gate = Gate()
    gate = replay_gate
    async with Workflow("step-st07", workflow, storage=history) as resumed:
        # The loop rebuilds its local list using the recorded result for call 0.
        await wait_for_gate(replay_gate)
        # Continue into new execution: calls 1 and 2 now run for the first time.
        replay_gate.release.set()
        await asyncio.wait_for(resumed, timeout=2)
    # The resumed loop returns all results in order, without rerunning call 0.
    assert observed == [[0, 10, 20]]
    assert effects == [0, 1, 2]


@pytest.mark.asyncio
async def test_replay_rejects_changed_step_sequence():
    # Scenario ST09: changing a recorded step sequence is rejected instead of
    # associating the old result with a different call.
    history = []

    @step
    async def first():
        return "first"

    @step
    async def second():
        return "second"

    gate = Gate()

    async def workflow():
        await first()
        await gate.wait()

    # Record a completed call to first(), then suspend before workflow completion.
    async with Workflow("step-st09", workflow, storage=history):
        await wait_for_gate(gate)

    # Replace the workflow's next replayed step with a different named function.
    # This deliberately violates compatibility with the recorded history.
    async def workflow():
        await second()

    # Reopen that history under the same workflow ID. Replay must reject the
    # mismatch rather than silently supply first()'s result to second().
    async with Workflow("step-st09", workflow, storage=history) as incompatible:
        with pytest.raises(Exception):
            await asyncio.wait_for(incompatible, timeout=2)
