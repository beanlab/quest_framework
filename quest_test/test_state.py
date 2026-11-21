import asyncio

import pytest

from quest import State, Workflow


async def wait_for(event):
    await asyncio.wait_for(event.wait(), timeout=2)


async def next_snapshot(stream, deadline=None):
    loop = asyncio.get_running_loop()
    if deadline is None:
        deadline = loop.time() + 2
    remaining = deadline - loop.time()
    if remaining <= 0:
        raise asyncio.TimeoutError
    return await asyncio.wait_for(anext(stream), timeout=remaining)


@pytest.mark.asyncio
async def test_state_initial_value_read_write_and_withdrawal():
    # Scenario SA01: state begins with its declared value, get/set agree, and
    # leaving its scope withdraws it from the view selected for that identity.
    entered = asyncio.Event()
    update = asyncio.Event()
    exit_scope = asyncio.Event()
    finish = asyncio.Event()
    observed_values = []

    async def workflow():
        # Enter the public state scope, expose its initial read, then wait for
        # the observer to authorize a write and later a scope exit.
        state = State("reader", "score", 1)
        async with state:
            observed_values.append(await state.get())
            entered.set()
            await update.wait()
            await state.set(8)
            observed_values.append(await state.get())
            await exit_scope.wait()
        # Keep the workflow alive long enough for the observer to see withdrawal.
        await finish.wait()

    # The host watches the selected identity's current view and releases workflow
    # phases only after observing each expected publication.
    async with Workflow("state-sa01", workflow) as running:
        async with running.resource_stream(["reader"]) as stream:
            initial = await _snapshot_with_value(stream, 1)
            assert initial.state == [("reader", "score", 1)]
            await wait_for(entered)
            update.set()
            changed = await next_snapshot(stream)
            assert changed.state == [("reader", "score", 8)]
            exit_scope.set()
            # Keep the workflow alive after leaving the scope so withdrawal is published.
            removed = await next_snapshot(stream)
            assert removed.state == []
            finish.set()
        await asyncio.wait_for(running, timeout=2)
    # Reads inside workflow code agree with the initial and updated snapshots.
    assert observed_values == [1, 8]



@pytest.mark.asyncio
async def test_state_values_are_visible_as_current_snapshots():
    # Scenario SA02: representative scalar, empty, and nested JSON-shaped values
    # can be written, read, and observed consistently through the public stream.
    entered = asyncio.Event()
    update = asyncio.Event()
    finish = asyncio.Event()
    complete = asyncio.Event()
    values = [0, "", [], {"items": [1, {"ok": True}], "meta": {}}]
    received = []

    async def workflow():
        # Publish the initial value, then apply each representative JSON-shaped
        # value under host control and record what workflow reads back.
        async with State("values", "payload", None) as state:
            received.append(await state.get())
            entered.set()
            await update.wait()
            for value in values:
                await state.set(value)
                received.append(await state.get())
            await finish.wait()
        await complete.wait()

    # Observe initial and final current snapshots; intermediate writes are not
    # required to appear individually in this resource stream.
    async with Workflow("state-sa02", workflow) as running:
        async with running.resource_stream(["values"]) as stream:
            snapshot = await _snapshot_with_value(stream, None)
            assert snapshot.state == [("values", "payload", None)]
            await wait_for(entered)
            update.set()
            # Multiple writes may coalesce: assert the current final view, not an event log.
            final = await _snapshot_with_value(stream, values[-1])
            assert final.state == [("values", "payload", values[-1])]
            finish.set()
            await _snapshot_empty(stream)  # The scope's removal notification.
            complete.set()
        await asyncio.wait_for(running, timeout=2)
    # Workflow reads retain every value, even when observer snapshots coalesce.
    assert received == [None, *values]


async def _snapshot_with_value(stream, value):
    deadline = asyncio.get_running_loop().time() + 2
    while True:
        snapshot = await next_snapshot(stream, deadline)
        if any(entry[2] == value for entry in snapshot.state):
            return snapshot


@pytest.mark.asyncio
async def test_private_and_internal_only_state_visibility():
    # Scenario SA03: a private state is visible only through its identity, and
    # external=False remains readable in workflow code but absent from snapshots.
    entered = asyncio.Event()
    finish = asyncio.Event()
    complete = asyncio.Event()
    internal_reads = []

    async def workflow():
        # Keep a private public-facing value alongside an internal-only state;
        # workflow code can read both while external observers see only one.
        async with State("alice", "private", "visible") as private:
            async with State("alice", "working", {"step": 1}, external=False) as internal:
                internal_reads.append(await internal.get())
                entered.set()
                await finish.wait()
        await complete.wait()

    # Compare Alice's view with Bob's while both scopes are active, then hold
    # workflow completion until the host observes Alice's scope withdrawal.
    async with Workflow("state-sa03", workflow) as running:
        async with running.resource_stream(["alice"]) as alice_stream:
            alice = await _snapshot_with_value(alice_stream, "visible")
            assert alice.state == [("alice", "private", "visible")]
            await wait_for(entered)
            async with running.resource_stream(["bob"]) as bob_stream:
                bob = await next_snapshot(bob_stream)
                assert bob.state == []
            finish.set()
            removed = await next_snapshot(alice_stream)
            assert removed.state == []
            complete.set()
        await asyncio.wait_for(running, timeout=2)
    assert internal_reads == [{"step": 1}]


@pytest.mark.asyncio
async def test_state_names_and_identities_are_independent():
    # Scenario SA04: equal names under distinct identities and distinct names
    # under one identity retain independent values.
    results = []
    mutate = asyncio.Event()
    exit_scope = asyncio.Event()
    hold = asyncio.Event()

    async def workflow():
        # Overlap same-named state for two identities with a second name for
        # Alice, then mutate only Alice's shared entry under host coordination.
        async with State("alice", "shared", "alice") as alice_shared:
            async with State("bob", "shared", "bob") as bob_shared:
                async with State("alice", "other", "separate") as alice_other:
                    results.append([
                        await alice_shared.get(),
                        await bob_shared.get(),
                        await alice_other.get(),
                    ])
                    await mutate.wait()
                    await alice_shared.set("changed")
                    results.append([
                        await alice_shared.get(),
                        await bob_shared.get(),
                        await alice_other.get(),
                    ])
                    await exit_scope.wait()
        await hold.wait()

    # Observe each identity through its own stream, then verify Alice's update
    # leaves Bob's same-named state and Alice's differently named state intact.
    async with Workflow("state-sa04", workflow) as running:
        async with running.resource_stream(["alice"]) as alice_stream:
            alice = await _snapshot_with_entries(alice_stream, {
                ("alice", "shared", "alice"),
                ("alice", "other", "separate"),
            })
            async with running.resource_stream(["bob"]) as bob_stream:
                bob = await _snapshot_with_value(bob_stream, "bob")
                assert bob.state == [("bob", "shared", "bob")]
            mutate.set()
            changed = await _snapshot_with_value(alice_stream, "changed")
            assert changed.state == [
                ("alice", "shared", "changed"),
                ("alice", "other", "separate"),
            ]
        exit_scope.set()
        # The test releases the workflow only after observing scope withdrawal.
        async with running.resource_stream(["alice"]) as cleanup_stream:
            assert (await _snapshot_empty(cleanup_stream)).state == []
            hold.set()
        await asyncio.wait_for(running, timeout=2)
    assert results == [["alice", "bob", "separate"], ["changed", "bob", "separate"]]


async def _snapshot_with_entries(stream, expected):
    deadline = asyncio.get_running_loop().time() + 2
    while True:
        snapshot = await next_snapshot(stream, deadline)
        if expected.issubset(set(snapshot.state)):
            return snapshot


async def _snapshot_empty(stream):
    deadline = asyncio.get_running_loop().time() + 2
    while True:
        snapshot = await next_snapshot(stream, deadline)
        if not snapshot.state:
            return snapshot


@pytest.mark.asyncio
async def test_state_is_reconstructed_from_recorded_reads_and_writes():
    # Scenario SA05: workflow code recreates its State object and replays its
    # recorded initialization/read/write operations after suspension.
    history = []
    runs = []
    checkpoint = asyncio.Event()
    release = asyncio.Event()

    async def workflow():
        # Recreate the state scope on each run and perform its initialization,
        # read, and write through the same recorded operation sequence.
        async with State("replay", "counter", 2) as counter:
            start = await counter.get()
            await counter.set(start + 5)
            runs.append(await counter.get())
            checkpoint.set()
            await release.wait()

    # Suspend only after the updated value has been read and witnessed.
    async with Workflow("state-sa05", workflow, storage=history):
        await wait_for(checkpoint)
    assert runs == [7]

    # Reconstruct using retained history and fresh host events, then release the
    # second execution after it has replayed the state operations.
    checkpoint = asyncio.Event()
    release = asyncio.Event()
    async with Workflow("state-sa05", workflow, storage=history) as resumed:
        await wait_for(checkpoint)
        release.set()
        await asyncio.wait_for(resumed, timeout=2)
    # Workflow code reads the same reconstructed value on both executions.
    assert runs == [7, 7]


@pytest.mark.asyncio
async def test_state_cleanup_on_normal_and_caught_exception_exit_allows_reuse():
    # Scenario SA06: normal and caught-exception exits withdraw state; a later
    # scope can safely reuse the same identity/name with a fresh initial value.
    phase = asyncio.Event()
    allow_exception = asyncio.Event()
    exception_exited = asyncio.Event()
    allow_reuse = asyncio.Event()
    finish = asyncio.Event()
    complete = asyncio.Event()

    async def workflow():
        # Exercise normal scope exit, then caught-exception exit, before waiting
        # for host confirmation that the name was withdrawn and can be reused.
        async with State("reuse", "status", "normal"):
            pass
        phase.set()
        await allow_exception.wait()
        try:
            async with State("reuse", "status", "exception"):
                raise ValueError("handled")
        except ValueError:
            pass
        exception_exited.set()
        # Wait for the observer to confirm withdrawal before reusing the name.
        await allow_reuse.wait()
        async with State("reuse", "status", "reused") as state:
            assert await state.get() == "reused"
            await finish.wait()
        await complete.wait()

    # The host confirms each removal before allowing the next scope to enter.
    async with Workflow("state-sa06", workflow) as running:
        await wait_for(phase)
        async with running.resource_stream(["reuse"]) as stream:
            normal_exit = await _snapshot_empty(stream)
            assert normal_exit.state == []
            allow_exception.set()
            await wait_for(exception_exited)
            exception_exit = await _snapshot_empty(stream)
            assert exception_exit.state == []
            allow_reuse.set()
            current = await _snapshot_with_value(stream, "reused")
            assert current.state == [("reuse", "status", "reused")]
            finish.set()
            removed = await next_snapshot(stream)
            assert removed.state == []
            complete.set()
        await asyncio.wait_for(running, timeout=2)


@pytest.mark.asyncio
async def test_relevant_observer_receives_state_change_snapshot():
    # Scenario SA07: an already-waiting observer for the state identity receives
    # the updated current snapshot; unrelated identity observers are not required.
    ready = asyncio.Event()
    change = asyncio.Event()
    finish = asyncio.Event()

    async def workflow():
        # Hold a visible state open until the observer is established, then
        # publish one update and remain active for the observer to receive it.
        async with State("watcher", "mode", "waiting") as mode:
            ready.set()
            await change.wait()
            await mode.set("ready")
            await finish.wait()

    # Subscribe to the relevant identity before authorizing the workflow update.
    async with Workflow("state-sa07", workflow) as running:
        async with running.resource_stream(["watcher"]) as stream:
            assert (await _snapshot_with_value(stream, "waiting")).state == [("watcher", "mode", "waiting")]
            await wait_for(ready)
            change.set()
            changed = await _snapshot_with_value(stream, "ready")
            assert changed.state == [("watcher", "mode", "ready")]
            finish.set()
        await asyncio.wait_for(running, timeout=2)
