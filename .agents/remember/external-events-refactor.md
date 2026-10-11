# External Events Refactor

## Status

This document captures the context, motivation, decisions, proposed design, and remaining questions for refactoring external-event execution and replay in Quest. It was separated from [resource-stream-revisit.md](resource-stream-revisit.md) because external-event durability can be implemented independently and will probably be addressed first.

The operation-task direction below is settled at the design level. Cancellation, suspension, exact history record shapes, and some compatibility behavior still require explicit decisions before implementation.

## Scope

An external event is a call from outside workflow execution to an action on a currently active resource. The call identifies a resource by workflow, name, and target identity, identifies an action, and supplies arguments. The caller awaits the operation and may receive a result or an exception.

This refactor concerns:

- how an external operation is represented in durable history;
- how the live caller awaits and receives its outcome;
- how the operation is re-executed during replay;
- how concurrent external operations receive independent task and replay state;
- how nondeterministic values used by an action are restored;
- how action success, rejection, failure, cancellation, and suspension interact with workflow execution.

It does not define resource-stream publication, snapshot coalescing, subscription lifecycle, or WebSocket delivery. Those concerns are covered by [resource-stream-revisit.md](resource-stream-revisit.md). The two designs meet at the resource action boundary: an external action may mutate exposed state and invalidate the current resource snapshot, but its durable execution does not depend on how snapshots are delivered.

Authorization is also a separate boundary concern. The manager or transport must distinguish authenticated caller identity from the target resource identity and authorize the operation before launching it.

## Existing behavior

`Historian.record_external_event()` delegates to `ResourceRuntime.record_external_event()`. The resource runtime:

1. finds the current resource by its name-and-identity address;
2. invokes the requested method;
3. waits for it to return;
4. appends one `ResourceAccessEvent` of type `external`, including the arguments and result;
5. returns the result to the caller.

During replay, one synthetic external handler consumes every external record under one shared external task identity. For each record it invokes the resource method again and asserts that the new result equals the recorded result.

This works for simple deterministic operations such as placing a supplied value onto a queue, but it has important limitations:

- the history record is appended only after execution, so it is not a durable launch boundary;
- all external calls conceptually share one replay task, prefix stack, and record stream;
- concurrent calls cannot safely interleave nested durable steps under that shared task identity;
- the action result is treated as both client response and replay validation, although these have different purposes;
- an action that generates a nondeterministic value directly will return a different value during replay;
- operation failure, cancellation, and interruption do not have a complete durable lifecycle model.

`identity_queue.put()` exposes the immediate problem. It generates an identity with `uuid.uuid4()`, enqueues `(identity, value)` for the workflow, and returns the identity to the unidentified caller. Re-executing it during replay generates a different identity and fails the current result assertion. Merely replaying the recorded return without executing the action would avoid the assertion but would fail to reconstruct the queue effect.

## Requirements

The refactor must preserve these properties:

- A live caller awaits the external operation and receives its result or exception.
- The operation is durably ordered relative to workflow tasks and other external operations.
- Replay re-executes the action so that its effects on workflow resources are reconstructed.
- Concurrent external operations have independent durable call stacks and replay streams.
- Values that affect reconstructed behavior are deterministic during replay.
- A rejected client operation does not automatically become workflow failure.
- No response is redelivered to an absent historical caller during replay.
- A recorded operation targeting code that is no longer compatible fails replay clearly.
- Resource routing remains generic; the historian does not acquire type-specific knowledge of queues, identity queues, or custom resources.

## Considered designs

### One post-execution external record

This is the current model. It is simple, but it cannot naturally contain nested durable work because the action has already finished by the time its record is written. It also forces concurrent calls through shared synthetic external task state and conflates a client return value with replay determinism.

This design is rejected.

### Shared `external_start` and `external_end` records

Recording boundaries around execution would allow nested records to appear between them. However, if all external calls retain the current shared external task identity, concurrent calls can interleave start and end records and corrupt the shared prefix stack or replay stream. Serializing all external actions would avoid that problem but would unnecessarily constrain independent callers and resources.

Start and end boundaries alone are therefore insufficient. Each operation needs a distinct task identity.

### Separate historian-managed task per operation

Each external invocation receives a unique operation task ID. A launch record durably identifies the operation and causes a historian-managed task to execute it. Records created during that action belong to the operation task, not to the shared external dispatcher. The live caller awaits a separate response associated with the task. During replay the dispatcher launches the same task under its recorded ID without creating or delivering a client response.

This is the selected direction.

A separate task still needs durable lifecycle boundaries. The chosen shape is one external-operation launch record followed by ordinary task-scoped execution and task completion. A second external-specific result envelope would duplicate task completion and is not needed.

## Settled design

### 1. Each external operation has a unique durable task identity

The historian allocates an operation ID before the operation starts. The ID is recorded with the resource address, action, and arguments. The operation executes in a historian-managed task using that ID for:

- its prefix stack;
- its replay-record stream;
- nested `handle_step()` calls;
- resource interactions;
- task completion.

Concurrent operations therefore cannot corrupt one another's task-local replay state.

### 2. The live caller awaits a response associated with the operation

Starting an operation and waiting for its outcome are distinct concerns. The caller of `record_external_event()` receives an awaitable response and remains suspended until the operation succeeds or fails.

The response may be implemented with a future associated with the operation task. On success it receives the action's live return value. On failure it receives the action exception. The response is resolved only after the operation's durable completion record has been appended.

Replay creates no response future because there is no historical caller waiting for a reply.

### 3. History contains a launch record and task completion

The launch record belongs to the synthetic external dispatcher and contains approximately:

```python
class ExternalOperationStarted(TypedDict):
    type: Literal["start_external_operation"]
    timestamp: str
    step_id: str
    task_id: str          # the synthetic external dispatcher
    operation_id: str     # the new historian-managed task
    resource_id: str
    action: str
    args: list
    kwargs: dict
```

Execution records belong to `operation_id`. Completion uses the ordinary task-lifecycle mechanism and records whether the operation succeeded or raised. The exact generic task-completion schema may evolve, but it must represent failure sufficiently for replay to validate the historical outcome.

The top-level action return value is not recorded as a value to redeliver or compare. It is an immediate live protocol result, not published workflow state and not a replay response.

### 4. Replay launches but does not await operations in the dispatcher

The external replay handler consumes `start_external_operation` records. For each record it starts a historian-managed task under the recorded operation ID and then releases the launch record's gate.

It must not await the operation in the dispatcher. The operation may need records that occur later in global history or may be interleaved with workflow tasks and other external operations. Awaiting it at launch could deadlock replay and would serialize operations unnecessarily.

The existing global record gates continue to preserve historical ordering. Each task consumes only records assigned to its task ID while waiting for intervening records to be completed by their owners.

### 5. Actions re-execute during replay

The whole external action must not be wrapped as one `handle_step()`. If it were, replay would rehydrate its result and skip its body, so effects such as queue insertion would not be reconstructed.

Instead, the action body runs again. Any nondeterministic value that affects its behavior or resource effects must be obtained through a durable step or another replay-aware Quest primitive.

For example, `identity_queue.put()` should durably generate its identity:

```python
async def put(self, value):
    identity = await historian.handle_step(
        "create_identity",
        lambda: str(uuid.uuid4()),
    )
    await self._queue.put((identity, value))
    return identity
```

During live execution, the generated identity is recorded by the nested step, enqueued for the workflow, and returned to the caller. During replay, the step restores the same identity and the action repeats the queue insertion. The replayed top-level return is discarded.

### 6. External operation failure is not automatically workflow failure

An external action may reject input or raise for reasons local to that request. The operation wrapper catches the outcome, records completion, and resolves or rejects the live response. A historically failed external operation must replay as that operation's recorded outcome without being mistaken for failure of the workflow itself.

Unexpected historian or replay-infrastructure failures remain fatal. The implementation must distinguish those from the action exception that constitutes the recorded client-operation outcome.

### 7. Resource routing remains generic and name-addressed

The resource runtime finds the current resource by `(name, target_identity)`, verifies that the requested action is externally exposed, and invokes it. It does not own the operation's durable task lifecycle.

Addresses remain stable and name-based. A call targets whichever resource is active at that address when the operation executes. Replay must fail clearly if the recorded resource or action is no longer compatible with the reconstructed workflow.

The authenticated caller identity is separate from the target identity in the resource address. Authorization must happen before the durable operation is launched.

## Proposed runtime flow

### Live execution

```text
caller
  -> Historian.record_external_event(...)
      -> authorize/validate request boundary
      -> allocate operation ID
      -> append start_external_operation
      -> create response future
      -> launch historian-managed operation task
      -> await response

operation task
  -> invoke current resource action
  -> execute nested durable steps and resource interactions
  -> append/validate task completion
  -> resolve response with result or exception
```

The launch append and task registration should occur without an intervening suspension that could expose a recorded launch with no registered task in the live process.

### Replay

```text
external replay dispatcher
  -> consume start_external_operation
  -> launch operation task under recorded ID
  -> do not create a response
  -> do not await the operation
  -> continue through dispatcher records

operation task
  -> consume records for its own task ID
  -> re-execute the resource action
  -> restore nested nondeterministic steps
  -> reconstruct resource effects
  -> validate recorded completion
  -> discard top-level return
```

Replay completion must wait for all launched operation tasks and all record gates, not merely for the dispatcher to reach the end of its own record stream.

## Proposed code changes

### `src/quest/quest_types.py`

- Replace `ResourceAccessEvent.type == "external"` with an external-operation launch record.
- Extend or replace `TaskEvent` completion records so success and expected action failure can be represented.
- Keep internal resource access records separate from external operation lifecycle records.

### `src/quest/historian.py`

- Change `record_external_event()` to allocate an operation ID, append the launch record, start an operation task, and await its response.
- Add a shared helper for registering historian-managed tasks with a specific recorded ID.
- Change `_external_handler()` to launch replay operation tasks rather than execute resource actions directly.
- Track dynamically launched operation tasks in `_open_tasks` and replay completion.
- Give every operation its own prefix and `_task_replay_records()` generator.
- Ensure expected action failures do not escape into the workflow `TaskGroup` as workflow failures.
- Update suspension and pruning behavior for operation tasks.

### `src/quest/historian_resources.py`

- Replace `record_external_event()` and `replay_external_event()` with a non-recording `invoke_external_action()` boundary.
- Keep resource lookup, exposed-action validation, invocation, and awaitable handling in the resource runtime.
- Leave operation history and task lifecycle under the historian.

### Built-in resources

- Change `identity_queue` identity generation to use a durable step.
- Audit built-in actions for other nondeterminism that affects replayed behavior.

### History pruning

The current pruning logic recognizes post-execution `external` records associated with resources scoped inside a skipped step. With operation tasks, pruning must remove both:

- the launch record for an operation on a pruned resource lifetime; and
- records owned by that operation task.

The exact indexing or traversal strategy remains an implementation detail, but no orphan operation-task records may remain after their launch is pruned.

## Determinism contract for action authors

External actions are replayed code. Therefore:

- supplied arguments come from the recorded launch;
- actions must use `handle_step()` or another durable primitive for nondeterministic values that affect control flow, resource effects, or reconstructed state;
- actions must not depend on an unrecorded live caller or transport object;
- effects outside Quest's replay model remain the application author's responsibility;
- the top-level return may provide immediate protocol information, but replay does not use it as the authoritative source of reconstructed state.

Quest does not thereby impose business idempotency, deduplication, one-shot queue behavior, or acknowledgement policy. Those remain workflow and application concerns.

## Remaining design questions

### Caller cancellation and disconnect

Decide whether cancellation of the caller's await cancels an already launched durable operation. The current recommendation is that it does not: the caller awaits a shielded response, and transport disconnection must not rewrite durable execution semantics. The abandoned response still needs safe cleanup.

### Suspension

Define what happens when the workflow is suspended while external operations are active:

- whether operation tasks receive the same distinguished suspension cancellation as workflow tasks;
- which partial records remain;
- how replay resumes an operation whose launch was durable but whose completion was not;
- how any live caller is notified.

### Completion and exception schema

Define the exact task-completion record shape and compatibility comparison for exceptions. Expected action failure must be distinguishable from replay-infrastructure failure without requiring the managed asyncio task itself to fail.

### Authorization

Define the manager and transport authorization API. Authenticated caller identity is distinct from the target resource identity in the address. The policy must define who may invoke private resources, who may invoke public resources, and how local trusted callers differ from remotely authenticated callers. Authorization failure should occur before durable launch so unauthorized attempts do not enter workflow history.

### Operation start validation

Decide exactly when missing-resource, missing-action, and argument-validation failures occur. The design must say whether they are rejected before history is written or represented as durably launched failed operations.

### Replay incompatibility

Define dedicated errors for cases where replay reconstructs no resource at the recorded address, the current resource no longer exposes the action, or the evolved action cannot accept the recorded arguments.

### Result serialization

The live result must be serializable when returned through a remote transport, but it is not currently intended to be persisted as the operation's top-level replay result. Confirm whether local-only actions may return values unsupported by the configured transport serializer.

### Task infrastructure

Decide whether to generalize the existing `start_task()` machinery for externally launched tasks or introduce a narrowly scoped operation-task helper. The implementation should share task registration and replay mechanics without forcing external operations through workflow-child semantics that do not apply.

## Verification

Tests should cover at least:

- a live caller waiting for and receiving an action result;
- an action exception being returned to its caller without failing the workflow;
- a launch record preceding operation-owned records;
- replay launching the operation under its recorded task ID;
- replay re-executing action effects rather than rehydrating the top-level result;
- concurrent external operations retaining independent prefix stacks and replay streams;
- interleaved workflow and operation records respecting global history order;
- the replay dispatcher not deadlocking by awaiting an operation;
- generated identities being identical internally during live execution and replay;
- generated identities being returned only to live callers;
- no response being created or delivered during replay;
- replay completion waiting for all operation tasks;
- historically failed operations replaying without unintended workflow failure;
- unexpected replay failures remaining fatal;
- suspension and resumption of an in-progress operation under the selected policy;
- caller cancellation or transport disconnect under the selected policy;
- clear incompatibility errors for missing resources, actions, or changed arguments;
- pruning removing both a scoped resource's operation launch and all records owned by that operation task;
- repeated name-addressed operations targeting the active resource at that address.
