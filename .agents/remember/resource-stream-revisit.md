# Resource Stream Revisit

## Status

This document captures the intent, motivation, and design decisions from the resource-stream design discussion on the `resource-stream-revisit` branch. It is a design record, not yet an implementation plan.

## Project intent

Quest is a framework for expressing long-running workflows that control a changing **state and action surface**.

A workflow publishes resources while they are relevant. A user interface observes the current resource surface and decides how to render it. The UI may invoke exposed resource actions, and those actions may allow the workflow to proceed.

For example, a workflow might expose:

- a `state` resource containing a message;
- another `state` resource containing supporting data;
- a `queue` resource accepting the user's response.

The UI can combine those resources into one component. A web UI, CLI, and mobile UI may render the same workflow surface differently.

Quest's responsibility is to provide useful, flexible, correct, and sound workflow/resource semantics. It is not intended to enforce application-level business practices such as acknowledgement policies, idempotency, deduplication, or whether extra queue inputs should be consumed or ignored.

## Separation of concerns

### Workflow responsibilities

The workflow controls:

- which resources currently exist;
- resource names, identities, types, and values;
- which actions are currently available;
- when the workflow waits for an action;
- how inputs are interpreted;
- when resources are changed or withdrawn.

### UI responsibilities

The UI controls:

- component selection;
- layout and styling;
- static text and labels;
- localization;
- how multiple resources compose into a component;
- how missing or temporarily incomplete resource combinations are rendered.

If text or labels are dynamic workflow-controlled content, the workflow can expose them through ordinary `state` resources. Quest does not need presentation metadata such as component names, layout, or styling in its core resource model.

## Existing behavior

The current resource stream provides an initial snapshot and subsequent resource snapshots. Resources are filtered by identity:

- public resources (`identity is None`) are visible to everyone;
- private resources are visible only to the matching identity.

The current implementation applies delivery backpressure:

1. A workflow resource changes.
2. `ResourceStreamManager.update()` signals each relevant stream.
3. The workflow waits on that stream's gate.
4. The gate is released only when the listener requests another item or closes the stream.

For a local `async for` listener, this means the workflow waits through the listener's loop body. For the WebSocket server, the server does not request the next stream item until `ws.send()` completes, so a stalled socket can stall the workflow.

This behavior ensures that every listener can observe every transition, but it also makes workflow progress depend on observers. A stale listener, abandoned iterator, slow UI, or blocked network send can stop the workflow.

## Refactor motivation

Transport delivery is not workflow intent.

The current design conflates:

- **transport progress**: a listener or socket accepted an update; and
- **workflow progress**: the intended user supplied an action the workflow needs.

A UI subscribing, rendering, or asking for another update should not advance the workflow. Likewise, an inactive UI should not prevent unrelated workflow execution.

Required interaction sequencing should be represented explicitly in workflow logic. If a workflow needs user input, it should expose an action resource and await that resource. If it does not need input, it should be free to continue regardless of subscriber behavior.

## Agreed design direction

### 1. The resource surface is authoritative latest state

Resource streaming should represent the latest workflow-controlled surface, not an irreplaceable sequence of events.

A subscriber needs:

- the current surface immediately on subscription;
- notification when a newer surface is available;
- the newest surface after reconnecting.

It does not need every intermediate surface. Intermediate revisions may be skipped.

### 2. Publication must not backpressure workflow execution

A workflow-side surface change should do only fast, local work:

1. Update the authoritative resource registry/state.
2. Mark relevant subscribers dirty.
3. Continue workflow execution.

It must not await a stream consumer or network send.

Each WebSocket connection should have an independent sender task. A stalled sender affects only that connection. It does not affect the workflow or other subscribers.

The one intentional exception is workflow logic waiting on a resource operation, such as `await response.get()`. That is explicit workflow synchronization, not stream-delivery backpressure.

### 3. Use overwrite/latest semantics, not an update queue

There is no need to queue, merge, or replay pending snapshots.

The system needs:

- one authoritative current surface;
- one dirty signal per subscriber;
- no per-update history.

When several changes occur while a sender is busy, they overwrite one another conceptually. Once the sender is available, it reads and sends the latest surface.

An already-started WebSocket send cannot be recalled. A client may therefore receive an older snapshot followed by the newest snapshot, but it need not receive the intermediate ones.

### 4. WebSockets provide live push, not durability

WebSocket delivery can be relied upon for live notification while a connection is healthy, but the connection is not durable storage.

The intended lifecycle is:

1. The UI connects and subscribes.
2. The server immediately sends the current surface.
3. The server pushes newer surfaces while connected.
4. If disconnected, the UI reconnects and receives the current surface again.

Routine polling is unnecessary. Correctness comes from resynchronizing to authoritative current state on subscription or reconnection, not from assuming every pushed message arrives.

### 5. Workflow sequencing is explicit through resources

Existing resources are sufficient to express required sequencing. No UI-specific acknowledgement primitive is currently needed.

For example:

```python
async with state("message", user_id, "Confirm purchase?"):
    async with state("warning", user_id, "This cannot be undone"):
        async with queue("confirmation", user_id) as confirmation:
            answer = await confirmation.get()
```

The supporting states are created before the action resource. Contexts unwind in reverse order, so the action disappears before its supporting states. A UI can treat the key action resource as the readiness marker and render only when it is present.

UIs are expected to tolerate incomplete combinations of resources by waiting for the key resources they need. Atomic multi-resource publication or UI-specific grouping is not currently required.

### 6. UI contracts remain dynamic

Strongly typed UI contracts are not required at this stage.

Resource names and runtime resource types form a dynamic interface in the same spirit as names in Python or JavaScript. Conventions and tests are sufficient while the design is experimental.

Typed schemas, generated bindings, or stronger contracts may be added later if concrete experience demonstrates their value. The core design should not require them now.

### 7. Resource generations identify exact resource lifetimes

A resource key such as `(name, identity)` may be deleted and later recreated. Those two resources can represent different workflow interactions even though their semantic key is the same.

Every resource lifetime therefore needs a **generation** identifier. A generation:

- is assigned when the resource is registered;
- remains stable while that resource exists;
- changes when a resource with the same key is deleted and recreated;
- is included in the published surface;
- is required on every external resource operation, including reads;
- is checked before invoking the operation.

A stale operation must be rejected rather than applied to a newer resource that happens to have the same name and identity. The client must not automatically retry a stale operation against the new generation, because it may represent a different interaction.

Quest already creates deterministic unique IDs for repeated invocations in the same scope. The `step_id` associated with a resource's `create_resource` lifecycle event can likely serve as its generation. This has the useful property that replay reconstructs the same generation for the same logical resource interaction, including across suspension and resumption.

Generation is a correctness token, not an authentication mechanism.

Breaking the external-operation API to require generation is acceptable because Quest is still experimental and design quality takes priority over compatibility.

### 8. Snapshot revision and resource generation are different

A **snapshot revision** identifies a version of the whole published surface.

A **resource generation** identifies one resource lifetime.

A resource can retain its generation across many state changes and surface revisions. External actions should be validated against the resource generation rather than the global surface revision; an unrelated surface update should not invalidate an otherwise relevant operation.

Snapshot revisions are useful for ordering pushed snapshots and ignoring older data around reconnection. Their exact representation and lifecycle remain to be finalized.

### 9. External operations do not inherently change the surface

An external signal does not necessarily change workflow-controlled published state.

For example, a queue may intentionally accept repeated inputs while remaining the same resource generation. Each `put()` should not automatically cause a redundant surface publication merely because an external operation occurred.

Surface publication should occur when the published surface actually changes, such as:

- resource creation;
- resource deletion;
- a change to state that is part of the published surface.

It should not occur after every arbitrary external resource operation.

### 10. Queue policy belongs to workflows and applications

Ordinary queues should retain ordinary repeatable queue semantics. If a queue remains active, it may accept multiple inputs. A workflow may consume all, consume some, or ignore extras.

Generation validation rejects inputs after the resource has disappeared or been replaced, but it does not impose one-response-only behavior during an active resource lifetime.

Quest should not impose one-shot behavior, acknowledgement policy, deduplication, idempotency, or business-level interpretation on all applications. Such behavior can be expressed by workflow code or optional abstractions if needed.

## Proposed runtime shape

The design suggests the following separation:

### Resource runtime

The resource runtime owns the authoritative registry and resource visibility. On a publishable change, it updates the current surface and marks relevant subscriptions dirty without waiting for them.

### Subscription

A subscription contains approximately:

- subscriber identity;
- a dirty event;
- closed/stopped state;
- enough revision information to avoid redundant sends.

It does not contain a queue of snapshots or a gate that workflow execution waits upon.

### WebSocket sender

Each connection runs independently:

```python
async def push_updates(subscription, websocket):
    while True:
        await subscription.changed.wait()
        subscription.changed.clear()

        snapshot = get_latest_surface(subscription.identity)
        await websocket.send(snapshot)
```

If the surface changes while `websocket.send()` is blocked, the dirty event is set again. After the send finishes, the sender reads the then-current surface.

The production implementation must preserve this behavior without a lost-notification race around initial subscription, event clearing, or stream termination.

### External operation

An external call identifies:

- workflow;
- resource name;
- identity;
- generation;
- action;
- arguments.

The runtime finds the current resource by `(name, identity)`, validates the generation, and invokes the action only on a match. A dedicated stale-resource error should distinguish a stale operation from an application/resource exception.

## Replay and determinism

The new stream model must preserve Quest's replay guarantees:

- replay reconstructs the authoritative current resource surface;
- replay does not emit historical intermediate surfaces as though they were live changes;
- a logically identical resource creation reconstructs the same generation;
- external history identifies the resource generation it targeted;
- stale-generation validation does not make replay nondeterministic;
- suspension and resumption preserve the intended logical resource lifetime.

Older persisted records are not a primary compatibility constraint while the project remains experimental, though the history schema change should still be deliberate and explicit.

## Scope boundaries

This design deliberately does not decide application policy for:

- whether extra queue entries are errors;
- whether user input requires acknowledgement;
- whether actions are idempotent;
- whether requests need application-level correlation IDs;
- whether a UI disables controls after submission;
- how consequential business operations are modeled.

Quest should make these policies possible, not mandate them.

## Remaining details to discuss

The following design questions remain open or need more precise decisions before implementation.

### Snapshot contents

- Does a surface snapshot include current `state` values, or only resource descriptors/handles?
- If values are fetched separately, what consistency guarantee exists between discovery and read?
- Which resource data is considered part of the published surface?
- What serialization constraints apply to published values?

Presentation metadata is out of scope, but the data-level snapshot contract still needs definition.

### Revision semantics

- Is the revision global per workflow or scoped by identity-visible surface?
- Does every publishable mutation increment it, including a mutation invisible to a given subscriber?
- How is revision identity handled across suspension, process restart, and replay?
- Does a reconnecting client compare revisions across connection lifetimes, or simply replace its state with the initial snapshot?

### Publication triggers

- How do resource implementations indicate that exposed state changed?
- Should built-in `state.set()` publish directly through the runtime?
- How should custom resources participate without requiring UI-specific metadata?
- Are current notifications after every internal resource operation too broad for the new semantics?

### Initial subscription and lost-update safety

- How are subscription registration and initial snapshot capture ordered?
- How does the implementation guarantee that a change concurrent with initial snapshot creation is either represented in that snapshot or causes a later dirty notification?
- What is the simplest event/condition implementation that preserves this invariant?

### Completion, suspension, and failure

- Does the stream send an explicit terminal snapshot or simply end?
- What surface remains available after completion?
- Should a reconnecting UI be able to obtain completed, suspended, or failed status?
- How long should terminal state be retained by `WorkflowManager`?
- Does resumption continue the same subscription conceptually, or require a new subscription?

### Generation persistence and history schema

- Confirm that the resource creation `step_id` is the correct generation identifier.
- Store the generation in `ResourceEntry` and expose it in snapshots.
- Add generation to external resource-access records and replay validation.
- Define the stale-resource exception and wire format.
- Confirm behavior for repeated create/delete cycles, nested scopes, and suspend/resume replay.

### API shape

- Define the new return shape of `get_resources()` and resource-stream snapshots.
- Decide whether `get_resource_stream()` remains a synchronous context manager around an async iterator or is replaced with a simpler subscription API.
- Update manager wrappers and external resource wrappers to retain and send generation.
- Decide whether direct name-based convenience APIs such as `get_state()` return generation-bound wrappers.

### WebSocket protocol

- Define initial subscription request and snapshot response shapes.
- Define stale-resource and workflow-terminal errors.
- Ensure authentication and identity authorization remain independent of resource generation.
- Define reconnect behavior and heartbeat/liveness handling.
- Bound socket/server buffering so obsolete snapshots cannot accumulate without limit.

### Multiple subscribers

- Confirm behavior for multiple tabs or devices using the same identity.
- Ensure each sender can stall or disconnect independently.
- Confirm that no subscriber lifecycle operation affects workflow progress.

### Verification

Tests should cover at least:

- immediate current snapshot on subscription;
- identity visibility rules;
- a workflow progressing while a listener never consumes updates;
- a stalled WebSocket not blocking workflow execution;
- slow listeners skipping intermediate states and eventually receiving the latest state;
- multiple independent subscribers;
- reconnect and current-state resynchronization;
- deterministic generations across replay and suspend/resume;
- stale operations rejected after resource replacement;
- repeated operations accepted while an ordinary queue generation remains active;
- no automatic publication solely because an external operation occurred;
- completion, suspension, and failure behavior once those semantics are decided.
