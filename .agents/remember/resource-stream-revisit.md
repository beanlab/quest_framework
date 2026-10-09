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

### 7. Communication primitives are directional

The external data-communication model has three resource types:

- `state` carries workflow-owned data from the workflow to clients;
- `queue` carries client input to the workflow for a known identity;
- `identity_queue` carries client input to the workflow when the interaction establishes or returns an identity.

State is externally read-only. The workflow may change it, and its current value is included directly in surface snapshots. Clients do not fetch or mutate state through external resource operations.

Queues expose only `put` externally. Business responses and acknowledgements should be represented through workflow-published state rather than a general request/response or arbitrary-action mechanism. Application-level correlation can be carried in queue payloads when needed.

An external `event` resource is unnecessary because a queue input can represent a signal. Writable state and arbitrary resource actions are also outside the external protocol. These concepts may remain useful as internal workflow implementation utilities, but they are not published communication primitives.

### 8. Resources retain stable name-based addresses

Resources continue to be addressed by workflow, name, and identity. Stable names are part of a workflow's designed public interface: an integration may intentionally target the current `messages` queue without first discovering a particular resource lifetime.

A delayed operation can therefore reach a replacement resource created later at the same `(name, identity)`. Optional resource generations could provide REST-like conditional-write semantics for clients that need exact-lifetime validation, but no concrete application currently requires that complexity. Generations are deferred and documented separately in [resource-generations.md](resource-generations.md).

### 9. Snapshot revisions are not part of the external protocol

The client protocol does not include a snapshot revision. WebSocket messages are ordered within a connection, each connection has one sender, and the initial snapshot after reconnection authoritatively replaces the client's prior surface.

The runtime does not need an internal or client-visible version counter. A per-subscription dirty event is sufficient; harmless redundant snapshots are preferred over version-tracking complexity. A revision can be added later if concrete experience demonstrates a need.

### 10. Dirty triggers remain generic

The runtime retains the current generic notification points:

- resource creation;
- resource deletion;
- completion of every internal resource event.

Each notification only marks relevant subscriptions dirty; it does not construct or push a snapshot and does not wait for a subscriber. This avoids teaching the historian which resource types or actions affect published data.

Some internal events, such as `queue.get()`, may therefore cause a redundant snapshot. Latest-state coalescing limits these snapshots, and the minor redundancy is preferable to type- and action-specific notification logic.

An external queue `put()` does not directly mark subscriptions dirty. If it allows an internal `queue.get()` to complete, that generic internal completion does mark them dirty. Repeated inputs remain valid while the queue is active.

### 11. Queue policy belongs to workflows and applications

Ordinary queues should retain ordinary repeatable queue semantics. If a queue remains active, it may accept multiple inputs. A workflow may consume all, consume some, or ignore extras.

A write fails when no resource is active at the named address. If a resource is replaced at the same address, name-based callers intentionally target the current resource. This does not impose one-response-only behavior during an active resource lifetime.

Quest should not impose one-shot behavior, acknowledgement policy, deduplication, idempotency, or business-level interpretation on all applications. Such behavior can be expressed by workflow code or optional abstractions if needed.

## Proposed runtime shape

The design suggests the following separation:

### Resource runtime

The resource runtime owns the authoritative registry and resource visibility. At each generic notification point, it marks relevant subscriptions dirty without constructing a snapshot or waiting for them.

### Subscription

A subscription contains approximately:

- subscriber identity;
- a dirty event, initialized as set;
- closed/stopped state.

Registration does not await: it adds the subscription and initializes the dirty event as set so the sender immediately reads the current surface. It does not contain a snapshot queue, version counter, or gate that workflow execution waits upon.

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

Subscription registration occurs without awaiting and initializes the event as set. The sender clears the event immediately before reading the latest surface. A concurrent change either appears in that read or sets the event for another pass. The design may produce a redundant snapshot in a race, but it does not lose the latest state.

### External operation

An external queue write identifies:

- workflow;
- resource name;
- identity;
- input value.

The runtime finds the current resource by `(name, identity)`, validates that it is a writable queue visible to the caller, and performs `put`. The stable name targets whichever resource is currently active at that address.

## Replay and determinism

The new stream model must preserve Quest's replay guarantees:

- replay reconstructs the authoritative current resource surface;
- replay does not emit historical intermediate surfaces as though they were live changes;
- external history preserves name-based queue writes;
- suspension and resumption preserve the intended active resource surface.

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

A snapshot is a plain list of type-specific resource records, with no envelope. The resource type determines permitted behavior, so records do not include an operation list.

```json
[
  {"type": "state", "name": "message", "identity": "user-1", "value": "Hello"},
  {"type": "queue", "name": "response", "identity": "user-1"},
  {"type": "identity_queue", "name": "register", "identity": null}
]
```

A `state` record includes its current value directly. State is not fetched separately. Queue records describe input channels but do not expose queue contents or internal state. The explicit fields replace the current mapping from `(name, identity)` tuple keys to resource class names.

State values and queue inputs must be supported by Quest's configured serializer. The resource surface does not publish input or value schemas. Validation belongs to workflow or application logic unless concrete experience justifies a protocol-level schema later.

Presentation metadata is out of scope.

### Publication triggers

The existing generic triggers remain: resource creation, resource deletion, and completion of every internal resource event. They mark subscriptions dirty rather than pushing snapshots. Redundant snapshots are acceptable in exchange for keeping the historian independent of resource type and action semantics.

### Initial subscription and lost-update safety

Registration synchronously adds the subscription with its `asyncio.Event` already set. The sender clears the event immediately before reading the latest surface. A concurrent change either appears in that read or leaves the event set for another pass. No internal version counter is needed; redundant snapshots are acceptable.

### Completion, suspension, and failure

The stream sends no terminal snapshot or status envelope. Its termination reason distinguishes outcomes:

- completion ends the async iterator normally and closes the WebSocket normally;
- suspension raises a dedicated `WorkflowSuspended` signal locally and uses a defined application WebSocket close code;
- workflow failure raises a failure error locally and uses a distinct failure close code;
- abnormal transport closure represents network loss rather than a managed workflow outcome.

Clients discard the displayed surface when the stream ends. Resumption requires a new subscription. Durable workflow status, results, and retention remain separate `WorkflowManager` API concerns.

### Deferred resource generations

Optional exact-lifetime preconditions are outside the current scope. Their motivation, compatibility strategy, and likely implementation are preserved in [resource-generations.md](resource-generations.md).

### API shape

- Define the new return shape of `get_resources()` and resource-stream snapshots.
- Decide whether `get_resource_stream()` remains a synchronous context manager around an async iterator or is replaced with a simpler subscription API.
- Update queue and identity-queue wrappers for the restricted, name-addressed `put` API.
- Remove external writable-state and event operations; state is consumed from snapshots.

### WebSocket protocol

- Define initial subscription request and snapshot response shapes.
- Define missing-resource and workflow-terminal errors.
- Preserve authentication and identity authorization for name-addressed resources.
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
- deterministic name-addressed operations across replay and suspend/resume;
- repeated operations accepted while an ordinary queue remains active;
- state values included directly in snapshots and unavailable for external mutation;
- queues exposing only name-addressed `put` operations;
- no external event resource or arbitrary resource actions;
- no automatic publication solely because a queue write occurred;
- completion, suspension, and failure behavior once those semantics are decided.
