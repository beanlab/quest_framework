# Resource Stream Revisit

## Status

This document captures the intent, motivation, settled decisions, and open design questions from the resource-stream discussion on the `resource-stream-revisit` branch. It is an evolving design record, not yet an implementation plan. Sections explicitly labeled as remaining design questions are not settled.

External-event execution and replay can be refactored independently and are now specified in [external-events-refactor.md](external-events-refactor.md). This document retains only the external-action details that directly define the resource surface and its integration boundary.

## Project intent

Quest is a framework for expressing long-running workflows that control a changing **resource surface**.

A workflow publishes resources while they are relevant. Each resource class declares the state values and actions it exposes externally. A user interface observes the current resource surface, decides how to render it, and may invoke exposed actions that allow the workflow to proceed.

For example, a workflow might expose separate built-in `state` and `queue` resources, or a custom `scene` resource that combines visible values with actions backed by its own private queues. A UI can combine multiple resources into one component or render one richer resource directly. A web UI, CLI, and mobile UI may render the same workflow surface differently.

Quest's responsibility is to provide useful, flexible, correct, and sound workflow/resource semantics. It is not intended to enforce application-level business practices such as acknowledgement policies, idempotency, deduplication, or whether extra queue inputs should be consumed or ignored.

## Separation of concerns

### Workflow responsibilities

The workflow and its resource classes control:

- which resources currently exist;
- resource names, identities, types, and values;
- which values and actions each resource exposes externally;
- when the workflow waits for resource interactions;
- how actions and inputs are interpreted;
- when exposed values change and should be republished;
- when resources are changed or withdrawn.

### UI responsibilities

The UI controls:

- component selection;
- layout and styling;
- static text and labels;
- localization;
- how multiple resources compose into a component;
- how missing or temporarily incomplete resource combinations are rendered.

If text or labels are dynamic workflow-controlled content, the workflow can expose them as values on built-in or custom resources. Quest does not need presentation metadata such as component names, layout, or styling in its core resource model.

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
- **workflow progress**: an intended user invoked a resource action or supplied input the workflow needs.

A UI subscribing, rendering, or asking for another update should not advance the workflow. Likewise, an inactive UI should not prevent unrelated workflow execution.

Required interaction sequencing should be represented explicitly in workflow logic. If a workflow needs user input, it should expose an appropriate action—such as `put` on a queue—and await the corresponding internal input. If it does not need input, it should be free to continue regardless of subscriber behavior.

## Agreed design direction

### 1. The resource surface is authoritative latest state

Resource streaming should represent the latest workflow-controlled surface, not an irreplaceable sequence of events.

A subscriber needs:

- the current surface immediately on subscription;
- notification when a newer surface is available;
- the newest surface after reconnecting.

It does not need every intermediate surface. Intermediate revisions may be skipped.

### 2. Publication must not backpressure workflow execution

A resource-surface change should do only fast, local publication work:

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

Resource lifetimes and actions are sufficient to express required sequencing. No UI-specific acknowledgement primitive is currently needed.

For example:

```python
async with state("message", user_id, "Confirm purchase?"):
    async with state("warning", user_id, "This cannot be undone"):
        async with queue("confirmation", user_id) as confirmation:
            answer = await confirmation.get()
```

The supporting states are created before the queue resource. Contexts unwind in reverse order, so the queue disappears before its supporting states. A UI can treat the key queue resource as the readiness marker and render only when it is present. A custom resource may instead combine the same visible values and actions into one resource-defined external surface.

UIs are expected to tolerate incomplete combinations of resources by waiting for the key resources they need. Atomic multi-resource publication or UI-specific grouping is not currently required.

### 6. UI contracts remain dynamic

Strongly typed UI contracts are not required at this stage.

Resource names and runtime resource types form a dynamic interface in the same spirit as names in Python or JavaScript. Conventions and tests are sufficient while the design is experimental.

Typed schemas, generated bindings, or stronger contracts may be added later if concrete experience demonstrates their value. Action parameter and return schemas are explicitly deferred until after the core refactor and are documented in [snapshot-schema.md](snapshot-schema.md). The current protocol exposes action names only.

### 7. Resource classes declare their external surface

The historian and workflow manager retain their generic external-event interfaces. They route and remember interactions without containing type-specific knowledge of `state`, `queue`, or custom resources. Client libraries may provide meaningful, protective type-specific interfaces over that generic backend.

A resource class declares its external surface through decorators with two independent roles:

- `@external_value` marks a getter whose current result is included in the resource representation;
- `@external_action` marks a method that may be invoked through the generic external-event interface and whose name is included in that representation.

External access is default-deny. An unannotated method remains available to workflow code through the internal resource interface but cannot be called externally.

The built-in resources use this generic mechanism to preserve directional communication:

- `state` exposes its current value through `@external_value` and exposes no external mutation action;
- `queue` exposes `put` through `@external_action` and does not expose `get` or its contents;
- `identity_queue` exposes `put` through `@external_action` and may return the established identity.

Published values remain the authoritative channel for workflow state and ongoing workflow output, but external actions are not required to return `None`. An action result may carry immediate protocol information needed to complete an interaction. `identity_queue` is the motivating case: it creates an identity, delivers that identity with the value to the workflow, and returns the same identity to the previously unidentified caller. The caller can then observe resources published privately for that identity. This bootstrap handshake requires both sides to learn the same value unless identity instead comes from authentication or the client. Such a result is not published workflow state and is not part of the resource snapshot.

A workflow author may define a custom resource that combines exposed values and actions—for example, a scene containing visible choices and actions that write to a private underlying queue. The historian does not know how the resource implements those actions.

Business acknowledgement, idempotency, and interpretation remain application concerns. The generic mechanism permits custom external actions, but only those explicitly declared by the resource author.

### 8. Resources retain stable name-based addresses

Resources continue to be addressed by workflow, name, and identity. Stable names are part of a workflow's designed public interface: an integration may intentionally invoke an action on the current resource at an address without first discovering a particular resource lifetime.

A delayed operation can therefore reach a replacement resource created later at the same `(name, identity)`. The operation succeeds only if the current replacement exposes the requested action. Optional resource generations could provide REST-like conditional-write semantics for clients that need exact-lifetime validation, but no concrete application currently requires that complexity. Generations are deferred and documented separately in [resource-generations.md](resource-generations.md).

### 9. Snapshot revisions are not part of the external protocol

The client protocol does not include a snapshot revision. WebSocket messages are ordered within a connection, each connection has one sender, and the initial snapshot after reconnection authoritatively replaces the client's prior surface.

The runtime does not need an internal or client-visible version counter. A per-subscription dirty event is sufficient; harmless redundant snapshots are preferred over version-tracking complexity. A revision can be added later if concrete experience demonstrates a need.

### 10. Publication triggers are resource-declared

Resource creation and deletion always mark relevant subscriptions dirty. Resource methods declare other publication triggers with `@invalidates_snapshot`.

After a snapshot-invalidating method terminates—successfully or exceptionally—the historian marks relevant subscriptions dirty. The decorator is independent of external visibility: `State.set()` invalidates snapshots but is not externally callable, while a custom action may be both `@external_action` and `@invalidates_snapshot`. Queue operations need not invalidate snapshots when the queue exposes no values.

The historian remains resource-agnostic: it recognizes decorator metadata but does not encode which resource types or action names change published state. Resource authors are responsible for marking each resource-boundary method that may make the previous external snapshot stale. Mutations that bypass snapshot-invalidating methods cannot automatically be detected.

Each trigger only marks subscriptions dirty; it does not construct or push a snapshot and does not wait for a subscriber. This replaces notification after every internal resource event and avoids redundant snapshots from operations such as `queue.get()`.

### 11. Queue policy belongs to workflows and applications

Ordinary queues should retain ordinary repeatable queue semantics. If a queue remains active, it may accept multiple inputs. A workflow may consume all, consume some, or ignore extras.

A write fails when no resource is active at the named address. If a resource is replaced at the same address, name-based callers intentionally target the current resource. This does not impose one-response-only behavior during an active resource lifetime.

Quest should not impose one-shot behavior, acknowledgement policy, deduplication, idempotency, or business-level interpretation on all applications. Such behavior can be expressed by workflow code or optional abstractions if needed.

## Proposed runtime shape

The design suggests the following separation:

### Resource runtime

The resource runtime owns the authoritative registry and resource visibility. It introspects resource decorator metadata to construct generic external representations, authorize external actions, and recognize snapshot-invalidating method completion. Creation, deletion, and snapshot invalidation mark relevant subscriptions dirty without constructing a snapshot or waiting for subscribers.

### Subscription

A subscription contains approximately:

- subscriber identity;
- a dirty event, initialized as set;
- closed state;
- workflow terminal state and, for failure, the original error.

Registration does not await: it adds the subscription and initializes the dirty event as set so the sender immediately reads the current surface. It does not contain a snapshot queue, version counter, or gate that workflow execution waits upon. Closing or terminating a subscription wakes any blocked `anext()` call so it can stop or raise the appropriate terminal signal.

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

Context exit unregisters the subscription and wakes local iteration. Workflow completion, suspension, or failure also wakes iteration and records a terminal reason. The exact precedence between a pending dirty snapshot, an in-progress send, and terminal state remains a transport/lifecycle design question; no subscriber race may delay workflow termination.

### External operation

An external operation identifies:

- workflow;
- target resource name;
- target resource identity;
- action name;
- arguments.

The runtime finds the current resource by `(name, identity)`, verifies that the requested method is marked `@external_action`, invokes it, and records the name-addressed interaction for replay. The stable name targets whichever resource is currently active at that address. Resource-specific and language-specific clients may expose safer convenience APIs over this generic operation.

The authenticated caller identity is distinct from the target resource identity in the address. Action authorization and its relationship to durable launch are covered by [external-events-refactor.md](external-events-refactor.md). Resource observation authorization remains part of this stream design.

## Replay and determinism

The stream model must preserve Quest's replay guarantees:

- replay reconstructs the authoritative current resource surface;
- replay does not emit historical intermediate surfaces as though they were live changes;
- suspension and resumption preserve the intended active resource surface.

The external-operation history model, concurrent replay, generated results such as identities, failure semantics, and the determinism contract for custom actions are specified independently in [external-events-refactor.md](external-events-refactor.md). The stream refactor must not depend on the current single-record external replay mechanism.

## Scope boundaries

This design deliberately does not decide application policy for:

- whether extra queue entries are errors;
- whether user input requires acknowledgement;
- whether actions are idempotent;
- whether requests need application-level correlation IDs;
- whether a UI disables controls after submission;
- how consequential business operations are modeled.

Quest should make these policies possible, not mandate them.

## Detailed decisions

The following sections elaborate settled decisions and the current working protocol shape. Any details that remain open are identified explicitly and summarized under “Remaining design questions.”

### Subscription API and lifecycle

The local API is named `subscribe_resources()`. It replaces `get_resource_stream()` without a deprecated alias. `get_resources()` remains the one-shot current-surface API, while `subscribe_resources()` clearly communicates registration and an ongoing lifecycle.

A subscription is a synchronous context manager and an asynchronous iterator:

```python
with manager.subscribe_resources(workflow_id, identity) as subscription:
    async for snapshot in subscription:
        ...
```

Entering the context synchronously registers the subscription, and exiting unregisters it deterministically. Registration and cleanup require no asynchronous I/O, so an asynchronous context manager would add asynchronous syntax without asynchronous work. A bare async generator is not used because its body would not register until iteration begins and early loop exit would make cleanup easier to omit.

Each subscription is single-consumer and single-use. The subscription is its own async iterator. Concurrent `anext()` calls, multiple consumers, or renewed iteration after termination are usage errors. Independent consumers create independent subscriptions.

Local terminal behavior is:

- workflow completion ends iteration normally;
- workflow suspension raises `WorkflowSuspended`;
- workflow failure raises `WorkflowFailed` from the original exception.

`WorkflowFailed` exposes the standard chained exception through a read-only public property without a custom initializer:

```python
class WorkflowFailed(Exception):
    @property
    def cause(self) -> BaseException | None:
        return self.__cause__
```

Quest raises it with standard chaining:

```python
raise WorkflowFailed("Workflow failed") from original_error
```

Quest guarantees that `WorkflowFailed` instances emitted by subscriptions have a non-`None` cause, though the property is typed as optional because callers can manually construct the exception without chaining it.

### Snapshot contents

A snapshot is a plain list of generic resource records, with no envelope. The current shape gives each record its stable address, resource type, exposed state, and a list of exposed action names. Parameter schemas, return schemas, and action descriptions are not included in the current protocol; that possible later feature is documented in [snapshot-schema.md](snapshot-schema.md).

```json
[
  {
    "type": "state",
    "name": "message",
    "identity": "user-1",
    "state": {"value": "Hello"},
    "actions": []
  },
  {
    "type": "queue",
    "name": "response",
    "identity": "user-1",
    "state": {},
    "actions": ["put"]
  }
]
```

The runtime builds `state` from getters marked `@external_value` and `actions` as a list containing the names of methods marked `@external_action`. An exposed-value getter must be synchronous, accept no arguments beyond `self`, and be side-effect-free. These constraints keep snapshot construction synchronous and deterministic; side-effect freedom is a resource-author contract rather than something the runtime can fully enforce. State is included directly rather than fetched through separate external operations. Action arguments and return values do not publish schemas in the current protocol; validation belongs to resource, workflow, or application logic.

Exposed state values must be supported by Quest's configured serializer because they are included in snapshots. External action arguments and results must likewise be serializable when they are transported or recorded, but they are not part of the snapshot record. If any exposed-value getter raises, construction of the entire snapshot fails. The runtime must not omit the affected resource, because omission would falsely represent it as withdrawn, and the protocol does not embed per-resource error records.

A snapshot-construction failure affects only the one-shot read or subscription performing that construction. It does not fail, suspend, or otherwise alter the workflow, historian, or other subscriptions. Exposed-value reads are observer-side operations: they are neither recorded in history nor replayed. An author may therefore fix a faulty getter and resume observation without changing workflow history. A failed subscription terminates; a new read or subscription may retry against the current resource surface.

Locally, both `get_resources()` and subscription iteration report this failure by raising `ResourceSnapshotFailed` from the original getter exception. The dedicated exception lets consumers distinguish an observation failure from a workflow failure, while standard exception chaining preserves the underlying bug.

Presentation metadata remains out of scope. By default, a resource's external type identifier is its package-qualified class name (`__module__` plus `__qualname__`). This disambiguates unrelated classes with the same class name, while treating a class move or rename like a change to any other public package reference. A resource may instead use `@resource_type("...")` to declare a stable observer-facing protocol type that remains unchanged when its implementation class moves or is renamed. An explicit type identifier is optional, not required. The built-in resources use explicit identifiers such as `state`, `queue`, and `identity_queue`, which is why the examples use those concise values.

An exposed value's state key and an external action's action name are the decorated Python member's name. The decorators do not currently accept observer-facing aliases. External action routing can therefore continue to find the requested member directly with `getattr()` and then verify its decorator metadata; it need not scan every annotated method for a matching alias. Renaming a decorated member is an external protocol change. Aliases may be added later if concrete need justifies the additional routing complexity.

Resource-record order, state-key order, and action-name order are not protocol semantics and are explicitly unspecified. The surface is conceptually a set of resources keyed by address, each containing a state mapping and a set of actions. An implementation may happen to emit a stable order, but observers must not rely on it or use resource position as identity. This keeps presentation ordering and layout under UI control.

Decorator metadata follows ordinary Python inheritance and wrapping semantics; Quest adds no inheritance machinery. An inherited decorated method retains its metadata through normal attribute lookup. An override replaces the inherited method and is exposed only if the override is itself decorated. Multiple inheritance follows the normal method resolution order. Wrappers built with `functools.wraps()` preserve decorator attributes in the usual way.

If snapshot construction fails during a WebSocket subscription, the server closes only that connection with a dedicated application close code and a generic reason such as `resource snapshot failed`. The original exposed-value getter exception is logged server-side but is not sent to the client, avoiding both implementation-detail leakage and an error envelope in the plain-list snapshot protocol. Other remote transports should likewise report a transport-appropriate typed subscription failure without altering the workflow.

### Publication triggers

Resource creation, resource deletion, and termination of methods marked `@invalidates_snapshot` mark subscriptions dirty. Snapshot-invalidating methods do so after successful or exceptional termination because a failing method may already have mutated exposed state. These triggers do not construct snapshots or wait for consumers.

Snapshot invalidation applies to calls routed through the resource boundary. Resource authors must mark the boundary method that performs or initiates a visible mutation. Later mutations performed by detached background work are not detected automatically. `@invalidates_snapshot` follows the ordinary Python inheritance and wrapping semantics defined above.

### Initial subscription and lost-update safety

Registration synchronously adds the subscription with its `asyncio.Event` already set. The sender clears the event immediately before reading the latest surface. A concurrent change either appears in that read or leaves the event set for another pass. No internal version counter is needed; redundant snapshots are acceptable. Context exit and workflow terminal state set the event so blocked iteration can wake and terminate.

### Completion, suspension, and failure

The stream sends no terminal snapshot or status envelope. Its termination reason distinguishes outcomes:

- completion ends the async iterator normally and closes the WebSocket normally;
- suspension raises a dedicated `WorkflowSuspended` signal locally and will use a defined application WebSocket close code;
- workflow failure raises `WorkflowFailed` locally, with the original exception available through `cause`, and will use a distinct failure close code;
- abnormal transport closure represents network loss rather than a managed workflow outcome.

Clients discard the displayed surface when the stream ends. Resumption requires a new subscription. Durable workflow status, results, and retention remain separate `WorkflowManager` API concerns.

### Deferred resource generations

Optional exact-lifetime preconditions are outside the current scope. Their motivation, compatibility strategy, and likely implementation are preserved in [resource-generations.md](resource-generations.md).

## Remaining design questions

The direction above is settled, but the following details require explicit decisions before implementation is fully specified.

### Observation authorization

- Distinguish authenticated observer identity from target resource identity in manager and transport APIs.
- Define who may observe public resources and how private-resource visibility is enforced.

### Subscription and transport termination

- Define terminal precedence when a snapshot is dirty or a send is already in progress.
- Define WebSocket close codes and whether failure details are transported separately from the close reason.
- Define missing-resource and workflow-terminal operation errors.
- Define reconnect behavior, heartbeat/liveness handling, and bounded socket/server buffering.

### Multiple subscribers

- Confirm behavior for multiple tabs or devices using the same identity.
- Ensure each sender can stall or disconnect independently.
- Confirm that no subscriber lifecycle operation affects workflow progress.

## Remaining implementation work

After the open design questions are resolved, implementation includes:

- add `@resource_type`, `@external_value`, `@external_action`, and `@invalidates_snapshot` metadata and introspection;
- update `get_resources()` and subscriptions to return the generic resource records;
- implement `subscribe_resources()` as the agreed synchronous context manager and single-use async iterator, replacing `get_resource_stream()` without an alias;
- integrate with the generic Historian and manager external-operation boundary defined in [external-events-refactor.md](external-events-refactor.md), restricting routing at runtime to actions declared by each resource;
- build protective type-specific convenience interfaces in client libraries, with exposed state consumed from snapshots rather than fetched separately;
- implement the initial WebSocket subscription request and snapshot serialization.

## Verification

Tests should cover at least:

- immediate current snapshot on subscription;
- identity visibility rules;
- a workflow progressing while a listener never consumes updates;
- a stalled WebSocket not blocking workflow execution;
- slow listeners skipping intermediate states and eventually receiving the latest state;
- multiple independent subscribers;
- reconnect and current-state resynchronization;
- repeated operations accepted while an ordinary queue remains active;
- exposed values included directly in snapshots;
- multiple exposed values and configured serialization behavior;
- exposed-value getter failure behavior, including local exception chaining and isolated WebSocket closure with the dedicated application close code;
- default package-qualified resource types and explicit `@resource_type` overrides;
- snapshots listing only the names of `@external_action` methods;
- only `@external_action` methods callable externally;
- unannotated methods rejected by external routing;
- custom resources mixing exposed state and actions;
- decorator inheritance, overrides, and workflow wrappers;
- snapshot-invalidating internal and external methods marking subscriptions dirty;
- snapshot-invalidating methods marking dirty after exceptional termination;
- queue operations without `@invalidates_snapshot` producing no automatic publication;
- authenticated observer versus target-resource visibility authorization;
- context exit or workflow termination waking blocked iteration;
- completion, suspension, and failure behavior, including the selected local signals and, once defined, WebSocket close codes.
