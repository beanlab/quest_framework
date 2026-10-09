# Resource Generations

## Status

Deferred. This document preserves the context, motivation, and likely extension path for resource generations. Generations are not part of the current resource-stream design and should be added only when concrete experience justifies them.

## Current addressing model

Resources have stable, name-based addresses within a workflow. The current `resource_id` is derived from the resource name and optional identity:

```python
_create_resource_id("messages", "user-1")  # "messages|user-1"
_create_resource_id("register", None)       # "register"
```

This is a semantic address, not a unique resource-lifetime identifier. Only one resource with a given `(name, identity)` may be active at a time, but the same address may be reused after deletion.

Stable names are intentional and useful. Some integrations know a workflow's designed public interface and write directly to a queue such as `messages` without first discovering resources. Many different workflow implementations may expose the same named interface. Requiring callers to discover and retain a unique ID for every workflow instance would make these integrations more complicated.

The model is analogous to a REST API: the workflow ID, resource name, and identity form a stable path.

## Motivation for generations

A delayed caller may act on a resource after its lifetime has ended. If a new resource is later created at the same `(name, identity)`, a name-based write can reach the replacement even though the caller observed or intended the earlier resource.

A generation would identify one exact resource lifetime. A caller that discovered a resource from a snapshot could provide its generation as a precondition. The runtime would reject the operation if the currently active resource had a different generation.

This is analogous to an HTTP `ETag` supplied through `If-Match`: the stable name remains the address, while the generation optionally constrains which version or lifetime may receive the operation.

A generation is a correctness token, not an authentication mechanism.

## Why it is deferred

No concrete application failure currently requires stale-lifetime protection. Name-addressed queue writes are simple and support existing static integrations. Adding mandatory generations now would impose discovery and tracking requirements on every caller.

An optional generation can be added later without changing the underlying name-based addressing model or resource-stream architecture. Deferral follows the project preference to add complexity only after concrete experience demonstrates its value.

## Likely future API

A future queue-write API could accept generation as an optional precondition:

```python
await put_queue(
    workflow_id,
    name="messages",
    identity=user_id,
    value=message,
    generation=generation,  # optional
)
```

Semantics would be:

- omitted generation: target the currently active resource at `(name, identity)`;
- supplied matching generation: perform the write;
- supplied nonmatching generation: reject it with a dedicated stale-resource error;
- no active resource: report that the named resource does not exist.

Static integrations could continue using stable names without discovery. Snapshot-driven clients could opt into exact-lifetime validation.

Making generation mandatory later would be a breaking API change. Adding it as an optional precondition is a clean extension.

## Likely generation source

Quest's deterministic `make_unique_id()` mechanism could generate a lifetime identifier during resource registration, for example from:

```python
make_unique_id(f"{resource_id}.__generation__")
```

The result would be stored rather than recomputed during external access. A delete/recreate cycle would receive a new generation, while replay of the same logical creation would reconstruct the same generation.

The existing resource-creation lifecycle `step_id` was also considered. Reusing it directly would expose an internal history identifier and couple the external protocol to the step-ID structure. Generating a dedicated value through the same deterministic mechanism would avoid that conflation.

The exact generation representation should be decided when the feature is implemented.

## Likely implementation changes

Adding optional generations would be cross-cutting but not a deep refactor, provided queue writes remain centralized in the resource runtime. Expected changes include:

1. Add generation to `ResourceEntry`.
2. Generate it during resource registration.
3. Store it in `create_resource` lifecycle history.
4. Include it in resource snapshots.
5. Accept an optional generation on external queue writes.
6. Validate a supplied generation before calling `put()`.
7. Store the supplied generation in external resource-access history.
8. Reproduce and validate generation semantics during replay.
9. Update client wrappers, WebSocket payloads, errors, and tests.

Adding a snapshot field is additive for clients that tolerate unknown fields. Adding a keyword argument with a default of `None` is backward compatible for direct callers.

## History compatibility

Older history records will not contain generations. They should retain unconditional name-based semantics rather than having a generation inferred retroactively.

New external records should distinguish between:

- no generation precondition supplied; and
- a specific generation supplied and validated.

Replay must preserve that distinction. It must not strengthen an old unconditional operation into a generation-bound operation.

## Alternatives considered

### Mandatory generation

Every queue write would include `(name, identity, generation)`. This gives strong stale-write protection but forces all callers to discover and track each resource lifetime. It conflicts with static integrations built around stable named queues.

### Unique lifetime resource IDs

`resource_id` could be changed from a semantic name-based address into a unique lifetime ID, with external writes targeting that ID directly. This naturally rejects stale writes but removes stable name-based addressing and creates the same discovery burden as mandatory generations.

### Name-only addressing

This is the current and simplest model. It intentionally targets whichever resource is active at the stable address. It remains appropriate until stale-lifetime behavior causes a concrete problem.

## Non-goals

Generations would not provide:

- authentication or identity authorization;
- deduplication;
- idempotency;
- one-response-only queue behavior;
- application acknowledgement policy;
- request correlation.

Those concerns remain separate workflow or application policies.

## Reasons to revisit

Revisit this feature if concrete experience shows one or more of the following:

- delayed UI input reaches a replacement resource incorrectly;
- reconnecting clients need optimistic concurrency for queue writes;
- workflows frequently reuse the same resource name for distinct interactions and stale input becomes observable;
- an integration requires an explicit exact-lifetime operation contract.
