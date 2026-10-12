# Quest Core Refactor: Values, Actions, Tasks, and Steps

## Status

This document captures the current design direction from the core-refactor discussion. It reframes and broadens the external-event design in [external-events-refactor.md](external-events-refactor.md). The earlier document remains useful background for concurrency, replay, failure, suspension, and pruning concerns, but its assumption that external actions are operations on resources is superseded here.

The conceptual model is settled at a high level. Exact history schemas, completion and failure records, suspension, cancellation, compatibility, and some namespace behavior remain to be designed.

## Motivation

Quest currently treats an external event as a call to an action on a particular resource. That is too narrow. Calls from outside workflow execution all have the same fundamental role, including the call that starts the workflow:

1. An external caller selects a registered action.
2. The caller supplies inputs, possibly none.
3. Quest durably records the invocation.
4. Quest starts the action in a new historian-managed task.
5. The action may inspect or modify values and resources in the historian's namespace.

An external event should therefore not be modeled as resource access. It is an **action invocation** that may affect resources.

This model also clarifies the different replay contracts:

- Actions remember their inputs and are re-executed.
- Steps remember their outputs and completed steps are not re-executed.
- Tasks provide independent durable execution and replay streams.

The refactor should make workflow start, later external input, and dynamically exposed actions instances of the same mechanism rather than separate lifecycle concepts.

## Core model

A historian tracks a dynamic namespace containing **values** and **actions**.

### Values

A value is a synchronous function that reads and returns data from the namespace. Values expose current application state but do not create durable tasks or history records merely by being read.

Values are dynamically registered and unregistered as namespace scopes enter and leave their lifetimes.

### Actions

An action is an externally invokable function registered in the historian namespace. Invoking an action is an external event.

Every action invocation:

- identifies a namespace action;
- supplies recorded positional and keyword inputs;
- is durably recorded before the start API reports success;
- starts a new historian-managed task;
- returns or exposes that task to the live caller;
- is re-executed from its recorded inputs during replay.

Actions are not intrinsically attached to resources. Their implementations may read or mutate any appropriate state or resource in the historian's shared namespace.

Workflow start is invocation of an initially registered action, conventionally `main`. It is not a separate kind of durable execution.

### Tasks

A task is a historian-managed `asyncio.Task`, not an operating-system thread. It is an independent durable execution unit with its own replay stream, prefix/call-stack state, and nested steps.

Tasks may be started by external action invocations or by other running tasks. Concurrent tasks share the historian's namespace but must not share task-local replay or prefix state.

Task IDs are produced by deterministic task-creation rules. An action-invocation record is not expected to store the ID of the managed task it launches. During replay, repeating the same task creation in the same durable structure must derive the same task ID. Task-owned records then validate that structure.

An invocation record may still carry the shared external handler's task identity as history-routing metadata under the current implementation. That identity is not the launched action task's identity.

### Steps

A step is a recorded/replayed operation inside a task.

When a step has not completed, Quest executes it and records its output or exception. When it has completed, Quest restores the recorded outcome and does not re-execute the step body.

Nondeterministic work that influences replayed behavior must occur in steps. For example, random values and generated UUIDs used by an action must come from a step.

## Primary framework concepts

The intended primary decorators and primitives are:

- `@value`: publish a synchronous value in the current namespace;
- `@action`: publish an externally invokable action in the current namespace;
- `@task`: define a function that starts durable managed execution;
- `@step`: define an operation whose completed outcome is restored during replay.

The distinctions are:

```text
value  -> synchronous observation

action -> externally initiated task with recorded inputs

task   -> durable concurrent execution

step   -> durable outcome within a task
```

Action registration is dynamic, as resource/action exposure is today. A namespace scope controls the lifetime of the values and actions it publishes.

## Namespace and application identity

Application identity is a first-class part of Quest's data model. It is distinct from authentication identity.

- **Authentication identity** identifies the authenticated caller and belongs to the application, manager, or transport boundary.
- **Application identity** selects a logical Quest namespace partition.
- One authenticated caller may be authorized for multiple application identities or for none.
- Quest should not equate an authenticated user with one application identity.

A namespace member is structurally addressed by data equivalent to:

```text
(application_identity, namespace_path, member_name)
```

Examples:

```text
("123", "inputs", "message")
("123", "inputs", "supply_value")
("abc", "inputs", "message")
(None,  "status", "version")
```

This makes separation reliable rather than a filtering convention over a flat registry. Two application identities may expose the same namespace path without collision.

`None` is the candidate public namespace partition. A view for an application identity may include public entries and entries belonging to that identity, while preserving each entry's registered identity so addresses do not become ambiguous.

External APIs accept the target application identity explicitly. Authentication and authorization determine whether the authenticated caller may use that application identity before invoking the historian API. The historian still owns and enforces application-identity namespace isolation.

## Action recording and replay

### Durability rule

The central replay rule is:

> Actions remember inputs; steps remember outputs.

An action invocation record needs to identify the action and contain its serialized inputs. Its conceptual shape is:

```python
{
    "type": "invoke_action",
    "action": action_name,
    "args": serialized_args,
    "kwargs": serialized_kwargs,
}
```

The exact schema and routing metadata are not yet settled.

The top-level action result is a live task result. It is not an input to replay and need not be persisted as an action result merely to redeliver it to a historical caller. Durable effects and nondeterministic results needed during replay belong in steps.

### Live invocation

Conceptually:

```text
start_action(application_identity, target, inputs)
  -> resolve and validate the registered action
  -> serialize inputs
  -> durably append the action-invocation record
  -> start and register a managed task
  -> return the deterministic task ID
```

`start_action()` must be asynchronous. Starting a durable task requires a history write, and the API must actually wait for that work. This is representative of why most Quest interfaces are asynchronous.

`invoke_action()` is the convenience API that starts the action and waits for its result.

A provisional API is:

```python
class Historian:
    async def start_action(
        self,
        application_identity: str | None,
        target: str,
        *args,
        **kwargs,
    ) -> str:
        """Durably start an action and return its task ID."""

    async def invoke_action(
        self,
        application_identity: str | None,
        target: str,
        *args,
        **kwargs,
    ):
        """Durably start an action and wait for its result."""
        task_id = await self.start_action(
            application_identity, target, *args, **kwargs
        )
        return await self.get_task(task_id)

    def get_task(self, task_id: str) -> asyncio.Task:
        """Return the live task associated with a task ID."""
```

The exact lifetime and post-restart semantics of `get_task()` remain open.

### Replay

A shared external-action replay handler owns and consumes action-invocation records. For each record it:

1. resolves the dynamically registered action at the recorded namespace address;
2. rehydrates the recorded inputs;
3. invokes the normal managed-task creation mechanism;
4. derives the same task ID through deterministic rules;
5. does not await the launched task before advancing to later external records.

The launched action task consumes its own task and step records. It does not also consume the action-invocation record. This gives every history record one replay owner and allows multiple external actions to execute concurrently without sharing task-local replay state.

Replay must wait for all dynamically launched action tasks and record gates, not merely for the external-action handler to exhaust its own stream.

## Illustrative usage

```python
async def main(b=7):
    ident1 = "123"
    ident2 = "abc"

    @step
    async def get_random() -> float:
        return random.random()

    @task
    async def get_user_input(ident):
        class Request:
            def __init__(self, message):
                self._message = message
                self._queue = asyncio.Queue()

            @value(ident)
            def message(self):
                return self._message

            @action(ident)
            async def supply_value(self, value):
                await self._queue.put(value)

            async def get_value(self):
                return await self._queue.get()

        with namespace(
            "inputs", ident, Request("Supply a value for A")
        ) as request:
            return await request.get_value()

    a1_task = get_user_input(ident1)
    a2_task = get_user_input(ident2)
    a1, a2 = await asyncio.gather(a1_task, a2_task)

    return a1 * a2 + b * await get_random()
```

A caller can start `main`, inspect the identity-specific surface, invoke the dynamic actions, and later await the original task:

```python
historian = Historian(actions={"main": main})
main_task_id = await historian.start_action(None, "main", b=5)

historian.get_values("123")
historian.get_values("abc")

await historian.invoke_action("123", "inputs.supply_value", 7)
await historian.invoke_action("abc", "inputs.supply_value", 12)

result = await historian.get_task(main_task_id)
```

The two `inputs` namespaces have the same path but are isolated by application identity.

## Repeated action invocation

Quest should remain simple and unopinionated about repeated action invocation. Calling an initially registered action such as `main` more than once is allowed by the action model.

If two invocations attempt to register the same namespace members under the same application identity, normal namespace collision rules may cause one invocation to fail. Applications can avoid collisions with different identities or dynamically selected namespace paths. This is application behavior, not a special reason for Quest to prohibit invoking an action twice.

## Relationship to resources

Resources are no longer the durable external-event routing primitive. An action may affect a resource, but its invocation is addressed through the historian's action namespace and replayed as a task.

The historian should not need resource-type-specific knowledge to dispatch external events. Resource-like objects remain useful ways to package values, actions, queues, and private implementation state inside a namespace scope.

The namespace is the application-facing API. Dynamically constructing names or identities therefore dynamically constructs that API, which Quest may permit without prescribing application policy.

## Settled directions

The discussion has established these directions:

- External events include every action initiated outside workflow/task execution, including workflow start.
- External actions are not intrinsically tied to resources.
- A historian owns a dynamic namespace of values and actions.
- Values are synchronous functions for observing current namespace data.
- Every action invocation starts a separate historian-managed task.
- “Thread” in this design means a historian-managed `asyncio.Task`, not an OS thread.
- Actions record and replay inputs.
- Steps record and restore outputs.
- Action bodies re-execute during replay.
- Task IDs are generated deterministically by task creation rather than copied from the action record.
- The external replay handler launches action tasks without awaiting them.
- Action registration and namespace lifetime are dynamic.
- Application identity is first-class and structurally partitions the namespace.
- Authentication identity is separate and may map to zero or many application identities.
- Starting a durable action/task is asynchronous because history must be written before startup is acknowledged.
- Repeated action invocation is allowed; namespace collisions are ordinary execution outcomes.

## Remaining design questions

Before implementation is complete, the following need explicit decisions:

### History and task lifecycle

- Exact action-invocation record schema and serialization format.
- Exact task-start and task-completion schemas.
- Whether an action invocation record also serves as the generic task-start record or precedes a separate task-start record.
- Deterministic task-ID generation rules, including repeated invocations and nested tasks.
- How task success, action failure, cancellation, and suspension are represented and validated during replay.
- How expected action failure is prevented from becoming workflow-wide failure.

### Persistence ordering

- The exact atomicity or critical-section requirements among persisting an invocation, registering the task, and scheduling its coroutine.
- Recovery when a process stops after the invocation write but before live task creation.
- Whether serialization remains asynchronous independently of the asynchronous history write.

### Namespace behavior

- Exact registration and collision rules within one application identity.
- Whether public and private entries may share the same namespace path.
- How public and identity-specific entries are represented in a returned view.
- Whether private entries shadow public entries or remain separately addressed.
- Exact action resolution rules; public action invocation should not rely on an ambiguous fallback.
- Whether `get_values()` is the right name if the returned surface also describes available actions.

### Task API

- Whether `get_task()` exposes only current live `asyncio.Task` objects or supports completed/reconstructed tasks after restart.
- How long completed task objects or results remain available.
- The precise return behavior of `@task` wrappers and how task launch durability is exposed to callers.

### Suspension and cancellation

- What happens to active action tasks when the historian suspends.
- How launched but incomplete actions resume through replay.
- Whether cancellation of a caller waiting in `invoke_action()` cancels the durable action task.
- How abandoned live responses and task handles are cleaned up.

### Authorization

- Where the mapping from authentication identity to allowed application identities is enforced.
- How local trusted callers differ from remote authenticated callers.
- What public application identity means for inspection and invocation.
- Authorization must occur before unauthorized invocation is written to durable history.

### Compatibility and errors

- Whether histories containing the old resource-bound `external` records remain replayable.
- Dedicated replay errors for missing actions, changed inputs/signatures, missing namespace state, and incompatible code.
- Pruning rules for action-invocation records and all records owned by their launched tasks.

## Design principle

The core abstraction can be summarized as:

> A historian maintains an identity-partitioned namespace of values and actions. Invoking an action durably records its inputs and starts an independently replayable task. Tasks re-execute their code, while completed steps restore their recorded outcomes.
