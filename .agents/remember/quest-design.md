# Quest: purpose, design, and contract boundaries

## Status and basis

This document synthesizes the framework author's explanations in a design interview, informed by consumer examples in `src/demos/`. It describes intended behavior, not a verification of implementation completeness. The author of this document did not inspect framework implementation code or existing tests.

The refactoring target is `src/quest/`; `src/v3` is the older version. Existing uses of v3 help explain the intended applications but do not establish that every capability is already implemented in the new version.

Statements about current limitations and future work below reflect the interview. They should not be read as permanent restrictions or promises about unimplemented features.

## 1. Purpose

Quest has two purposes:

1. **Describe a dynamic application interface in readable, writable imperative code.** A workflow describes how a user's experience—the public API—evolves over time. The workflow's control flow determines the state and actions available at each stage.
2. **Give workflow code durable execution through replay.** Durability makes long-lived imperative interfaces feasible, but it is also valuable independently: a workflow need not expose an interactive interface to benefit from resumability.

These purposes support both ordinary durable computation and interactions with human or agent participants.

## 2. Where Quest belongs in an application

Quest tracks durable action sequences: workflows whose progress should survive interruption. The workflow author decides which sequence needs to be performed durably and where its boundaries belong.

Quest is **not intended to contain an entire application's execution or complete application history**. APIs, event listeners, and other hosting infrastructure remain outside the durable workflow. They receive events, trigger or interact with workflows, and move on.

An interaction's business logic can reside entirely in a workflow while its UI renders the current scene. That does not mean the entire application, including its event listeners and hosting infrastructure, should run inside Quest.

## 3. Representative usage

### Resumable compute pipelines

Record progress through computation so an interrupted pipeline can continue without repeating completed recorded work. Store bulk data in an appropriate location and pass metadata through Quest layers. This is a usage recommendation, not a specified payload-size limit.

### Durable conversations

The author currently uses v3 for chatbot workflows that resume after server restarts. A conversation can be an ordinary loop that:

1. Obtains user input through a recorded step.
2. Appends the input to a local conversation-history list.
3. Calls a model API through a recorded step.
4. Appends the response to the list and repeats.

During replay, completed input and model-call steps supply their recorded outcomes. Deterministic ordinary code rebuilds the local history list. Previous input need not be requested again, and completed model calls need not be executed again.

### Multi-user interactions

Games and similar applications can place the interaction's business logic in a workflow. The UI renders each scene: the state and actions currently available to its participant. Dynamic resource streams fit naturally with React's rendering model.

### Agent task scaffolds

An AI agent can consume the same interface without a graphical UI:

- `State` supplies instructions.
- Available actions supply tools.
- Workflow control flow provides a deterministic sequence of phases, with specific instructions and tools available in each phase.

### External event routing

A Discord bot provides a concrete routing example. A conversation workflow creates a thread and publishes messages there. Incoming Discord messages identify that thread, not the original Quest workflow. The workflow registers the thread ID as an alias so the bot can route incoming messages to it.

## 4. Durable execution and replay

### Interruption and resumption

Durability means that if a workflow is interrupted—for example, by a system signal or process interruption—and the process halts, **a new process can resume the workflow where it left off**. This is cross-process continuity, not merely pausing and restarting within one process.

Resumption follows recorded completion boundaries:

- Completed steps rehydrate their recorded results rather than executing again.
- A step that did not complete starts again.
- Steps can be nested. An incomplete enclosing step starts again, but its completed substeps rehydrate rather than execute.
- Exceptions from steps are recorded and replayed too. A workflow can catch a step exception, and replay reproduces that exceptional outcome and the workflow's original handling of it.

This is not restoration of an arbitrary Python instruction pointer.

### Determinism is an author responsibility

Code between steps must be **100% deterministic** so replay follows exactly the same path. Ordinary code re-executes during reconstruction; Quest does not make arbitrary Python code deterministic.

Workflow code must replay recorded history with perfect fidelity. If history does not match code flow, an error is raised.

### Best-effort external effects

Quest takes a simple, best-effort approach. There is a gap between performing an external effect and recording its completion. A process interruption in that gap can cause the operation to execute again.

Quest therefore does not promise exactly-once external effects. Authors needing stronger guarantees must structure their workflows to support them, using mechanisms such as idempotent side effects and confirmation steps.

### Real time and downtime

Durable resumption does not imply uninterrupted execution. Authors interacting with real time must account for periods when workflows are inactive, including deadlines that pass while the process is stopped. The workflow determines how to handle that situation; the interview established no automatic application-specific deadline behavior.

## 5. Evolving code while preserving history

Authors are responsible for maintaining replay compatibility when workflow code changes.

The time-travel API supports compatibility branches. `src/demos/time_travel.py` imports `when()` and `get_current_timestamp()` from `quest` and compares workflow time against a code-release timestamp to select old or new code paths.

The author's definition is:

> `when` reports the timestamp of the last quest event to be replayed, until all quest events have been replayed, at which point it reports the current time.

The demo explains that these timestamps are anchored to Quest events, including steps, tasks, and resource creation/actions. Time-based branches let old histories follow compatible paths while later execution uses new paths. They do not remove the requirement for faithful replay.

## 6. The exposed surface is the interface

State and actions form a temporally controlled application interface.

> The exposed interface IS the interface. If it isn't exposed, it doesn't exist.

Exposure is not merely discovery of independently callable operations. Knowing an action's name, or anticipating that it will become available, does not make it invocable.

This also applies during replay: actions are not published until replay is complete and cannot be invoked before then.

### Identities, ownership, and scope

Every state and action is associated with an identity:

- `None` denotes a public resource, visible to all identities.
- Other resources can be observed or invoked only through the identities a caller presents.
- Identities can represent roles as well as actors.
- Multiple users can share a role identity.
- Different roles can have overlapping or identical scope.

For example, a workflow author can make confirmation available to several users by publishing an action under multiple identities or by giving the users access to a shared role identity.

Identities can originate from code or configuration, workflow arguments supplied by the application, or `IdentityQueue` interactions.

### IdentityQueue

An `IdentityQueue` accepts an item and returns a unique identity to the submitter. The workflow consumes a tuple containing that same identity and the submitted item.

This establishes a relationship the workflow can use to direct later information or actions: “whoever gave me this value is the intended recipient of this information.”

### Authentication belongs to the application

Quest's identity model provides resource ownership and scope; the hosting application enforces authentication and real-user identity.

An application may map an authenticated user to a stored set of application identities and pass those identities to Quest. The application establishes the caller's entitlement to present them; Quest applies resource scope using them.

### Action arguments

Consumers currently know action arguments **by convention**. Schemas accompanying actions are a TODO, not a current guarantee. Agent-tool use does not imply automatic schema generation.

## 7. Resource streams and client behavior

### Current views, not event logs

The resource stream always returns the current view. Changes to resources for the specified identities cause a new snapshot to be published. If multiple changes occur before the next snapshot is retrieved, they coalesce: only the latest view is returned.

Consumers must not depend on receiving every intermediate scene or state transition.

During reconstruction, the stream publishes only once the last recorded event has been replayed. Consumers do not see historical scenes traversed during replay.

### Availability races

An advertised action is not reserved for a caller. It can disappear before invocation.

Behavior depends on workflow code and Python's single-threaded async execution. In the example discussed, one invocation triggers resource cleanup and a subsequent invocation is evaluated after that resource has ceased to exist. Clients must handle these races gracefully.

Single-threaded async should not be interpreted as a promise that arbitrary action bodies are atomic across awaits.

### Completion and failure

- On normal workflow completion, the resource stream closes.
- An unhandled workflow exception propagates through the resource stream iterator and is terminal for the workflow.
- The proxy carrying the stream to outside consumers must communicate completion or handle and communicate the error.

Quest's stream lifecycle is distinct from the application's external transport protocol.

## 8. External action invocation

An external action failure fails **only that invocation**, not the workflow. This differs from an unhandled exception in workflow code, which is terminal.

The first step of `invoke_action` is to record the request so it will complete after restart. A successful return means the invocation has been recorded and survives an immediate process interruption. This recorded-request recovery does not remove the best-effort boundary around external effects.

Quest does not currently provide a way to recover an individual invocation's outcome when its request was recorded but its response was lost. That is distinct from retention of a workflow's final result or exception.

Authors can provide their own confirmation mechanism. For example, an action can publish state for the caller to observe rather than relying only on delivery of its invocation response.

## 9. Async tasks and author responsibilities

`@task` wraps a function in a Quest-managed task. Authors should use it instead of `asyncio.create_task` for concurrent workflow work. Managed tasks are tracked in Quest history, which matters for successful replay.

Quest remains an async framework. Authors are expected to understand async programming's tradeoffs and requirements, including managing the lifetimes of the tasks they start. Starting a managed task and returning without awaiting it is analogous to doing so with `asyncio.create_task`; history tracking is not a replacement for managing async lifetimes.

If authors want permanent cancellation, they must currently design a mechanism to signal and handle it in their workflow. The interview established no universal built-in operation for permanently cancelling arbitrary workflows.

## 10. Workflow management and persistence

`WorkflowManager` is responsible for finding unfinished workflows and starting their replay after restart.

- `BlobStorage` represents the storage layer.
- The manager uses it to remember unfinished workflows and resume them.
- `PersistentHistory` implementations can use it to record ledger events.
- Each running manager should have its own namespace.
- The manager retains a completed workflow's **result or exception**, effectively serializing a Future's outcome so observers can retrieve it later, including after restart.

`src/demos/many_workflows.py` shows a manager configured with a namespace, storage, a history factory keyed by workflow ID, a workflow factory keyed by workflow type, and a serializer. Its in-memory restart illustration is not itself a fresh-process durability test; cross-process continuity is the intended contract described above.

### Recorded values

Values recorded in history must currently be storable in JSON. Exceptions must be reconstructable from their qualified type name and arguments.

Other options, such as pickle and Pydantic, are being explored; they are not current guarantees.

### Aliases

An alias is a **temporary alternate workflow ID**, chosen by the workflow, through which `WorkflowManager` can route external actions. Normally routing uses the original workflow ID. An alias allows a workflow to be reached using an identifier known to an external system, as in the Discord thread example.

Routing aliases should not be confused with identities used for resource ownership and scope.

### Coordination between workflows

Authors can devise coordination through queues or external APIs, including calling an API whose behavior is managed by another workflow. The interview did not establish a special built-in parent/child workflow or inter-workflow orchestration contract.

## 11. Responsibility summary

| Concern | Quest's role | Author or hosting application's role |
| --- | --- | --- |
| Durable progress | Record history and replay completed outcomes; resume unfinished execution. | Choose workflow and step boundaries; keep code between steps deterministic. |
| External side effects | Best-effort recording and recovery. | Provide idempotency, confirmation, or other stronger guarantees when needed. |
| Evolving interface | Scope the published state/actions over time; expose current snapshots. | Define phases, operations, and consumer conventions. |
| Identity | Apply resource ownership and scope, including public resources. | Authenticate users and determine permitted identities. |
| Async work | Track managed tasks in history. | Manage async control flow and lifetimes. |
| Client communication | Close streams on completion; propagate workflow errors through iteration. | Translate those outcomes into the external protocol and handle availability races. |
| Persistence | Manage unfinished workflows and retained outcomes through configured storage. | Supply storage and application workflow configuration. |
| Code changes | Detect replay mismatches; provide event-time branching support. | Preserve replay-compatible paths. |
| Time and cancellation | Resume durable execution after interruption. | Account for inactivity and implement application-specific deadlines and cancellation. |

This document deliberately does not fill in contracts that were not established in the interview. New requirements or ambiguities should be discussed with the framework author rather than inferred from implementation details.
