# Working with Quest

Practical guidance for authoring workflows and testing their recovery. Read alongside [quest-design.md](quest-design.md) for purpose and contract boundaries and [quest-test-plan.md](quest-test-plan.md) for coverage, implementation status, and unresolved contract questions.

This guide describes the intended execution model and lessons from implementation/review, not a claim that every implementation path is correct. Do not turn implementation quirks or unresolved contracts into application guarantees.

## 1. Choose workflow and step boundaries

Quest tracks durable action sequences, not an application's entire execution. Keep hosting infrastructure, event listeners, authentication, and transport outside the workflow; route their interactions into its currently exposed interface.

Durability is reconstruction through recorded completion boundaries, not restoration of an arbitrary Python instruction pointer:

- Ordinary workflow code re-executes to reconstruct local variables, control flow, and resources. Code between recorded steps must be deterministic.
- A completed `@step` supplies its recorded result or exception without executing its body again.
- An incomplete step starts again. If an incomplete enclosing step contains completed substeps, those substeps supply their recorded outcomes.
- Put nondeterministic work and external effects behind appropriate step boundaries. Do not put an effect in ordinary reconstruction code and expect Quest to suppress it on replay.
- Recorded values must currently be JSON-storable; exceptions must be reconstructable from their qualified type name and arguments. Consult the test plan's open questions before relying on exact type fidelity or unsupported-value behavior.

Quest provides best-effort external effects, not exactly-once execution. An interruption after an effect but before its completion is recorded can repeat that effect. Use application idempotency keys, confirmation, or other application mechanisms when stronger guarantees are needed. For bulk data, store the data elsewhere and pass metadata through the workflow.

## 2. Tasks reconstruct; they are not cached steps

- Use `@task` instead of `asyncio.create_task` for concurrent workflow work.
- Calling a decorated task function starts managed execution and returns a task handle; await that handle to obtain its result or exception.
- During replay, ordinary task code runs again to reconstruct its authored path.
- Task completion does not imply that its entire body is skipped or its return value is cached. Completed steps inside it supply recorded outcomes.

For example:

```python
from quest import step, task


@step
async def fetch_value(key):
    # Perform an application-defined external operation here.
    return await external_lookup(key)


@task
async def worker(key):
    values = []
    values.append(await fetch_value(key))
    return values
```

On reconstruction, `worker` creates a new list. If `fetch_value` completed previously, it supplies its recorded result rather than repeating the lookup. The ordinary append reconstructs the list returned to the awaiter.

### Authors own child lifetimes

History tracking is not a substitute for async lifetime management:

- Keep task handles and await the tasks you start.
- If children need a stop mechanism, define one in workflow code and await their completion before the parent returns.
- A task started without being awaited is not an independently managed durable workflow.
- Do not assume universal permanent cancellation or automatic sibling-failure semantics beyond the async constructs the author actually uses.

## 3. Resource lifetime belongs to reconstructed execution

Create workflow-owned queues and other resources inside workflow execution and manage their scopes explicitly. Re-execution creates fresh resource objects and reconstructs their state through recorded operations; a retained live Python object is not durable recovery.

For shared resources, the parent workflow can create a resource and let its managed children use that same active instance. Keep its scope open until the children finish using it. This is sharing within one execution, not retaining an instance across restarts.

### The exposed surface is the interface

Workflow control flow determines which states and actions exist for external consumers at each phase:

- Scope resources to the identities that should see or invoke them; `None` denotes public resources. Authentication and entitlement to present identities belong to the hosting application. Follow the agreed public-resource identity-selection convention; that calling convention remains an open question in the test plan.
- Knowing an action name does not make an unavailable or internal-only action externally invocable. Actions are not externally available during reconstruction.
- An advertised action can disappear before invocation; clients must handle that race. Arbitrary action bodies are not atomic across awaits.
- Resource streams provide current snapshots, not event logs. Updates can coalesce, and historical scenes are not published during replay. Do not require consumers to observe every intermediate phase.
- Streams close on normal workflow completion and propagate terminal workflow failure. The hosting transport must communicate those outcomes to its consumers.

An `IdentityQueue` correlates a submission with its returned identity: the workflow consumes that same identity with the item and can use it to scope subsequent resources. An alias instead provides temporary alternate workflow routing; it does not grant resource identity or authorization.

External action requests are recorded for recovery. A successful invocation return means the request survives immediate process interruption, but there is no individual invocation-outcome lookup for a caller whose response was lost. Publish application confirmation state when needed; request recovery does not eliminate the best-effort external-effect boundary.

## 4. Replay constrains task starts as well as operations

Concurrency permits different scheduling, but replay must follow recorded history faithfully. This includes **task-start records**, not just resource operations. Calling two decorated task functions in sequence does not establish that both task bodies have started before either performs work.

Distinguish these checkpoints:

1. **Task handle created:** the caller has requested managed execution.
2. **Task body entered:** its recorded start has replayed and its ordinary code can run.
3. **Operation attempted:** code has reached the call, but replay may still gate that operation.
4. **Operation completed:** the awaited call has returned.
5. **Task completed:** its awaiter has received the outcome.

A host event placed at one checkpoint does not prove a later checkpoint has occurred. Even a signal immediately before an operation cannot fire until the task is permitted to enter its body.

### Why reversing producer releases can deadlock

Suppose the live history is:

```text
producer one starts
producer one puts "one"
producer one completes
producer two starts
producer two puts "two"
```

On replay, producer two cannot enter its body before producer one's earlier put has replayed. This host sequence therefore deadlocks:

```text
release producer two
wait for producer two's pre-put "attempted" signal
release producer one
```

Producer two's start waits for producer one's put; producer one waits for its host release; the host waits for producer two's body signal before providing that release. The deadlock occurs before producer two attempts its put, even though the host never waits for that put to complete. This was the TK04 timeout after the `get_id` normalization fix; it was not evidence of a queue-action completion-gating defect.

### Establish readiness before recording the order you want to vary

For a changed-scheduling replay test:

1. Give each producer a distinct body-readiness signal, emitted **before** its host-controlled release wait.
2. In the original execution, wait for **both** readiness signals before releasing either producer. This establishes that both start records precede the puts.
3. Release producer one and wait for its put to complete; then release producer two. Suspend at the intended checkpoint.
4. Reconstruct the same workflow/task/resource structure with fresh host signals and gates.
5. Release producer two and wait only for its pre-put attempt signal, then release producer one. Producer two's put may wait for producer one's historically earlier put.
6. Assert the recorded observable ordering, not reversed operation completion or incidental sibling scheduling.

The readiness/attempt signals here are test instrumentation. In application code, express dependencies that must survive replay through Quest-managed operations rather than non-durable host events. More generally, never make an earlier recorded event depend on a signal that can only be produced after a later recorded event.

Do not assume task creation order establishes body-start order, or use sleeps to establish readiness. When diagnosing a replay hang, include task starts in the dependency chain before blaming operation completion or the global replay-complete barrier. Do not fix a test-created dependency cycle by weakening valid replay ordering.

## 5. Exceptions follow normal async propagation

- An exception from an awaited task propagates to its awaiter normally.
- If workflow code catches and handles it, execution can continue.
- If it propagates unhandled out of the workflow, the workflow fails.
- Merely creating an unawaited child does not make its exception propagate into the parent as though the parent had awaited it.
- Do not conflate task exception propagation with recorded step exceptions. A step's exceptional outcome is recorded; ordinary task code reconstructs the authored path that reaches an exception.
- An external action failure fails that invocation, not automatically the workflow.

Tests should distinguish handled child failure, unhandled awaited child failure, replay of a recorded exceptional step inside a task, and invocation-only failure. Unobserved observer-Future warnings are a separately tracked implementation issue; catching a workflow exception alone does not establish that every internal exception carrier has been observed.

## 6. Plan for restart and code evolution

- Use `WorkflowManager` when unfinished workflows must be discovered and resumed after process restart. Configure real storage/history and a distinct namespace for each running manager.
- A manager retains completed results or exceptions according to its configured completion policy. Do not equate retained outcomes with a lookup for individual action responses.
- Preserve replay-compatible task creation and recorded control flow when changing code. History mismatches are errors, not permission to attach old results to different calls.
- Use event-time compatibility branches when appropriate: `when()` reports the timestamp of the last Quest event to be replayed until replay catches up, then current time. Compare with `get_current_timestamp()` for release boundaries; exact boundary details remain subject to the design/test-plan contract questions.
- Account explicitly for downtime and deadlines. Durable execution does not mean uninterrupted execution or automatic application-specific expiry behavior.
- If permanent cancellation is needed, author a cooperative signal/handling path; suspension is not permanent cancellation.

## 7. Practical testing lessons

### Recovery setup

- Test through imports from `quest` and actual async execution, not mocks of replay, resources, or persistence. Include a scenario comment in every test.
- Preserve history across attempts; create a fresh `Workflow` each time and reconstruct workflow-owned resources inside it.
- Give every attempt fresh test-host gates/events. Otherwise a previously released event can bypass the intended checkpoint.
- The test host may retain execution witnesses for comparison. These are instrumentation, not durable application state.
- A list-backed history proves in-process replay behavior, not serialization or cross-process durability. Use fresh persistent-history/storage objects for disk recovery and genuinely separate processes for cross-process claims.
- Graceful context exit and abrupt process termination exercise different boundaries. A checkpoint inside a step does not establish recorded completion of that step.

### Coordination and assertions

- Establish checkpoints through observable conditions, not sleeps or assumed scheduler order.
- To prove both children started, wait for a distinct readiness signal from each; a shared event proves only that at least one started. Establish readiness before releasing work whose records must follow those starts.
- To prove a child completed, await its handle or signal after that await; reaching a point inside its body is not the same guarantee.
- Check results where awaiters receive them, not only where task bodies compute them.
- Separate ordinary body-execution witnesses from recorded-step effect witnesses. Their expected replay counts differ.
- Use bounded waits and explain execution, interruption, replay, and assertions with play-by-play comments.
- Do not add a suspension-settling/`CancelledError` assertion to every replay test as a prerequisite for reusing history. Current step tests reuse history directly after context exit; one dedicated test checks cancellation of the public workflow task handle. This does not establish every possible shutdown/cleanup contract.
- Keep framework failures visible rather than weakening replay assertions, importing private machinery, or mocking the behavior under test. Equally, verify that host coordination permits the recorded path before diagnosing a timeout as a framework defect.
