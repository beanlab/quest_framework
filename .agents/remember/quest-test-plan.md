# Quest test plan

## Scope and principles

This is a scenario inventory for a new suite in `quest_test/`, organized around every export in `src/quest/__init__.py`, followed by cross-feature scenarios. It is not a report of passing behavior or a specification inferred from existing implementation quirks.

Basis: `.agents/remember/quest-design.md`, the current public exports, current `src/quest/` code to identify available operations, and consumer examples in `src/demos/`. Neither `quest_test/v3/` nor `src/v3/` was read. No test files have been created as part of this plan.

- Exercise Quest through imports from `quest` only. Do not import internal historians, ledgers, serializers, resource managers, or replay machinery.
- Use real async execution, real persistence, and real replay. Do not mock the machinery whose behavior is under test.
- Each test must include a comment explaining its scenario. Tests should also demonstrate understandable public API usage.
- Assert observable outcomes: results, exceptions, snapshots, available actions, routing, persisted recovery, and actual side-effect counts. Avoid assertions about private fields, internal record encodings, hash names, or incidental scheduling.
- Use `asyncio.Event` or queues as test-host coordination barriers, not arbitrary sleeps. Put bounded timeouts around waits so regressions fail rather than hang. Workflow operations that need durable ordering must use Quest-managed primitives.
- Test deterministic reconstruction by rebuilding ordinary local variables, not by expecting Python instruction-pointer restoration.
- For replay scenarios, use fresh `Workflow`, `PersistentHistory`, and storage objects. Include genuinely separate-process tests: same-process reconstruction alone does not prove durability.
- Use temporary directories and independent namespaces. A real append-only file can serve as an external side-effect witness across processes; it is test instrumentation, not a replacement for Quest persistence.
- Cover normal and caught-exception cleanup, suspension, and restart where relevant. Do not assume suspension is permanent cancellation.
- Requirements from the design document are intended assertions even if the implementation currently fails them. Unsettled behavior is collected under **Contract questions**, not silently adopted from the implementation.

Scenario IDs below are stable references; one scenario may require several parameterized cases. Suggested files are organizational guidance, not a requirement to create one file per export.

## 1. `step` — `quest_test/test_step.py`

- **ST01 — Ordinary calls:** An async step receives positional/keyword arguments and returns its result; cover no arguments, defaults, `None`, and representative nested JSON-compatible values.
- **ST02 — Completed work:** Suspend after a completed step, restart, and verify its recorded result is supplied without repeating its external effect.
- **ST03 — Incomplete work:** Interrupt inside a step before completion; restart and verify its body begins again rather than resuming at the interrupted await.
- **ST04 — Nested completion boundaries:** An incomplete outer step has completed and incomplete substeps. Replay skips completed substeps, reruns incomplete work, and eventually records the outer result.
- **ST05 — Completed outer step:** Once the outer step completes, replay does not execute either its body or its nested steps again.
- **ST06 — Recorded exceptions:** A step raises a reconstructable exception; workflow code catches it and continues. Restart reproduces the type/arguments and handling path without repeating the failed step's body. Include a nested caught exception.
- **ST07 — Repeated calls:** Repeated calls to the same decorated function in a loop retain distinct results and replay in the original sequence.
- **ST08 — Reconstruction:** Ordinary deterministic code between steps rebuilds a conversation/history accumulator correctly over multiple restarts.
- **ST09 — Compatibility violations:** Change a recorded step sequence by inserting, removing, or reordering distinguishable steps; replay rejects incompatible history rather than silently attaching results to the wrong calls. Do not require a non-exported exception type.
- **ST10 — Best-effort boundary:** An effect occurs inside a step but the process stops before completion is recorded. On recovery the effect may repeat; an idempotent version reaches the intended single logical outcome. Do not assert exactly-once effects.

## 2. `task` — `quest_test/test_task.py`

- **TK01 — Basic execution:** Start and await a managed task; positional/keyword arguments and returned values reach their callers.
- **TK02 — Actual concurrency:** Two managed tasks reach independent waiting points before either is released; both complete and return the correct results.
- **TK03 — Mixed progress:** Restart with one child complete and another incomplete. Completed recorded work is not repeated, and both tasks reconstruct correctly.
- **TK04 — Ordering:** Concurrent tasks operate on shared Quest queues/state with deliberately controlled live interleavings. Replay preserves recorded ordering even when test-host scheduling differs.
- **TK05 — Task trees:** A managed task starts and awaits other managed tasks, including repeated calls to the same task function. Results and task identity remain correctly associated during replay.
- **TK06 — Exceptions:** A task raises a reconstructable exception; its awaiting parent catches it or leaves it unhandled. Test both recovery of the caught path and terminal workflow failure of the unhandled path.
- **TK07 — Repeated suspension:** Suspend and resume while several managed tasks wait on different queues; no lost progress, duplicated recorded effects, or cross-delivered results.
- **TK08 — Managed lifetimes:** Workflow code explicitly signals children to finish and awaits them before returning; all resources are cleaned up. Do not treat unawaited children as automatically durable independent workflows.

## 3. `State` — `quest_test/test_state.py`

- **SA01 — Scope:** Enter a state context, observe the initial value, use `get`/`set`, and observe removal on exit.
- **SA02 — Values:** Set successive scalar, empty, and nested JSON-compatible values; reads and snapshots agree on the current value.
- **SA03 — Visibility:** Public, actor-specific, and role-specific state is visible only in the appropriate views; `external=False` state remains usable internally but is absent from external views.
- **SA04 — Independence:** Equal names under different identities and different names under one identity do not interfere.
- **SA05 — Replay:** Rebuild state through recorded reads/writes across suspension; publish the reconstructed current state, not historical intermediate values.
- **SA06 — Lifetime cleanup:** Normal exit and a caught exception remove state; a later phase can reuse the same identity/name without stale state leaking.
- **SA07 — Notifications:** A waiting relevant observer receives a new current snapshot after a change. A slow observer is permitted to skip intermediate values.

## 4. `Action` — `quest_test/test_action.py`

- **AC01 — Scoped callable:** Register a standalone callable with `Action`, invoke it externally with positional/keyword arguments, and receive its result.
- **AC02 — Temporal availability:** Invocation before entry and after exit fails, even when the caller knows the action name; invocation while exposed succeeds.
- **AC03 — Identity scope:** Invoke the correct identity/name combination; other identities cannot access the action. A shared role permits callers presenting that role to use it.
- **AC04 — Hidden action:** `external=False` actions are not advertised and cannot be externally invoked merely by knowing their names. Internal workflow use remains possible through the supported authoring API.
- **AC05 — Invocation failure:** The callable raises; only that invocation fails. The workflow remains usable and can accept a later valid invocation.
- **AC06 — Request recovery:** An externally submitted request interrupted after durable acceptance is completed through replay. Include a lost-response case and verify recovery via workflow-published confirmation, not a nonexistent invocation-result lookup API.
- **AC07 — Withdrawal race:** Two callers observe an action; the first invocation causes the workflow to withdraw it before the second invocation. The second fails gracefully, without reviving the resource or failing the workflow.
- **AC08 — Awaiting action bodies:** Use an action that yields while executing alongside another invocation. Assert application-defined synchronization and valid outcomes, not blanket atomicity across awaits.
- **AC09 — Replay gate:** A known external action cannot be invoked before reconstruction has finished, even if its registration has already been replayed internally.
- **AC10 — Cleanup:** Normal exit and a caught exception withdraw the action; reusing the name in a later phase targets the new callable.

## 5. `action` and `Resource` — `quest_test/test_resource.py`

These are separate exports exercised together because `action` decorates resource methods.

- **RE01 — Resource registration:** A subclass with multiple decorated methods and `State` members exposes exactly the intended surface during its context and removes it on exit.
- **RE02 — Decorator forms:** Cover `@action`, `@action()`, explicit `name=...`, and `external=False`; default and custom names are scoped by the resource name.
- **RE03 — Internal routing:** Call a decorated method from workflow code; its effects are ordered and replayed consistently with external calls, including methods invoking other resource methods.
- **RE04 — Hidden method:** An internal-only decorated method is callable by workflow code but neither advertised nor externally invocable.
- **RE05 — Naming isolation:** Two resource instances with different names under the same identity, and the same name under different identities, do not collide.
- **RE06 — Inheritance:** A subclass uses inherited decorated methods and overrides a decorated method; the visible operations behave as authored, without duplicate registration.
- **RE07 — State plus action:** An action updates a member `State`; observers receive the updated surface and see both state and actions disappear on scope exit.
- **RE08 — Reusable phases:** Exit and re-enter a resource through a workflow loop; actions are registered once per active scope and call the correct implementation after replay.
- **RE09 — Exception cleanup:** A caught exception in a resource scope removes its states and actions; subsequent phases remain usable.

## 6. `Queue` — `quest_test/test_queue.py`

- **QU01 — FIFO:** Internal puts and gets preserve item order, including repeated equal items and nested JSON-compatible payloads.
- **QU02 — External input:** The surface exposes `name.put`, not `name.get`; external put delivers its payload to the waiting workflow.
- **QU03 — Blocking:** An empty get waits without completing; a later put releases it with the correct item.
- **QU04 — Replay backlog:** Enqueue multiple items, consume only some, suspend, and resume; consumed items are not redelivered and remaining items retain their order.
- **QU05 — Concurrent users:** Multiple managed producers/consumers neither lose nor duplicate items; verify the controlled, recorded ordering on replay.
- **QU06 — Scope and lifetime:** Identity filtering and scope exit govern put availability; a new queue in a later phase does not inherit stale items from an old resource instance.

## 7. `IdentityQueue` — `quest_test/test_identity_queue.py`

- **IQ01 — Correlation:** `put(item)` returns an identity and the workflow's `get()` yields that same identity with the same item.
- **IQ02 — Uniqueness:** Multiple submissions, including equal payloads, produce distinct identities.
- **IQ03 — Targeted continuation:** Publish subsequent state/actions under the returned identity. That submitter can access them; other submitter identities cannot.
- **IQ04 — Replay stability:** Suspend after submission and before consumption or later use. The originally returned identity survives restart; replay must not generate a replacement.
- **IQ05 — Queue semantics:** Repeat blocking, FIFO backlog, concurrent submissions, scope cleanup, and hidden-get scenarios from `Queue`, retaining correct identity/item pairing.

## 8. `IDENTITY` — identity cases across surface tests

`IDENTITY` is a public type alias, not an independent runtime service. Test its meaning through actual resources rather than testing Python's type-alias representation.

- **ID01 — Public:** `None` resources are available as public resources to every observer under the intended resource-stream identity-selection convention.
- **ID02 — Private:** String identities scope resources; knowing another identity's resource name does not expand the presented scope.
- **ID03 — Roles and unions:** A view presenting multiple identities sees the appropriate combined surface; multiple callers presenting one role share its resources.
- **ID04 — Namespaces:** A routing alias, workflow ID, and resource identity with identical text still perform distinct roles and do not grant additional visibility.

Authentication and entitlement to present an identity belong to the hosting application, not Quest tests.

## 9. `these` — `quest_test/test_these.py`

- **TH01 — Collections:** Enter lists, dictionaries, and generators of real Quest resources. All expected resources are active inside the combined context and absent afterward.
- **TH02 — Returned values:** Dictionary keys and list/generator ordering associate returned entered resources with their original positions.
- **TH03 — Empty and mixed collections:** Empty collections work; non-context values alongside resources remain available unchanged where supported.
- **TH04 — Dynamic addition:** Add a resource to an entered list context; it becomes active immediately and is removed with the enclosing scope.
- **TH05 — Nested usage:** Nest grouped contexts and combine `State`, `Action`, `Queue`, and custom `Resource` instances; cleanup leaves no stale surface.
- **TH06 — Exceptions:** A caught exception in the grouped body removes all entered resources. Entry-failure cleanup and exact exit ordering require the contract decision below.
- **TH07 — Replay:** Reconstruct a deterministic generated collection, including dynamic additions, without duplicated registrations or premature publication.

## 10. `when` and `get_current_timestamp` — `quest_test/test_time.py`

- **TI01 — Clock interoperability:** `get_current_timestamp()` values can be compared with `when()` values and used as release boundaries without timezone/type errors. Basic successive clock reads are comparable; do not assume strict monotonicity of a wall clock.
- **TI02 — Fresh execution:** With no history remaining, `when()` reports current time, bounded by surrounding real clock reads rather than an exact expected timestamp.
- **TI03 — Replay time:** While events remain, `when()` follows the design's recorded-event time definition, not the restart's wall-clock time.
- **TI04 — Catch-up transition:** Once the final historical event is replayed, `when()` switches to current time. Exercise boundaries involving steps, managed tasks, and resource creation/actions.
- **TI05 — Compatible code release:** Histories paused at different points run compatible old branches while replaying and new branches for new execution after a release timestamp. Include a fresh workflow started after the release.
- **TI06 — Multiple releases:** Resume through two compatible code changes and multiple restarts; each historical branch remains faithful.
- **TI07 — Incompatible change:** A time branch selecting a history-incompatible path produces a replay mismatch rather than silently accepting it.

## 11. `Workflow` — `quest_test/test_workflow.py`

- **WF01 — Lifecycle:** Enter an async workflow context, await completion, and obtain its result; cover positional/keyword arguments and `None` results.
- **WF02 — Task handle:** Await the public `task` handle for success and failure; its outcome agrees with awaiting the workflow. Do not assume stable object identity between property accesses.
- **WF03 — Suspension:** Exit while the workflow waits, then resume from the same persistent history using a fresh workflow object. Completion boundaries and deterministic local reconstruction are preserved.
- **WF04 — Multiple recoveries:** Suspend at several distinct boundaries and resume repeatedly without losing progress or repeating completed effects.
- **WF05 — Failure:** An unhandled workflow exception reaches awaiters and active resource observers and is terminal; a caught exception permits continued interaction.
- **WF06 — Stream initial view:** An observer entering an active workflow's resource stream receives its current visible surface, including the empty view when appropriate.
- **WF07 — Snapshots, not logs:** A slow consumer sees the latest view after several changes; no requirement to observe every intermediate scene.
- **WF08 — Independent observers:** Two observers with overlapping/different identities receive their appropriate current views. Closing one stream early does not break the workflow or the other stream.
- **WF09 — Stream termination:** Active streams close on normal completion and propagate terminal workflow failure. Resource cleanup does not leave observers blocked forever.
- **WF10 — Replay publication barrier:** Start observing immediately on restart. No historical scenes are published; the first delivered snapshot represents replay-complete execution.
- **WF11 — Invocation integration:** Exercise successful, failed, withdrawn, and replay-recovered external invocations through `Workflow.invoke_action`, not resource internals.
- **WF12 — Independent workflows:** Concurrent workflow instances do not leak resources, events, task context, or step results into one another.

## 12. `WorkflowManager` — `quest_test/test_manager.py`

- **MA01 — Start and route:** Start different workflow types with distinct IDs and arguments; route streams and action invocations to the intended workflow.
- **MA02 — Active duplicates:** Reject a duplicate active workflow ID without disturbing the original workflow.
- **MA03 — Manager restart:** Exit with unfinished workflows, reconstruct the manager with fresh factories/history/storage objects, and verify all unfinished workflows resume with their original type and arguments.
- **MA04 — Isolation:** Several workflows at different progress points resume independently; distinct manager/history namespaces sharing a storage root do not contaminate one another.
- **MA05 — Outcomes:** While running, result retrieval waits and returns the eventual value or raises the workflow exception.
- **MA06 — Retention:** With `delete_on_finish=False`, retrieve successful results and reconstructable exceptions repeatedly, including after a fresh-process restart.
- **MA07 — Deletion policy:** With `delete_on_finish=True`, completed workflows are removed. With retained outcomes, result retrieval with `delete=True` removes the stored outcome, including across restart.
- **MA08 — Explicit deletion:** Delete an active workflow or retained outcome and verify it does not reappear after restart. Timing and interaction with retention need the contract decision below; this is distinct from a universal author-facing cancellation primitive.
- **MA09 — Presence and metrics:** Public presence/metrics accurately distinguish the supported active/retained/deleted states and identify workflow type/ID. Do not hard-code undocumented timestamp formatting.
- **MA10 — Missing targets:** Missing workflow routing, result lookup, and deletion fail without affecting other workflows; avoid importing non-exported exception classes.
- **MA11 — Mixed outcomes:** One workflow fails while others wait or finish; unrelated workflows remain usable and recoverable.
- **MA12 — Exit races:** A workflow completes or an accepted action progresses during manager shutdown; restart does not lose both the unfinished execution and its retained terminal outcome.
- **MA13 — Abrupt restart:** Terminate a subprocess without normal context exit after externally witnessed progress; a new process discovers and resumes unfinished workflows. Test retained outcomes separately after an externally witnessed completion.

## 13. `alias` — `quest_test/test_alias.py`

- **AL01 — Temporary routing:** Inside `async with alias(...)`, streams and actions can use the alternate ID and reach the same workflow; canonical routing remains valid.
- **AL02 — Lifetime:** On normal scope exit or a caught exception, alias routing ceases without deleting the workflow. A later workflow can use the released alias.
- **AL03 — Conflict:** Two active workflows attempting the same alias cannot silently hijack one another's routing.
- **AL04 — Multiple aliases:** One workflow has several scoped aliases; withdrawing one does not remove the others or canonical access.
- **AL05 — Restart:** Replaying a workflow that is inside an alias scope restores routing when it reaches its live phase; aliases from already-ended scopes are not resurrected.
- **AL06 — Identity separation:** Routing through an alias does not bypass state/action identity scope.

## 14. `PersistentHistory` — `quest_test/test_persistence.py`

- **PH01 — Real recovery:** A workflow backed by this history records progress, a fresh history instance loads it, and replay reconstructs the correct continuation.
- **PH02 — Accumulated history:** Many repeated steps, nested steps, exceptions, tasks, and resource interactions survive multiple reconstruction cycles in the correct sequence.
- **PH03 — Isolation:** Different history namespaces in the same real storage remain independent.
- **PH04 — Collection operations:** Through public `append`, iteration, reverse iteration, `remove`, and `clear`, valid history entries preserve logical order and removals across reconstruction. Resolve whether caller-authored ledger entries are a supported public contract before implementing direct entry-construction tests.
- **PH05 — Workflow cleanup:** Finishing or suspending a workflow leaves enough persisted information for the promised subsequent outcome/recovery; do not require a private file layout or undocumented pruning strategy.
- **PH06 — Serialization boundaries:** Representative supported JSON data and reconstructable exceptions survive real disk serialization, not just in-memory replay.

## 15. `LocalFileSystemBlobStorage` — `quest_test/test_storage.py`

- **BS01 — Directory creation:** Construct storage at a missing nested temporary root and use it successfully.
- **BS02 — Round trips:** Write/read supported JSON values, including Unicode, nested/empty structures, booleans, numbers, and null where supported; preserve JSON value semantics.
- **BS03 — Existence and overwrite:** `has_blob` distinguishes missing/present keys; overwriting a key changes its value without changing another key.
- **BS04 — Deletion:** Delete an existing blob, verify absence, and recreate it successfully.
- **BS05 — Reopening:** Fresh instances and a fresh process read the same stored values.
- **BS06 — Value independence:** Mutating the original input or a returned decoded object does not silently change a stored blob.
- **BS07 — Real integration:** Use this storage for every required disk-backed history/manager restart scenario rather than an in-memory substitute.

## 16. Cross-feature scenarios — `quest_test/test_integration.py` and `quest_test/test_process_recovery.py`

These complement the feature tests; do not duplicate every feature across every axis.

- **CX01 — Durable conversation:** A loop consumes queue input, rebuilds local conversation history, and calls a recorded model-like step with a real side-effect witness. Restart across turns without re-requesting consumed input or repeating completed calls. No real model service is necessary.
- **CX02 — Multi-user phased interaction:** Public `IdentityQueue` enrollment leads to private player state/actions, concurrent managed tasks, a shared-role confirmation phase, then completion. Restart during each phase and verify only the current interface is available.
- **CX03 — External-system routing:** A recorded step obtains an external identifier; `alias` registers it; manager routing delivers queue input under that identifier while identity filtering still applies. Restart and later withdraw the alias.
- **CX04 — Pending invocation:** Manager + persistent history + a resource action containing nested steps. Stop while the action is incomplete; replay completes the accepted request without repeating its completed nested effects. Publish confirmation for a caller whose response was lost.
- **CX05 — Failure separation:** An external action error leaves the workflow alive; a caught step/task exception drives a recovery phase; a later unhandled workflow exception terminates observers and remains retrievable as a retained manager outcome after restart.
- **CX06 — Replay and interface gate:** Restore dynamically grouped resources used by several tasks while observers and an informed caller are already waiting. No historical snapshots or early action invocation escape the replay barrier.
- **CX07 — Time-compatible deployment:** Manager resumes multiple persistent workflows from different phases using time-branched updated code; old history is honored and subsequent phases use the new interface.
- **CX08 — Namespace and failure isolation:** Multiple workflows/managers use one filesystem root with independent namespaces; completion, error, deletion, alias withdrawal, and restart in one do not corrupt the others.
- **CX09 — Idempotent external effects:** A real subprocess is stopped after an effect but before its step completes. An application-provided idempotency key makes retry safe; contrast this with the allowed duplicate-attempt behavior of best-effort execution.
- **CX10 — Downtime-aware application:** Workflow code records an application deadline in a step. Restart after the deadline, and verify the author's explicit expiry branch, not imaginary uninterrupted execution or an automatic Quest deadline policy.
- **CX11 — Cooperative permanent stop:** An author-defined action/queue signal asks managed tasks to finish; the parent awaits them and returns. Restart does not revive the completed interaction when the manager's configured completion policy applies.

### Bounded coverage matrix

Use focused parameterization and selected multi-way scenarios, not an exhaustive Cartesian product.

| Dimension | Representative cases | Main coverage |
| --- | --- | --- |
| Completion boundary | completed step; incomplete step; nested partial step; caught exception | ST02–ST06, CX04 |
| Execution shape | sequential; loop; sibling tasks; nested tasks | ST07–ST08, TK02–TK05 |
| Interruption | clean context exit; repeated suspension; abrupt process stop | WF03–WF04, MA13, CX09 |
| Surface lifetime | before exposure; active; withdrawn; replaying; finished/failed | AC02, AC07, AC09, WF09–WF10 |
| Scope | public; one actor; multiple actors; shared role; internal-only | SA03–SA04, AC03–AC04, CX02 |
| Observer timing | already listening; late joiner; slow consumer; multiple observers | SA07, WF06–WF10 |
| Invocation outcome | success; invocation-only error; response lost; action withdrawn | AC05–AC08, CX04–CX05 |
| Persistence state | queued backlog; partial tasks; retained result; retained exception; deleted | QU04, TK03, MA06–MA08 |
| Routing | canonical ID; active alias; withdrawn alias; isolated namespace | AL01–AL06, CX08 |
| Deployment | no change; compatible event-time branch; incompatible history | ST09, TI05–TI07, CX07 |

For each core recovery category, include at least one success and one exception path, one resource-bearing scenario, and genuine cross-process coverage where continuity is the claim. Not every feature needs its own subprocess test.

## 17. Reusable test resources and recipes

Keep **resources** (fixtures), **procedures** (small helpers), and **scenarios** (the tests themselves) distinct. Reuse synchronization and lifecycle mechanics without hiding the workflow logic or assertions behind a universal test runner. The following are planned test-support tools, not additional Quest APIs.

### Targeted interruption with checkpoint gates

A test-owned gate has a `reached` event and a `release` event:

```python
class Gate:
    def __init__(self):
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def wait(self):
        self.reached.set()
        await self.release.wait()
```

Workflow or step code calls `await gate.wait()` at the chosen boundary. The test waits for `gate.reached`, then either releases execution or initiates interruption. All test-side waits need bounded timeouts.

| Gate placement | Recovery behavior exercised |
| --- | --- |
| After awaiting a step | Completed step supplies its recorded outcome without executing again |
| Inside a step | Incomplete step begins again |
| Between nested substeps | Completed children replay while the incomplete enclosing step restarts |
| Inside an action | Accepted but unfinished invocation recovers |
| Inside a managed task | Partial concurrent progress reconstructs |

Rules for gates:

- Create fresh gates for each execution attempt. Keep the recorded workflow path compatible across attempts; do not branch around recorded operations based on a test's run/replay flag.
- Gates are non-durable test-host coordination, not application state or replacements for Quest-managed queues and ordering. Prefer a real Quest queue when waiting for external input is the behavior being tested.
- A gate inside a completed step is not reached during replay because that step's body is skipped. Choose the next checkpoint accordingly.
- Reaching a gate only establishes progress to its location. In particular, a gate inside a step does not establish recorded completion of that step.
- An application-code gate cannot target an internal filesystem-write boundary. Such fault-injection tests require a separately agreed technique and durability contract.

### Two interruption procedures

**Graceful suspension:** Run until the target gate, leave the public `Workflow` context, and wait for the original execution to settle before reopening the history. Capture the public task handle while the context is active. The current context exit requests suspension without itself awaiting workflow termination, so the helper must avoid overlapping old and recovered instances. Treat this as a current implementation consideration, not a permanent public shutdown contract. Account explicitly for the expected suspension outcome and propagate unexpected failures; do not blanket-suppress exceptions or cancel the task merely to implement a timeout.

**Abrupt process interruption:** A child process signals its checkpoint through IPC. The parent terminates it without normal context cleanup, waits for process exit, and starts a fresh child using the same storage directory. Use this for cross-process continuity and unclean-exit claims, not for every replay test. It is distinct from graceful suspension and must not be described as a completed durable checkpoint unless the checkpoint actually follows the relevant recorded operation.

### Candidate pytest fixtures

Use function scope by default so tests do not share mutable history, gates, or running workflows.

| Fixture | Responsibility |
| --- | --- |
| `tmp_path` | Use pytest's existing temporary-directory fixture; no custom replacement needed |
| `history` | A fresh plain list reused across successive workflow instances within one test |
| `storage` | Real `LocalFileSystemBlobStorage` rooted under `tmp_path` |
| `history_factory` | Open fresh `PersistentHistory` instances for explicit namespaces; reopening a namespace preserves its persisted history |
| `gate_factory` | Create independent gates for named phases or tasks, with fresh gates per execution attempt |
| `effect_log` | Record real body-execution attempts; use a list in-process and a file-backed witness across processes |
| `workflow_factory` | Construct fresh workflows with explicit functions, IDs, arguments, and histories; let the test show context entry/exit |
| `manager_factory` | Construct managers with explicit workflow registrations, namespaces, retention policy, and serializer setup |
| `process_runner` | Own subprocesses, checkpoint IPC, output capture, bounded waits, and cleanup |

Do not begin with an automatically entered, shared `Workflow` fixture: recovery scenarios need distinct instances, and their lifetimes are part of the demonstration. A list-backed history proves replay behavior, not JSON serialization or filesystem durability. Keep the list and persistent-storage options explicit rather than automatically running every scenario against every backend.

The manager fixture must use the agreed public/application-owned serializer setup; it must not bypass that open question by importing an internal serializer. Test-owned logs, gates, factories, and subprocess communication are instrumentation, not mocks of Quest's execution machinery.

### Small shared helpers

- **Observe until a predicate:** Consume the real resource stream until a snapshot matches an expected phase, state, or action. Allow intermediate snapshots to coalesce. On timeout, report the predicate and last observed snapshot. Enter and close the stream through its public context protocol.
- **Suspend and settle:** Encapsulate the graceful-suspension procedure, wait for the captured task handle to terminate, and leave history available for reconstruction. Keep fresh workflow construction and scenario assertions visible in the test.
- **Effect witness:** Record actual executions separately from returned values so a test distinguishes ordinary reconstruction, completed-step replay, and incomplete-step retry. A cross-process witness should make each acknowledged write visible before signaling its checkpoint; do not claim filesystem power-loss durability from ordinary process-restart tests.
- **Bounded waits and cleanup:** Centralize diagnostic timeouts and cleanup for observer tasks, external invocation tasks, and subprocesses. On failed tests, terminate remaining work before temporary storage is discarded. Prefer explicit checkpoints and predicates over sleeps.

### Recipe catalog

Each recipe should show a minimal public-API example, synchronization points, expected observations, and limits on what it proves.

| Recipe | Procedure and assertions | Related scenarios |
| --- | --- | --- |
| Completed-step replay | Gate after the step, suspend, reopen history, and verify the effect count stays unchanged while the result is restored | ST02, ST05, WF03 |
| Incomplete-step retry | Gate inside the step, interrupt, restart with a fresh gate, and verify a new attempt completes | ST03, ST10 |
| Partial nested-step recovery | Gate after one child completes but before the outer step completes; verify only unfinished bodies rerun | ST04, CX04 |
| Controlled concurrent recovery | Coordinate sibling tasks at separate gates, record an ordering through real Quest operations, and verify reconstruction under changed host scheduling | TK03–TK05, TK07 |
| Surface interaction | Wait for a matching snapshot, invoke the advertised action, and observe resulting state or withdrawal without requiring every intermediate snapshot | AC01–AC03, AC07, WF06–WF10 |
| Failure separation | Compare an invocation-only error, a caught step/task exception, and an unhandled workflow failure through public results and streams | AC05, TK06, WF05, CX05 |
| Persistent manager restart | Run with real storage, suspend/finish selected workflows, reconstruct manager and histories, and verify unfinished progress and retained outcomes | MA03–MA07 |
| Abrupt-process recovery | Synchronize the child at a known application boundary, terminate without cleanup, start a new process, and inspect progress/effect witnesses | MA13, CX04, CX09 |
| Compatible workflow replacement | Run old code to selected event boundaries, record a real release timestamp, and resume compatible time-branched code | TI05–TI06, CX07 |

Start with gates, list history, real filesystem storage, an effect witness, and snapshot-predicate waiting. Add manager and subprocess helpers as those tests arrive. Extract repeated mechanics after their shape is clear rather than building a general-purpose “run, interrupt, replay, assert” framework.

## 18. Contract questions before turning affected scenarios into assertions

These do not block the rest of the inventory. Discuss one at a time with the author when implementing the affected tests.

1. **Public serializer setup:** `WorkflowManager` requires a serializer, but no serializer implementation is exported from `quest`. Should tests provide a small application-owned JSON pass-through serializer, or should a supported implementation become public? Do not solve this by importing a private serializer.
2. **Public identity convention:** Does `resource_stream([actor])` automatically include public (`None`) resources, or must callers supply `[None, actor]`? What should empty and duplicate identity selections do? Public visibility must be tested with the intended convention.
3. **Group context guarantees:** Does `these` promise contextlib-style cleanup after partial entry failure, reverse exit order, synchronous context support, exception suppression, or contexts whose enter result differs from the context object? Test only the agreed guarantees; ordinary resource cleanup remains required.
4. **Collisions and reuse:** What happens when simultaneously active state/actions share an identity/name, an alias equals a canonical workflow ID, or a new workflow uses an ID with a retained outcome? Rejection, replacement, and shadowing must not be inferred from dictionaries.
5. **Persistence failure boundaries:** What durability is promised if interruption occurs during a filesystem write or between history/manager index writes? Distinguish established replay guarantees from filesystem atomicity/fsync, corruption recovery, and unsupported concurrent writers. Abrupt-stop tests should first synchronize at completed durable operations.
6. **JSON and error boundaries:** What exact JSON value/type fidelity is promised (tuples, non-string dictionary keys, null, non-finite numbers), and how are unsupported values or unreconstructable exceptions reported? Include dict results resembling serialized exceptions to ensure ordinary results are not misclassified once the encoding contract is settled.
7. **Raw history API:** Are user-created history entries and their field schema part of the supported public API, or should `PersistentHistory` primarily be tested through `Workflow`? What are missing-entry removal and repeated-clear semantics?
8. **Deletion and shutdown:** When does manager deletion become durable, does it override outcome retention for a running workflow, and what does returning from manager exit guarantee? Which manager operations resolve aliases? Do not conflate manager deletion with a general permanent-cancellation facility.
9. **Terminal streams:** Active streams must close/fail as designed. What should opening a new stream for an already completed/retained workflow do, and what is an observer meant to see when a workflow is suspended rather than completed?
10. **Timestamp precision:** The design defines replay event-time behavior. Confirm boundary inclusivity and which event advances `when()` before writing exact timestamp assertions; require comparison compatibility, not undocumented strict clock monotonicity.
11. **Storage errors and keys:** Are missing reads/deletes errors or idempotent operations, which keys are valid, and what is the expected response to corrupt JSON or I/O failures? Path confinement/security cases depend on whether keys are trusted or externally supplied.
12. **Use outside workflow context:** Which authoring operations should reject calls without an active workflow/manager context, and is a public error contract intended? Avoid freezing incidental `ContextVar` errors.

## Suggested implementation order

1. Basic workflow lifecycle, steps, real filesystem storage/history, and a clean restart; establish checkpoint gates, effect witnesses, and suspend-and-settle mechanics.
2. State/action surface, identities, streams, queues, and resource lifecycle; establish snapshot-predicate waiting.
3. Managed concurrency, nested replay, and exceptional outcomes.
4. Manager retention, aliases, grouped/dynamic contexts, and time-compatible changes.
5. Cross-process durability, interrupted invocations, shutdown races, and selected end-to-end combinations.

This order establishes trustworthy public-API test helpers before adding the most timing-sensitive scenarios. Contract questions should be resolved as their affected scenarios are reached; implementation gaps should remain visible rather than becoming the expected behavior.
