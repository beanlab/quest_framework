# Quest test plan

## Current status and session handoff

### Reading order for a new session

1. [quest-design.md](quest-design.md): purpose, intended contracts, and author/application responsibilities. This is design guidance, not verification of implementation completeness.
2. This document: scenario inventory, current progress, known blockers, recipes, and unresolved contract questions.
3. [working-with-quest.md](working-with-quest.md): practical workflow authoring, reconstruction, resource lifetimes, replay ordering (including task starts), exception handling, and test-host coordination guidance learned during implementation/review.

For test work, also read the implemented `quest_test/test_step.py` and `quest_test/test_task.py`, plus current `src/quest/__init__.py`. For failure/stream work, read [the unobserved resource-stream exception backlog item](../backlog/resource-stream-unobserved-exceptions.md). Read relevant current implementation as needed, but **do not inspect `quest_test/v3/` or `src/v3/`** without a change in the user's instruction. Scratch probe/review files are not required session context.

### Implemented suites

| Suite | Status | Coverage |
| --- | --- | --- |
| `quest_test/test_step.py` | 8 tests passing; user-requested comment/lifecycle revisions applied | ST01–ST07 (ST05 combined with ST02), ST09, plus one dedicated WF03 task-cancellation check; ST08/ST10 remain assigned to later integration coverage |
| `quest_test/test_task.py` | 8 tests passing; interactive delegate implementation/review and TK04 readiness correction complete | TK01–TK06 and TK08, including TK04's changed-scheduling replay-order assertions; TK07 remains optional |
| Remaining suites | Planned, not implemented in this effort | Sections 3–16, apart from coverage explicitly supplied by the two suites above |

Both suites have scenario comments and play-by-play explanations. Helpers are currently small/local; no general fixture framework has been built. The task suite received revisions for fresh resources/events per attempt, distinguishing operation attempts from completion, explicit sibling readiness/completion, and checking values at awaiters. TK04 now waits for both producer bodies to start before recording either put; its replay retains the original operation-order assertions while reversing host release order.

Latest verification after the user's `get_id` fix and the delegate's TK04 readiness correction:

```bash
poetry run pytest -q quest_test/test_task.py::test_controlled_task_interleaving_replays_queue_operation_order
# 1 passed
poetry run pytest -q quest_test/test_task.py quest_test/test_step.py
# 16 passed
```

The combined run still emitted the known `Future exception was never retrieved` diagnostic for an expected workflow failure. Passing tests do not resolve that observer-Future issue.

Normal pytest invocation works; no logging-suppression wrapper is needed. Run these explicit paths rather than broad discovery of the old suite.

### Known implementation issues

- **TK04 blockers resolved:** The user fixed `get_id` to recursively normalize tuples, resolving replay initialization's unhashable kwargs dictionary. The subsequent timeout was a test coordination error, not evidence of a replay-gating defect: the live history placed producer two's start after producer one's put, but replay's host waited for producer two's attempt signal before releasing producer one. Distinct body-readiness events now establish both task starts before either live put; fresh events are used on replay. TK04 passes without weakening its ordering assertions. Earlier failed runs also emitted pending-task/`coroutine ignored GeneratorExit` teardown diagnostics; their independent cause was not established, and they were not reported in the latest successful combined run.
- **Unobserved resource-stream exception Future:** Expected workflow failures can emit `Future exception was never retrieved` even though the test catches the workflow exception. Cause and desired behavior are recorded in [the backlog item](../backlog/resource-stream-unobserved-exceptions.md); unresolved.
- **Workflow return value:** The first delegate reported that awaiting `Workflow` drops the workflow function's returned value. Current step/task tests assert results as received inside workflow code rather than claim workflow-result coverage. Independently verify/address this when implementing WF01; it is not resolved by passing task tests.
- **Logging fixed:** `TaskFieldFilter` in `src/quest/logs.py` now catches `RuntimeError` from `asyncio.current_task()` when there is no running loop and uses `'-'` for the task field. The previous test-command workaround was removed; there was no suppression code in the test files themselves.

### Decisions and working practices established so far

- Exceptions follow normal async propagation: handled exceptions permit continuation; an exception propagating unhandled out of the workflow fails it. Do not infer automatic parent propagation from an unawaited child.
- Replay tests reuse history directly after leaving the workflow context; repeated suspension-settling checks were removed at the user's request. One dedicated test checks that the public workflow task raises `CancelledError` after context exit. The supporting probe passed 100 repetitions for each completed/incomplete nested-step and wait/no-wait combination (400 total); this is evidence for these cases, not a universal cleanup guarantee.
- Workflow-owned resources reconstruct inside workflow execution. Fresh test-host gates/events belong to each attempt. Retain history and external execution witnesses, not live resource state, across attempts.
- Replay constrains task starts as well as resource operations. A changed-scheduling replay test may attempt a later recorded operation first only if the recorded task-start order permits reaching that attempt. Establish both producer bodies' readiness before either live put, then vary host releases on replay without demanding reversed completion order. Task-handle creation alone does not prove body entry; see [working-with-quest.md](working-with-quest.md) for the TK04 dependency cycle and safe coordination pattern.
- Use interactive delegates (`--interactive true`) for suite implementation, followed by review. Do not fix framework bugs or weaken expected assertions as an incidental part of test implementation. The broader contract questions below are still open; the passing suites do not finalize them.

## Scope and principles

This is a scenario inventory for a new suite in `quest_test/`, organized around every export in `src/quest/__init__.py`, followed by cross-feature scenarios. It is not a report of passing behavior or a specification inferred from existing implementation quirks.

Basis: `.agents/remember/quest-design.md`, the current public exports, current `src/quest/` code to identify available operations, and consumer examples in `src/demos/`. Neither `quest_test/v3/` nor `src/v3/` was read. The plan began as an inventory; implementation progress is tracked above.

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

Review pass: each of the 16 test-domain sections received an independent delegate review, reconciled against the design and practical coverage needs. **Covered by** entries retain IDs for traceability but do not request separate tests; **optional** entries are extensions, and **contract-gated** entries await an author decision. Unmarked scenarios are baseline coverage. Reuse tests across related IDs rather than repeating setup or creating a test for every bullet. Real disk-backed recovery tests must use `LocalFileSystemBlobStorage`; focused list-backed replay tests remain useful.

## 1. `step` — `quest_test/test_step.py`

- **ST01 — Ordinary calls:** An async step receives positional/keyword arguments and returns its result; cover no arguments, defaults, `None`, and a compact representative nested value. Do not conflate live argument passing with serialized result fidelity; JSON edge cases depend on contract question 6.
- **ST02 — Completed work:** Suspend after a completed step, restart, and verify its recorded result is supplied without repeating its external effect.
- **ST03 — Incomplete work:** Interrupt inside a step before completion; restart and verify its body begins again rather than resuming at the interrupted await.
- **ST04 — Nested completion boundaries:** An incomplete outer step has completed and incomplete substeps. Replay skips completed substeps, reruns incomplete work, and eventually records the outer result.
- **ST05 — Completed outer step (extension of ST02):** Use a completed outer step containing nested steps in ST02, and verify replay skips both outer and child bodies. No separate test is needed.
- **ST06 — Recorded exceptions:** A step raises a reconstructable exception; workflow code catches it and continues. Restart reproduces the type/arguments and handling path without repeating the failed step's body. A nested variant should specifically show a child exception caught by its outer step, rather than repeat exception-encoding checks.
- **ST07 — Repeated calls:** Repeated calls to the same decorated function in a loop retain distinct results and replay in the original sequence.
- **ST08 — Reconstruction (covered by CX01):** Ordinary deterministic code between steps rebuilds a conversation/history accumulator. Keep any step-only example small rather than repeating the multi-turn integration test.
- **ST09 — Compatibility violations:** Change a recorded step sequence by inserting, removing, or reordering distinguishable steps; replay rejects incompatible history rather than silently attaching results to the wrong calls. Do not require a non-exported exception type.
- **ST10 — Best-effort boundary (covered by CX09):** Use the abrupt-process recipe to demonstrate effect-before-recording retries and application idempotency. ST03 supplies the focused incomplete-step test; graceful suspension alone is not proof of crash recovery.

## 2. `task` — `quest_test/test_task.py`

- **TK01 — Basic execution:** Start and await a managed task; positional/keyword arguments and returned values reach their callers.
- **TK02 — Actual concurrency:** Two managed tasks reach independent waiting points before either is released; both complete and return the correct results.
- **TK03 — Mixed progress:** Restart with one child complete and another incomplete. Ordinary task bodies reconstruct; completed steps inside them supply recorded outcomes without repeating their effects, incomplete steps retry, and both task results reach the correct callers. Do not assume task completion caches a return value or skips the entire task body.
- **TK04 — Ordering:** Concurrent tasks operate on shared Quest queues/state with deliberately controlled live interleavings. Replay preserves recorded ordering even when test-host scheduling differs.
- **TK05 — Task trees:** A managed task starts and awaits other managed tasks, including repeated calls to the same task function. Results and task identity remain correctly associated during replay.
- **TK06 — Awaited exceptions:** A managed task raises and its awaiting parent catches the exception or lets it propagate to fail the workflow. On replay, task code reconstructs the authored path; only exceptions from recorded steps have the step-outcome caching guarantee. Do not imply detached task errors automatically terminate the workflow.
- **TK07 — Repeated suspension (optional extension of TK03):** Resume managed tasks waiting on distinct queues more than once if this adds a distinct reconstruction check. Avoid repeating the queue concurrency matrix; TK04 owns controlled shared-operation ordering and QU05 owns item delivery.
- **TK08 — Managed lifetimes:** Workflow code explicitly signals children to finish and awaits them before returning; all resources are cleaned up. Do not treat unawaited children as automatically durable independent workflows.

## 3. `State` — `quest_test/test_state.py`

- **SA01 — Scope:** Enter a state context, observe the initial value, use `get`/`set`, and observe removal on exit.
- **SA02 — Values:** Set a small representative selection of scalar, empty, and nested values; reads and snapshots agree. State is reconstructed by workflow code, not independently restored as a persisted object; disk/type fidelity belongs to PH06 and contract question 6.
- **SA03 — Visibility:** A compact state-focused case checks public/private scope and internally usable but externally absent `external=False` state. Follow the agreed public-resource selection convention; ID01–ID03 own the broader identity/role combinations.
- **SA04 — Independence:** Equal names under different identities and different names under one identity do not interfere.
- **SA05 — Replay:** Rebuild state through recorded reads/writes across suspension and verify the current value. WF10 owns the detailed no-historical-publication assertion.
- **SA06 — Lifetime cleanup:** Normal exit and a caught exception remove state; a later phase can reuse the same identity/name without stale state leaking.
- **SA07 — Notifications:** A waiting relevant observer receives an updated current snapshot after a state change. WF07 owns slow-consumer/coalescing coverage.

## 4. `Action` — `quest_test/test_action.py`

- **AC01 — Scoped callable:** Register a standalone callable with `Action`, invoke it externally with positional/keyword arguments, and receive its result.
- **AC02 — Temporal availability:** Invocation before entry and after exit fails, even when the caller knows the action name; invocation while exposed succeeds.
- **AC03 — Identity scope:** Invoke the correct identity/name combination; other identities cannot access the action. A shared role permits callers presenting that role to use it.
- **AC04 — Hidden action:** `external=False` actions are not advertised and cannot be externally invoked merely by knowing their names. Standalone `Action` has no public internal-invoke method; calling its original callable directly would bypass action routing. RE03–RE04 cover tracked internal invocation through decorated resource methods.
- **AC05 — Invocation failure:** The callable raises; only that invocation fails. The workflow remains usable and can accept a later valid invocation.
- **AC06 — Request recovery:** A request submitted through `Workflow.invoke_action` and interrupted after durable acceptance completes through replay using real persistent history. A published confirmation supports a lost-response caller; there is no invocation-result lookup API. CX04 extends this with manager discovery and nested steps, rather than repeating all variants here.
- **AC07 — Withdrawal race:** Two callers observe an action; the first invocation causes the workflow to withdraw it before the second invocation. The second fails gracefully, without reviving the resource or failing the workflow.
- **AC08 — Awaiting action bodies:** Use an action that yields while executing alongside another invocation. Assert application-defined synchronization and valid outcomes, not blanket atomicity across awaits.
- **AC09 — Replay gate:** A known external action cannot be invoked before reconstruction has finished, even if its registration has already been replayed internally.
- **AC10 — Cleanup (part of AC02):** Cover normal exit and caught-exception withdrawal in temporal-availability tests. A later phase may reuse the name; simultaneous collisions remain an open contract.

## 5. `action` and `Resource` — `quest_test/test_resource.py`

These are separate exports exercised together because `action` decorates resource methods.

- **RE01 — Resource registration:** A subclass with multiple decorated methods and `State` members exposes exactly the intended surface during its context and removes it on exit.
- **RE02 — Decorator forms:** A compact resource covers `@action`, `@action()`, and explicit `name=...`; default and custom names are resource-scoped. RE04 supplies the `external=False` variant.
- **RE03 — Internal routing:** Call a decorated method from workflow code, including one decorated method calling another, across one replay boundary. Verify observable effects participate in recorded ordering; do not imply atomicity across awaits or duplicate external-request recovery.
- **RE04 — Hidden method:** An internal-only decorated method is callable by workflow code but neither advertised nor externally invocable.
- **RE05 — Naming isolation:** Two resource instances with different names under the same identity, and the same name under different identities, do not collide.
- **RE06 — Inheritance (optional):** A small case uses an inherited decorated method and a decorated override. Defer undecorated-override semantics unless this becomes an intended consumer requirement.
- **RE07 — State plus action (part of RE01):** An action updates a member `State`; observe the resulting surface and scope cleanup in the registration test. Do not require every intermediate registration snapshot.
- **RE08 — Reusable phases:** Use a short loop with a resource per phase; after replay, the active phase calls the correct implementation and no prior-phase operations remain. Keep this focused on registration/routing reuse, not a broad restart matrix.
- **RE09 — Exception cleanup (variant of RE01):** A caught exception in the resource scope removes states/actions and leaves subsequent phases usable. This is scope cleanup, not the invocation-only failure case AC05.

## 6. `Queue` — `quest_test/test_queue.py`

- **QU01 — FIFO:** Put distinguishable values in a known order, including a repeated equal item and one nested payload, then assert the consumed sequence. Do not expand this into a JSON-fidelity matrix.
- **QU02 — External input:** The surface exposes `name.put`, not `name.get`; external put delivers its payload to the waiting workflow.
- **QU03 — Blocking:** An empty get waits without completing; a later put releases it with the correct item.
- **QU04 — Replay backlog:** Enqueue multiple items, consume only some, suspend, and resume; consumed items are not redelivered and remaining items retain their order.
- **QU05 — Concurrent users:** A small managed-producer/consumer case delivers each distinguishable submission once and preserves explicitly coordinated ordering on replay. Do not assume a universal scheduler order; TK04 owns the broader shared-operation replay-order check.
- **QU06 — Scope and lifetime:** Queue put availability ends with scope exit. A newly constructed queue in a later phase is independent of the old instance; do not infer a general backlog-retention policy across resource destruction or reopening the same instance. Use one representative scope check, not the whole identity matrix.

## 7. `IdentityQueue` — `quest_test/test_identity_queue.py`

- **IQ01 — Correlation:** `put(item)` returns an identity and the workflow's `get()` yields that same identity with the same item.
- **IQ02 — Uniqueness:** Multiple submissions to the queue, including equal payloads, produce distinct identities. Do not assert UUID formatting or derive authentication guarantees from identity generation.
- **IQ03 — Targeted continuation:** Publish subsequent state/actions under the returned identity. That submitter can access them; other submitter identities cannot.
- **IQ04 — Replay stability:** Capture an externally returned identity, suspend before consumption, reopen real persistent history, and verify the consumed pair retains that identity. Do not require a separate IdentityQueue-only subprocess test.
- **IQ05 — Identity-preserving delivery:** Verify put is exposed and get is hidden, plus one small FIFO/backlog or controlled concurrent-submission case that could detect swapped identity/item pairs. Do not repeat Queue's generic blocking, cleanup, and concurrency matrix.

## 8. `IDENTITY` — identity cases across surface tests

`IDENTITY` is a public type alias, not an independent runtime service. Test its meaning through actual resources rather than testing Python's type-alias representation.

- **ID01 — Public:** `None` resources are available as public resources to every observer under the intended resource-stream identity-selection convention.
- **ID02 — Private:** String identities scope resources; knowing another identity's resource name does not expand the presented scope.
- **ID03 — Roles and unions:** A view presenting multiple identities sees the appropriate combined surface; multiple callers presenting one role share its resources.
- **ID04 — Namespaces (covered by AL06):** Routing identifiers and resource identities have distinct roles. Identical text must not grant resource scope, but alias-versus-canonical-ID collision policy remains contract-gated.

Authentication and entitlement to present an identity belong to the hosting application, not Quest tests.

## 9. `these` — `quest_test/test_these.py`

- **TH01 — Collections:** Enter lists, dictionaries, and generators of real Quest resources. All expected resources are active inside the combined context and absent afterward.
- **TH02 — Returned values:** Dictionary keys and list/generator order associate entered resources with their positions. Do not assert internal container types or unusual context-enter replacement behavior pending contract question 3.
- **TH03 — Empty and mixed collections:** Empty collections work; non-context values alongside resources remain available unchanged where supported.
- **TH04 — Dynamic addition:** Add a resource to an entered list context; it becomes active immediately and is removed with the enclosing scope.
- **TH05 — Nested usage:** A small nested group of representative resources leaves no stale surface on exit. Combine with TH06's caught-exception variant rather than retesting each member's own behavior.
- **TH06 — Exceptions:** A caught exception in the grouped body removes all entered resources. Entry-failure cleanup and exact exit ordering require the contract decision below.
- **TH07 — Replay:** One small deterministic generated group with dynamic addition reconstructs its intended active surface. WF10 and AC09 own replay publication/invocation barriers; no second broad replay matrix is needed.

## 10. `when` and `get_current_timestamp` — `quest_test/test_time.py`

- **TI01 — Clock interoperability:** `get_current_timestamp()` values can be compared with `when()` values and used as release boundaries without timezone/type errors. Basic successive clock reads are comparable; do not assume strict monotonicity of a wall clock.
- **TI02 — Fresh execution:** With no history remaining, `when()` reports current time, bounded by surrounding real clock reads rather than an exact expected timestamp.
- **TI03 — Replay time:** While events remain, `when()` follows the design's recorded-event time definition, not the restart's wall-clock time.
- **TI04 — Catch-up transition:** Once the final historical event is replayed, `when()` switches to current time. Extend TI03 with representative step/task/resource events rather than a Cartesian boundary matrix. Exact event advancement/inclusivity awaits contract question 10.
- **TI05 — Compatible code release:** Histories paused at different points run compatible old branches while replaying and new branches for new execution after a release timestamp. Include a fresh workflow started after the release.
- **TI06 — Multiple releases (optional):** Two compatible releases over multiple restarts can extend TI05 if migration complexity warrants it; the baseline is one compatible deployment at different history positions.
- **TI07 — Incompatible change:** A time branch selecting a history-incompatible path produces a replay mismatch rather than silently accepting it.

## 11. `Workflow` — `quest_test/test_workflow.py`

- **WF01 — Lifecycle:** Enter an async workflow context, await completion, and obtain its result; cover positional/keyword arguments and `None` results.
- **WF02 — Task handle:** Await the public `task` handle for success and failure; its outcome agrees with awaiting the workflow. Do not assume stable object identity between property accesses.
- **WF03 — Suspension:** Exit while the workflow waits, then resume from the same persistent history using a fresh workflow object. Completion boundaries and deterministic local reconstruction are preserved.
- **WF04 — Multiple recoveries:** Suspend at several distinct boundaries and resume repeatedly without losing progress or repeating completed effects.
- **WF05 — Failure:** An unhandled workflow exception reaches awaiters and active resource observers and is terminal. A small ordinary caught-exception path continues interacting; ST06 and AC05 separately own step-exception replay and invocation-only failures.
- **WF06 — Stream initial view:** An observer entering an active workflow's resource stream receives its current visible surface, including the empty view when appropriate.
- **WF07 — Snapshots, not logs:** A slow consumer sees the latest view after several changes; no requirement to observe every intermediate scene.
- **WF08 — Independent observers:** Two observers with overlapping/different identities receive their appropriate current views. Closing one stream early does not break the workflow or the other stream.
- **WF09 — Stream termination:** Active streams close on normal completion and propagate terminal workflow failure. Resource cleanup does not leave observers blocked forever.
- **WF10 — Replay publication barrier:** Start observing immediately on restart. No historical scenes are published; the first delivered snapshot represents replay-complete execution.
- **WF11 — Invocation integration (covered by AC01–AC09):** Those scenarios must call public `Workflow.invoke_action`, not resource internals; no duplicate workflow-level invocation matrix is needed.
- **WF12 — Independent workflows:** Two concurrent bare workflow instances with independent histories and distinguishable outcomes demonstrate resource/task/result isolation. Manager namespaces and multi-workflow restart belong to MA04.

## 12. `WorkflowManager` — `quest_test/test_manager.py`

- **MA01 — Start and route:** Start different workflow types with distinct IDs and arguments; route streams and action invocations to the intended workflow.
- **MA02 — Active duplicates:** Reject a duplicate active workflow ID without disturbing the original workflow.
- **MA03 — Manager restart:** Exit with unfinished workflows, reconstruct the manager with fresh factories/history/storage objects, and verify all unfinished workflows resume with their original type and arguments.
- **MA04 — Isolation:** Use a small set of workflows at different progress points, including one failure (MA11), and verify unaffected execution/recovery. Test distinct manager namespaces sharing one storage root once here; PH03 separately checks history namespaces.
- **MA05 — Outcomes:** While running, result retrieval waits and returns the eventual value or raises the workflow exception.
- **MA06 — Retention:** With `delete_on_finish=False`, retrieve successful results and reconstructable exceptions repeatedly, including after a fresh-process restart.
- **MA07 — Deletion policy:** With `delete_on_finish=True`, completed workflows are removed. With retained outcomes, result retrieval with `delete=True` removes the stored outcome, including across restart.
- **MA08 — Explicit active deletion (contract-gated):** After agreeing deletion timing and retention semantics, delete an active workflow and verify the agreed restart behavior. MA07 already covers retained-outcome removal. Do not conflate this with a universal author-facing cancellation primitive.
- **MA09 — Presence and metrics (contract-gated):** Agree whether presence includes retained outcomes and what metrics represent, then assert the supported states/type/ID. Do not freeze current method naming, timestamp formatting, or an assumed active-only view.
- **MA10 — Missing targets:** Missing workflow routing, result lookup, and deletion fail without affecting other workflows; avoid importing non-exported exception classes.
- **MA11 — Mixed outcomes (part of MA04):** One workflow fails while others wait or finish; unrelated workflows remain usable and recoverable. Reuse the isolation setup.
- **MA12 — Exit races (contract-gated extension):** Once shutdown/durability boundaries are agreed, coordinate completion or accepted-action progress during manager exit and verify the promised recoverable execution or retained outcome. Do not use undefined write-interruption behavior as a baseline assertion.
- **MA13 — Abrupt restart:** Terminate a subprocess without normal context exit after externally witnessed progress; a new process discovers and resumes unfinished workflows. Test retained outcomes separately after an externally witnessed completion.

## 13. `alias` — `quest_test/test_alias.py`

- **AL01 — Temporary routing:** Inside `async with alias(...)`, streams and actions can use the alternate ID and reach the same workflow; canonical routing remains valid.
- **AL02 — Lifetime:** On normal scope exit or a caught exception, alias routing ceases without deleting the workflow. A later workflow can use the released alias.
- **AL03 — Conflict:** Two active workflows attempting the same alias cannot silently hijack one another's routing; both remain canonically reachable. Assert observable rejection, not a private exception type. Alias/canonical-ID collisions remain unresolved.
- **AL04 — Multiple aliases:** One workflow has several scoped aliases; withdrawing one does not remove the others or canonical access.
- **AL05 — Restart:** Replaying a workflow inside an alias scope restores live routing; ended scopes are not resurrected. This tests replay-driven registration, not independently persisted alias dictionaries, and can reuse a manager-restart test.
- **AL06 — Identity separation:** One authorized invocation succeeds and a wrong-identity invocation through the alias fails. Routing text, even if equal to a resource identity, does not grant that identity. Broader scope combinations belong to ID01–ID03; only routing operations are required to resolve aliases unless further agreed.

## 14. `PersistentHistory` — `quest_test/test_persistence.py`

- **PH01 — Real recovery:** A workflow backed by this history records progress, a fresh history instance loads it, and replay reconstructs the correct continuation.
- **PH02 — Accumulated history:** A focused multi-reopen case appends new progress after each reconstruction and preserves logical sequence. Let relevant ST/TK/resource tests use persistent history instead of building an all-features history mega-test.
- **PH03 — Isolation:** Interleave progress in two history namespaces in the same real storage, reopen them independently, and verify changes to one do not alter the other. Assert outcomes, not namespace filenames or hash formats.
- **PH04 — Collection operations (contract-gated):** Agree whether raw history entries/schema are public before testing `append`, iteration, reverse iteration, `remove`, and `clear` directly. If supported, use agreed valid entries (potentially obtained from a workflow-backed history), not private ledger imports or invented encodings; assert logical order/removals across reopening.
- **PH05 — Lifecycle outcomes (covered by PH01 and MA06):** PH01 proves unfinished execution can recover; MA06 proves retained terminal outcomes survive restart. Terminal history pruning is not a separate guarantee without an explicit contract.
- **PH06 — Serialization boundaries:** Representative supported JSON data and reconstructable exceptions survive real disk serialization, not just in-memory replay.

## 15. `LocalFileSystemBlobStorage` — `quest_test/test_storage.py`

- **BS01 — Directory creation:** Construct storage at a missing nested temporary root and use it successfully.
- **BS02 — Round trips:** Write/read supported JSON values, including Unicode, nested/empty structures, booleans, numbers, and null where supported; preserve JSON value semantics.
- **BS03 — Existence and overwrite:** `has_blob` distinguishes missing/present keys; overwriting a key changes its value without changing another key.
- **BS04 — Deletion:** Delete an existing blob, verify absence, and recreate it successfully.
- **BS05 — Reopening:** Fresh storage instances read the same values. Existing disk-backed manager/process tests supply cross-process coverage; a storage-only subprocess test is optional, not another baseline harness.
- **BS06 — Value independence:** With one nested mutable value, mutate the original input and a decoded read result, then verify a later read still returns the stored value.
- **BS07 — Real integration (suite-level rule):** Required disk-backed history/manager restart scenarios use this real storage, not an in-memory substitute. This is a fixture/coverage choice, not a separate storage test.

## 16. Cross-feature scenarios — `quest_test/test_integration.py` and `quest_test/test_process_recovery.py`

These complement the feature tests; do not duplicate every feature across every axis. Prioritize CX01–CX04 and CX09. Other entries identify shared coverage or optional consumer examples, not additional mandatory end-to-end suites. Manager-based recipes depend on the serializer setup decision.

- **CX01 — Durable conversation:** A loop consumes queue input, rebuilds local conversation history, and calls a recorded model-like step with a real side-effect witness. Restart across turns without re-requesting consumed input or repeating completed calls. No real model service is necessary.
- **CX02 — Multi-user phased interaction:** Public `IdentityQueue` enrollment leads to a private participant phase. Recover once across this transition and verify the returned identity scopes the current interface. Add concurrency/shared-role confirmation only if it closes a gap; do not require restart in every phase of a game-like mega-test.
- **CX03 — External-system routing:** A recorded step obtains an external identifier; `alias` registers it; manager routing delivers queue input under that identifier while identity filtering still applies. Restart and later withdraw the alias.
- **CX04 — Pending invocation:** Manager + persistent history + a resource action containing nested steps. Stop while the action is incomplete; replay completes the accepted request without repeating its completed nested effects. Publish confirmation for a caller whose response was lost.
- **CX05 — Failure separation (covered by feature tests):** AC05, TK06, WF05/WF09, and MA06 establish invocation-only failure, caught/awaited errors, terminal streams, and retained exceptions. No combined failure mega-test is required.
- **CX06 — Replay and interface gate (covered by WF10 and AC09):** Keep both observer publication and known-action invocation barriers as baseline assertions. Combining them with dynamic groups and many tasks is optional; do not drop invocation gating merely because publication gating is covered.
- **CX07 — Time-compatible deployment (optional):** TI05 establishes compatible deployment and MA03 establishes manager restart. Combine them only if manager-driven multi-history migration adds a distinct risk.
- **CX08 — Namespace and failure isolation (covered by MA04, PH03, AL02):** Reuse manager/history isolation and alias-lifetime tests rather than multiplying all outcomes, deletion policies, and routing combinations.
- **CX09 — Idempotent external effects:** A real subprocess is stopped after an effect but before its step completes. An application-provided idempotency key makes retry safe; contrast this with the allowed duplicate-attempt behavior of best-effort execution.
- **CX10 — Downtime-aware application (optional consumer example):** Persist an application deadline and recover into an author-defined expiry branch if a real consumer requirement warrants it. Expiry policy is application-owned, not a missing automatic Quest guarantee.
- **CX11 — Cooperative permanent stop (covered by TK08 and manager completion tests):** An author-defined signal lets children finish and be awaited; configured completion policy governs restart. This is not a separate built-in cancellation contract.

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

**Graceful suspension:** Run until the target gate, leave the public `Workflow` context, and open a fresh workflow against the same history with fresh host gates/events. Do not add an explicit suspension-settling wait or `CancelledError` assertion to every replay test: the user requested direct history reuse, supported by the focused probe described in the status section. One dedicated test checks the cancelled public task handle after exit, using a bounded wait that does not itself cancel the handle. More complex shutdown/cleanup timing remains subject to the contract questions below.

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
- **Replay lifecycle:** Keep the graceful-suspension/reopening sequence visible and simple: exit, refresh host coordination, reopen the same history. Do not introduce a mandatory suspend-and-settle helper. Keep task-cancellation assertions in their dedicated test.
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
8. **Manager lifecycle and inspection:** When does deletion become durable, does it override outcome retention for a running workflow, and what does returning from manager exit guarantee? Which operations resolve aliases? Does `has_workflow` include retained outcomes, and do metrics describe active workflows, retained outcomes, or both? Do not conflate manager deletion with a general permanent-cancellation facility.
9. **Terminal streams:** Active streams must close/fail as designed. What should opening a new stream for an already completed/retained workflow do, and what is an observer meant to see when a workflow is suspended rather than completed?
10. **Timestamp precision:** The design defines replay event-time behavior. Confirm boundary inclusivity and which event advances `when()` before writing exact timestamp assertions; require comparison compatibility, not undocumented strict clock monotonicity.
11. **Storage errors and keys:** Are missing reads/deletes errors or idempotent operations, which keys are valid, and what is the expected response to corrupt JSON or I/O failures? Path confinement/security cases depend on whether keys are trusted or externally supplied.
12. **Use outside workflow context:** Which authoring operations should reject calls without an active workflow/manager context, and is a public error contract intended? Avoid freezing incidental `ContextVar` errors.

## Suggested implementation order

1. Basic workflow lifecycle, steps, real filesystem storage/history, and a clean restart; establish checkpoint gates and effect witnesses without mandatory suspension-settling waits. Step tests now supply list-backed replay coverage; real persistence coverage remains planned.
2. State/action surface, identities, streams, queues, and resource lifecycle; establish snapshot-predicate waiting.
3. Managed concurrency, nested replay, and exceptional outcomes.
4. Manager retention, aliases, grouped/dynamic contexts, and time-compatible changes.
5. Cross-process durability, interrupted invocations, shutdown races, and selected end-to-end combinations.

This order establishes trustworthy public-API test helpers before adding the most timing-sensitive scenarios. Contract questions should be resolved as their affected scenarios are reached; implementation gaps should remain visible rather than becoming the expected behavior.
