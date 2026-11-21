# Unobserved resource-stream exception Future emits asyncio diagnostic

## Observed behavior

Running the new step suite normally succeeds:

```bash
poetry run pytest -q quest_test/test_step.py
```

However, `test_replay_rejects_changed_step_sequence` produces a `Future exception was never retrieved` diagnostic after the expected replay-mismatch exception has been caught by the test.

This is separate from the logging-filter bug involving `asyncio.current_task()` outside a running event loop, which was fixed in `src/quest/logs.py`.

## Cause

`ResourceManager` in `src/quest/core/resources.py` creates a `_fatal_exception` Future. When workflow execution fails, the historian's exception-handling wrapper calls `signal_workflow_error(exc)`, which stores the failure using:

```python
self._fatal_exception.set_exception(exc)
```

The exception is carried through two independent paths:

1. The workflow task raises it to its awaiter. The replay-mismatch test awaits this task through the public workflow interface and catches the expected failure.
2. The resource manager's Future holds the same failure for resource-stream observers. `_get_resources()` retrieves it by awaiting that Future.

The test does not open a resource stream, so the second Future's exception is never retrieved. Asyncio reports the diagnostic when the Future is garbage-collected. Catching the workflow task's exception does not mark the separate Future's exception as retrieved.

## Desired behavior

- Workflow failures must still propagate to workflow awaiters.
- Resource observers must still receive terminal workflow failures through stream iteration, as required by the design contract.
- Not opening a resource stream must not itself produce an unhandled-Future diagnostic. Observing resources is optional.
- Do not fix this by suppressing asyncio logging or requiring every caller/test to create a resource observer.

## Follow-up testing

Use the public `quest` API and real async execution to cover a failing workflow with no resource observer, ensuring the expected workflow exception is caught without an unobserved-Future diagnostic. Also retain coverage that active resource-stream observers receive the workflow failure and terminate rather than hanging or silently completing.

The current stream loop also checks `_workflow_complete` before obtaining a snapshot; when addressing this issue, verify that a waiting observer actually receives the terminal exception rather than returning normally. This is a related propagation check, not a reason to weaken the stream-failure contract.

No fix for this resource-stream issue has been implemented as part of capturing this backlog item.
