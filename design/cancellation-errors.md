# Why suspension uses `CancelledError` with a message

Quest suspends workflows with `task.cancel(SUSPENDED)`, where `SUSPENDED` is the message `"__WORKFLOW_SUSPENDED__"`. This combines two distinct signals:

- **`asyncio.CancelledError`** tells asyncio that the task was cancelled, not that it failed.
- **The message** tells Quest that cancellation means suspension for later replay, rather than permanent cancellation.

## Suspension must retain cancellation semantics

`asyncio.TaskGroup`, used by `Historian._run()`, treats cancellation differently from failure. A child ending with `CancelledError` is not recorded as a failure. An unrelated custom exception triggers failure handling and is collected into an `ExceptionGroup` or `BaseExceptionGroup`. Deriving a custom signal directly from `BaseException` does not avoid this behavior.

Quest also needs the distinction: suspension leaves steps unfinished, avoids pruning their history, avoids reporting a fatal workflow error, and preserves workflow metadata for resumption. Resource and context-manager cleanup also distinguish suspension from other exits.

## Why a `CancelledError` subclass would not simplify this

A subclass would retain asyncio's cancellation semantics. Quest's exact-type checks could be updated to recognize it; those checks are not the fundamental obstacle.

The obstacle is that asyncio's cancellation API carries a **message**, not an exception type:

- `Task.cancel(message)` injects a plain `CancelledError`. It cannot inject a chosen subclass.
- Cancellation forwarded to an awaited task uses its `cancel(message)` method, rather than forwarding a custom exception instance.
- Some asyncio helpers construct fresh, plain cancellation exceptions. For example, `gather(..., return_exceptions=True)` constructs cancellation results from `CancelledError` and the task's cancellation message, not the original exception subtype.

A suspension subclass would therefore require translation from marked cancellations and could lose its identity across asyncio composition. Quest would still need the message to recognize suspension after those boundaries, adding another representation without eliminating the existing one.

Using `CancelledError(SUSPENDED)` directly matches asyncio's cancellation mechanism while giving Quest the distinction it needs. The message is not guaranteed to survive every possible cancellation path, but a subclass does not remove that limitation.
