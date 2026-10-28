import asyncio
import traceback
from typing import Callable, TypedDict, Any

from .ledger import Ledger
from .logging import quest_logger
from .replay_stream import ReplayStream
from .serializer import StepSerializer
from .suspension import is_suspension
from .unique_ids import UniqueIdHandler


class StepResult(TypedDict):
    result: Any | None
    exception: Any | None


class StepHandler:
    def __init__(self,
                 ledger: Ledger,
                 unique_id_handler: UniqueIdHandler,
                 replay_stream: ReplayStream,
                 serializer: StepSerializer
                 ):
        self._ledger = ledger
        self._unique_id_handler = unique_id_handler
        self._replay_stream = replay_stream
        self._serializer = serializer

    async def run_step(self, task_id, func_name, func: Callable, *args, **kwargs):
        step_id = self._unique_id_handler.get_unique_id(task_id, func_name)
        return await self._run_with_pruning(task_id, step_id, func_name, func, *args, **kwargs)

    async def _run_with_pruning(self, task_id, step_id, func_name, func: Callable, *args, **kwargs):
        prune_on_exit = True
        try:
            return await self._run_step_with_record_check(task_id, step_id, func_name, func, *args, **kwargs)

        except asyncio.CancelledError as cancel:
            if is_suspension(cancel):
                prune_on_exit = False
                raise
        finally:
            if prune_on_exit:
                await _prune(step_id, self._ledger)

    async def _run_step_with_record_check(self, task_id, step_id, func_name, func: Callable, *args, **kwargs):

        if (next_record := await self._replay_stream.get_next_record(task_id)) is None:
            # The replay has finished and we are now running live code
            quest_logger.debug(f'Starting step {func_name} with {args} and {kwargs}')
            await self._ledger.add_record(
                task_id=task_id,
                entry_type='start',
                entry_id=step_id,
                data=None
            )

        else:
            with next_record as record:
                assert record['entry_id'] == step_id, f'{record["entry_id"]} != {step_id}'
                if record['entry_type'] == 'end':
                    # When sub-step records are pruned, only the 'end' record remains
                    # So when we replay, if we find an 'end' record at the beginning of the step,
                    #  it means the step has already completed and we should rehydrate the result
                    if record['data']['exception'] is not None:
                        exception_data = record['data']['exception']
                        ex = _deserialize_exception(exception_data)
                        raise ex

                    deserialized_result = await self._serializer.deserialize(record['data']['result'])
                    return deserialized_result

                else:
                    assert record['entry_type'] == 'start'

        return await self._run_step_with_task_prefix(task_id, step_id, func_name, func, *args, **kwargs)

    async def _run_step_with_task_prefix(self, task_id, step_id, func_name, func: Callable, *args, **kwargs):
        # We keep add the unique step ID to the prefix in order to clearly identify
        # all records under the scope of a step
        # This way we can cleanly prune all obsolete records after a step finishes.

        # We use the last token of the step_id because this has been uniquified,
        # while the function name might be repeated
        unique_func_name = step_id.split('.')[-1]

        try:
            self._unique_id_handler.push_prefix(task_id, unique_func_name)
            return await self._run_step_with_recorded_result(task_id, step_id, func_name, func, *args, **kwargs)

        finally:
            self._unique_id_handler.pop_prefix(task_id)

    async def _run_step_with_recorded_result(self, task_id, step_id, func_name, func: Callable, *args, **kwargs):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, '__await__'):
                result = await result
            quest_logger.debug(f'Completing step {func_name} with {result}')

            serialized_result = await self._serializer.serialize(result)

            await self._ledger.add_record(
                task_id=task_id,
                entry_type='end',
                entry_id=step_id,
                data=StepResult(result=serialized_result, exception=None)
            )

            return result

        except asyncio.CancelledError as cancel:
            if is_suspension(cancel):
                raise
            else:
                quest_logger.debug(f'{step_id} canceled')
                serialized_exception = _serialize_exception(cancel)
                await self._ledger.add_record(
                    task_id=task_id,
                    entry_type='end',
                    entry_id=step_id,
                    data=StepResult(result=None, exception=serialized_exception)
                )
                raise

        except BaseException as ex:
            serialized_exception = _serialize_exception(ex)
            quest_logger.debug(f'Exception in {step_id}: {serialized_exception}')

            await self._ledger.add_record(
                task_id=task_id,
                entry_type='end',
                entry_id=step_id,
                data=StepResult(result=None, exception=serialized_exception)
            )
            raise

        except:
            quest_logger.exception('Unhandled error passing through handle_step!')
            raise


# History pruning
# - cutting out nested steps
# - but how do we know which external events "belonged" to a given step
# - several external events may come in for one task while the other performs a step
# - can an external event be associated with a task?
# - the external event IS associated with a resource
# - all resources are created in a task; are all associations with a resource unique to that task?
#  - they ARE associated with that task and all sub-tasks.
#  - but that means that a resource can be created in the top task and passed to two sub-tasks
#  - then one task can interact with the resource and external events while the other performs sub-steps
# - so...we'd have to know which later task was affected by the external event
#
#  E    A
#  .    S
#  1    |    # We don't want to miss replaying this event, even though it falls between the step records
#  .    S
#  .  ->1
#
#  E    A
#  .    S
#  1    |    # This event is safe to skip because the event resource is scoped to the step
#  .  ->1
#  .    S
#
# If a resource was created and destroyed within a step, all events associated with that resource can be skipped.
# If a resource was created outside a step, actions on that resource should be preserved.
# If the author puts "side effect" work on a resource inside a step, that's a problem.
# Principle: all resources, associated events, and substeps that happen in a step will be entirely skipped.
#            Never have side effects that are not entirely scoped to the step.
#
# To prune correctly, I need to turn process the sequence like a tree
# Each task and step is a separate branch
# I need to look for resources that are open in each branch and match the relevant events

async def _prune(step_id: str, ledger: Ledger):
    """
    Remove substep work
    Records whose step_ids are prefixed by the step_id of the step are substep work
    Keep external events that belong to resources created outside the step
    """
    items = reversed(await ledger.get_records())
    to_delete = []

    # Last record should be a step with the give step ID
    record = next(items)
    assert record['entry_type'] == 'end', f'{record["entry_type"]} != end'
    assert record['entry_id'] == step_id, f'{record["entry_id"]} != {step_id}'

    end_of_life_resources = set()
    try:
        while record := next(items):
            if record['entry_id'].startswith(step_id):
                # Found a sub-record of the step, we can delete it
                # But if it is a resource scoped to this step, keep track of it first
                if record['entry_type'] == 'delete_resource':
                    end_of_life_resources.add(record['entry_id'])
                to_delete.append(record)

            if record['entry_type'] == 'external' and record['entry_id'] in end_of_life_resources:
                to_delete.append(record)

            if record['entry_id'] == step_id:
                # Found the beginning of the step, we can stop searching
                break

    except StopIteration:
        pass

    await ledger.remove_records(to_delete)


def _get_type_name(obj):
    return obj.__class__.__module__ + '.' + obj.__class__.__name__


def _get_exception_class(exception_type: str):
    module_name, class_name = exception_type.rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    exception_class = getattr(module, class_name)
    return exception_class


def _serialize_exception(ex: BaseException) -> dict:
    return {
        "type": _get_type_name(ex),
        "args": ex.args,
        "traceback": traceback.format_exc()
    }


def _deserialize_exception(data: dict) -> Exception:
    exc_cls = _get_exception_class(data["type"])
    exc_instance: Exception = exc_cls(*data.get("args", ()))
    exc_instance._original_traceback = data["traceback"]  # Include original tracebook info
    return exc_instance
