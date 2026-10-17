import asyncio
from typing import Callable, Protocol

from quest.refactor.suspension import is_suspension
from .ledger import Ledger
from .logging import logger as quest_logger
from .tasks import get_task_id
from .utils import get_current_timestamp


def _prune(step_id: str, history: "History"):
    """
    Remove substep work
    Records whose step_ids are prefixed by the step_id of the step are substep work
    Keep external events that belong to resources created outside the step
    """
    items = reversed(history)
    to_delete = []

    # Last record should be a step with the give step ID
    record = next(items)
    assert record['type'] == 'end', f'{record["type"]} != end'
    assert record['step_id'] == step_id, f'{record["step_id"]} != {step_id}'

    end_of_life_resources = set()
    try:
        while record := next(items):
            if record['step_id'].startswith(step_id):
                # Found a sub-record of the step, we can delete it
                # But if it is a resource scoped to this step, keep track of it first
                if record['type'] == 'delete_resource':
                    end_of_life_resources.add(record['resource_id'])
                to_delete.append(record)

            if record['type'] == 'external' and record['resource_id'] in end_of_life_resources:
                to_delete.append(record)

            if record['step_id'] == step_id:
                # Found the beginning of the step, we can stop searching
                break

    except StopIteration:
        pass

    for record in to_delete:
        history.remove(record)


class PrefixHandler(Protocol):
    def get_unique_id(self, func_name) -> str: ...

    def push_prefix(self, step_name): ...

    def pop_prefix(self): ...


class StepHandler:
    def __init__(self, ledger: Ledger, prefix_handler: PrefixHandler):
        self._ledger = ledger
        self._prefix_handler = prefix_handler

    async def run_step(self, func_name, func: Callable, *args, **kwargs):
        step_id = self._prefix_handler.get_unique_id(func_name)
        return await self._run_with_pruning(step_id, func_name, func, *args, **kwargs)

    async def _run_with_pruning(self, step_id, func_name, func: Callable, *args, **kwargs):
        prune_on_exit = True
        try:
            return await self._run_step_with_record_check(step_id, func_name, func, *args, **kwargs)

        except asyncio.CancelledError as cancel:
            if is_suspension(cancel):
                prune_on_exit = False
                raise
        finally:
            if prune_on_exit:
                _prune(step_id, self._ledger)

    async def _run_step_with_record_check(self, step_id, func_name, func: Callable, *args, **kwargs):

        if (next_record := await self._ledger.get_next_record()) is None:
            # The replay has finished and we are now running live code
            quest_logger.debug(f'Starting step {func_name} with {args} and {kwargs}')
            self._history.append(StepStartRecord(
                type='start',
                timestamp=get_current_timestamp(),
                task_id=get_task_id(),
                step_id=step_id
            ))

        else:
            with next_record as record:
                assert record['step_id'] == step_id, f'{record["step_id"]} != {step_id}'
                if record['type'] == 'end':
                    # When sub-step records are pruned, only the 'end' record remains
                    # So when we replay, if we find an 'end' record at the beginning of the step,
                    #  it means the step has already completed and we should rehydrate the result
                    if record['exception'] is not None:
                        exception_data = record['exception']
                        ex = deserialize_exception(exception_data)
                        raise ex

                    deserialized_result = await self._serializer.deserialize(record['result'])
                    return deserialized_result

                else:
                    assert record['type'] == 'start'

        return await self._run_step_with_task_prefix(step_id, func_name, func, *args, **kwargs)

    async def _run_step_with_task_prefix(self, step_id, func_name, func: Callable, *args, **kwargs):
        # We keep add the unique step ID to the prefix in order to clearly identify
        # all records under the scope of a step
        # This way we can cleanly prune all obsolete records after a step finishes.

        # We use the last token of the step_id because this has been uniquified,
        # while the function name might be repeated
        unique_func_name = step_id.split('.')[-1]

        try:
            self._prefix_handler.push_prefix(unique_func_name)
            return await self._run_step_with_recorded_result(step_id, func_name, func, *args, **kwargs)

        finally:
            self._prefix_handler.pop_prefix()

    async def _run_step_with_recorded_result(self, step_id, func_name, func: Callable, *args, **kwargs):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, '__await__'):
                result = await result
            quest_logger.debug(f'Completing step {func_name} with {result}')

            serialized_result = await self._serializer.serialize(result)

            self._history.append(StepEndRecord(
                type='end',
                timestamp=get_current_timestamp(),
                task_id=get_task_id(),
                step_id=step_id,
                result=serialized_result,
                exception=None
            ))

            return result

        except asyncio.CancelledError as cancel:
            if is_suspension(cancel):
                raise
            else:
                quest_logger.debug(f'{step_id} canceled')
                serialized_exception = serialize_exception(cancel)
                self._history.append(StepEndRecord(
                    type='end',
                    timestamp=get_current_timestamp(),
                    task_id=get_task_id(),
                    step_id=step_id,
                    result=None,
                    exception=serialized_exception
                ))
                raise

        except BaseException as ex:
            serialized_exception = serialize_exception(ex)
            quest_logger.debug(f'Exception in {step_id}: {serialized_exception}')

            self._history.append(StepEndRecord(
                type='end',
                timestamp=get_current_timestamp(),
                step_id=step_id,
                task_id=get_task_id(),
                result=None,
                exception=serialized_exception
            ))
            raise

        except:
            quest_logger.exception('Unhandled error passing through handle_step!')
            raise
