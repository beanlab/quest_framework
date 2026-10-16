class Historian:
    """This class embodies how the Ledger is used to create the Quest experience"""

    def __init__(self, ledger: Ledger, entrypoint: Callable):
        self._ledger = ledger

    # Versioning of code
    async def when(self): ...  # Return the timestamp of the currently held record

    # Steps
    async def run_step(self, func_name, func: Callable, *args, **kwargs):
        return await self._run_with_pruning(func_name, func, *args, **kwargs)

    async def _run_with_pruning(self, func_name, func: Callable, *args, **kwargs):
        try:
            prune_on_exit = True
            return await self._run_step(func_name, func, *args, **kwargs)

        except asyncio.CancelledError as cancel:
            if cancel.args and cancel.args[0] == SUSPENDED:
                prune_on_exit = False
                raise
        finally:
            if prune_on_exit:
                _prune(step_id, self._history)

    async def _run_live_step(self, func_name, func: Callable, *args, **kwargs):
        pass

    async def _run_step(self, func_name, func: Callable, *args, **kwargs):
        step_id = self._get_unique_id(func_name)
        unique_func_name = step_id.split('.')[-1]

        if (next_record := await self._next_record()) is not None:
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

        if next_record is None:
            # i.e. the replay has finished and we are now running live code
            quest_logger.debug(f'{self._get_task_name()} starting step {func_name} with {args} and {kwargs}')
            self._history.append(StepStartRecord(
                type='start',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                step_id=step_id
            ))

        try:
            self._prefix[self._get_task_name()].append(unique_func_name)

            result = func(*args, **kwargs)
            if hasattr(result, '__await__'):
                result = await result
            quest_logger.debug(f'{self._get_task_name()} completing step {func_name} with {result}')

            serialized_result = await self._serializer.serialize(result)

            self._history.append(StepEndRecord(
                type='end',
                timestamp=_get_current_timestamp(),
                task_id=self._get_task_name(),
                step_id=step_id,
                result=serialized_result,
                exception=None
            ))

            return result

        except asyncio.CancelledError as cancel:
            if cancel.args and cancel.args[0] == SUSPENDED:
                raise
            else:
                quest_logger.debug(f'{step_id} canceled')
                serialized_exception = serialize_exception(cancel)
                self._history.append(StepEndRecord(
                    type='end',
                    timestamp=_get_current_timestamp(),
                    task_id=self._get_task_name(),
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
                timestamp=_get_current_timestamp(),
                step_id=step_id,
                task_id=self._get_task_name(),
                result=None,
                exception=serialized_exception
            ))
            raise

        except:
            quest_logger.exception('Unhandled error passing through handle_step!')
            # TODO - raise?

        finally:
            self._prefix[self._get_task_name()].pop(-1)

    # Resources
    async def add_state(self, ident, name, get_func): ...

    async def remove_state(self, ident, name): ...

    async def add_action(self, ident, name, action_func): ...

    async def remove_action(self, ident, name): ...

    async def get_state(self, ident): ...  # i.e. get_resources

    async def call_action(self, ident, name, *args, **kwargs): ...  # i.e. handle_external_event

    # Tasks
    async def start_task(self, task_name, callable, *args, ** kwargs) -> Task: ...

    # Workflow entrypoint
    async def run(self): ...

    async def suspend(self): ...