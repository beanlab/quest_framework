from .tasks import get_task_id, external_task_name


class UniqueIdHandler:
    def __init__(self, workflow_id: str):
        # The workflow ID is used as the task name for the root task
        self._prefix = {
            workflow_id: [],
            external_task_name: [],
        }

        self._unique_ids: set[str] = set()
        self._prefix = {}

    def _get_prefixed_name(self, event_name: str) -> str:
        return '.'.join(self._prefix[get_task_id()]) + '.' + event_name

    def get_unique_id(self, event_name: str) -> str:
        prefixed_name_root = self._get_prefixed_name(event_name)
        prefixed_name = prefixed_name_root
        counter = 0
        while prefixed_name in self._unique_ids:
            counter += 1
            prefixed_name = f'{prefixed_name_root}_{counter}'
        self._unique_ids.add(prefixed_name)
        return prefixed_name

    def push_prefix(self, unique_step_name: str):
        self._prefix[get_task_id()].append(unique_step_name)

    def pop_prefix(self):
        self._prefix[get_task_id()].pop(-1)


