class UniqueIdHandler:
    def __init__(self):
        self._prefix = {}

        self._unique_ids: set[str] = set()
        self._prefix = {}

    def _get_prefixed_name(self, task_id: str, event_name: str) -> str:
        return '.'.join(self._prefix[task_id]) + '.' + event_name

    def get_unique_id(self, task_id: str, event_name: str) -> str:
        prefixed_name_root = self._get_prefixed_name(task_id, event_name)
        prefixed_name = prefixed_name_root
        counter = 0
        while prefixed_name in self._unique_ids:
            counter += 1
            prefixed_name = f'{prefixed_name_root}_{counter}'
        self._unique_ids.add(prefixed_name)
        return prefixed_name

    def start_prefix(self, task_id: str):
        self._prefix[task_id] = [task_id]

    def push_prefix(self, task_id: str, unique_step_name: str):
        self._prefix[task_id].append(unique_step_name)

    def pop_prefix(self, task_id: str):
        self._prefix[task_id].pop(-1)


