from src.quest.workflow import find_workflow


class PersistentQueue:
    def __init__(self, name, serializer, publisher):
        self._name = name
        self._serializer = serializer
        self._publisher = publisher
        self._queue = []

    def _load(self):
        self._queue = self._serializer.load(self._name)

    def _save(self):
        self._serializer.save(self._name, self._queue)

    def __enter__(self):
        self._load()
        self._publisher.register(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._save()
        self._publisher.unregister(self)

    def push(self, item):
        self._queue.append(item)

    def pop(self):
        return self._queue.pop(0)


def queue(name):
    workflow = find_workflow()
    return PersistentQueue(name, workflow.serializer, workflow)