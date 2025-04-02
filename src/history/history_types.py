from typing import TypedDict, Literal, Any


class ExceptionDetails(TypedDict):
    type: str
    args: tuple
    details: str


class VersionRecord(TypedDict):
    type: Literal['set_version', 'after_version']
    timestamp: str
    step_id: str  # stores the version name
    task_id: str
    version: str


class ConfigurationRecord(TypedDict):
    type: Literal['configuration']
    timestamp: str
    step_id: Literal['configuration']
    task_id: str
    function_name: str
    args: list
    kwargs: dict


class StepStartRecord(TypedDict):
    type: Literal['start']
    timestamp: str
    step_id: str
    task_id: str


class StepEndRecord(TypedDict):
    type: Literal['end']
    timestamp: str
    step_id: str
    task_id: str
    result: Any
    exception: Any | None


class ResourceEntry(TypedDict):
    name: str
    identity: str | None
    type: str
    resource: Any


class ResourceLifecycleEvent(TypedDict):
    type: Literal['create_resource', 'delete_resource']
    timestamp: str
    step_id: str
    task_id: str
    resource_id: str
    resource_type: str


class ResourceAccessEvent(TypedDict):
    type: Literal['external', 'internal_start', 'internal_end']
    timestamp: str
    step_id: str
    task_id: str
    resource_id: str
    action: str
    args: list
    kwargs: dict
    result: Any


class TaskEvent(TypedDict):
    type: Literal['start_task', 'complete_task']
    timestamp: str
    step_id: str
    task_id: str  # the name of the task created/completed


EventRecord = StepStartRecord \
              | StepEndRecord \
              | ResourceAccessEvent \
              | TaskEvent \
              | VersionRecord \
              | ConfigurationRecord \
              | ResourceLifecycleEvent