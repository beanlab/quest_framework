import shutil
import uuid
import pytest
from quest import *
from quest.workflow import *

STOP_EVENT_NAME = 'stop'
OTHER_EVENT_NAME = 'other_event'
event_counter = 0
not_event_counter = 0


def create_workflow_manager(flow_function, flow_function_name, path) -> WorkflowManager:
    saved_state = path / "saved-state"

    workflow_manager = WorkflowManager(
        JsonMetadataSerializer(saved_state),
        JsonEventSerializer(saved_state / 'workflow_state'),
        {flow_function_name: StatelessWorkflowSerializer(flow_function)}
    )
    return workflow_manager


def get_workflow_id() -> str:
    return str(uuid.uuid4())


# an event test should have an event call counter + a stop external event
def event_test(tmp_path, workflow_func, workflow_name, run_test, event_counter_correct,
               not_event_counter_correct, return_val):
    global event_counter, not_event_counter
    event_counter = 0
    not_event_counter = 0
    workflow_manager = create_workflow_manager(workflow_func, workflow_name, tmp_path)
    workflow_id = get_workflow_id()
    run_test(workflow_manager, workflow_id, workflow_func, event_counter_correct,
             not_event_counter_correct, return_val)


class InternalEventFlow:

    @event
    def count(self):
        global event_counter
        event_counter += 1

    def not_event_count(self):
        global not_event_counter
        not_event_counter += 1

    @external_event(STOP_EVENT_NAME)
    def stop(self): ...

    def __call__(self):
        self.count()
        self.not_event_count()
        self.stop()
        return "finished"


def call_count_stop_external(workflow_manager, workflow_id, workflow_func, event_counter_correct,
                             not_event_counter_correct, *args, **kwargs):
    with workflow_manager:
        result = workflow_manager.start_workflow(workflow_id, workflow_func())
        assert result is None
        assert event_counter == event_counter_correct
        assert not_event_counter == not_event_counter_correct


def call_signal_external(workflow_manager, workflow_id, workflow_func, event_counter_correct,
                         not_event_counter_correct, return_val, *args, **kwargs):
    with workflow_manager:
        # Start workflow. Should return none because of the stop, would have called count once
        result = workflow_manager.start_workflow(workflow_id, workflow_func())
        assert result is None
        assert event_counter == event_counter_correct
        assert not_event_counter == not_event_counter_correct
        # Signal the workflow to start again, call count should still be 1 and not 2
        result = workflow_manager.signal_workflow(workflow_id, STOP_EVENT_NAME, None)
        assert result is not None
        assert result.payload == return_val
        assert event_counter == event_counter_correct
        assert not_event_counter == not_event_counter_correct * 2


def kill_context_in_stop(workflow_manager, workflow_id, workflow_func, event_counter_correct,
                         not_event_counter_correct, *args, **kwargs):
    with workflow_manager:
        # Start workflow. Should return none because of the stop, would have called count once
        result = workflow_manager.start_workflow(workflow_id, workflow_func())
        assert result is None
        assert event_counter == event_counter_correct
        assert not_event_counter == not_event_counter_correct
    with workflow_manager:
        assert event_counter == event_counter_correct
        assert not_event_counter == not_event_counter_correct * 2


def kill_context_after_stop(workflow_manager, workflow_id, workflow_func, event_counter_correct,
                            not_event_counter_correct, return_val, *args, **kwargs):
    with workflow_manager:
        # Start workflow. Should return none because of the stop, would have called count once
        result = workflow_manager.start_workflow(workflow_id, workflow_func())
        assert result is None
        assert event_counter == event_counter_correct
        assert not_event_counter == not_event_counter_correct
        # Signal the workflow to start again, call count should still be 1 and not 2
        result = workflow_manager.signal_workflow(workflow_id, STOP_EVENT_NAME, None)
        assert result is not None
        assert result.payload == return_val
        assert event_counter == event_counter_correct
        assert not_event_counter == not_event_counter_correct * 2
    with workflow_manager:
        assert event_counter == event_counter_correct
        assert not_event_counter == not_event_counter_correct * 3


def test_call_count_stop_external(tmp_path):
    event_test(tmp_path, InternalEventFlow, 'InternalEventFlow', call_count_stop_external, 1, 1, "finished")


def test_call_signal_external(tmp_path):
    event_test(tmp_path, InternalEventFlow, 'InternalEventFlow', call_signal_external, 1, 1, "finished")


def test_kill_context_in_stop(tmp_path):
    event_test(tmp_path, InternalEventFlow, 'InternalEventFlow', kill_context_in_stop, 1, 1, "finished")


def test_kill_context_after_stop(tmp_path):
    event_test(tmp_path, InternalEventFlow, 'InternalEventFlow', kill_context_after_stop, 1, 1, "finished")


#################################################################################################################

@event
def count():
    global event_counter
    event_counter += 1


def not_event_count():
    global not_event_counter
    not_event_counter += 1


class ExternalEventFlow:

    @external_event(STOP_EVENT_NAME)
    def stop(self): ...

    def __call__(self):
        count()
        not_event_count()
        self.stop()
        return "finished"


def test_external_event_call_count_stop_external(tmp_path):
    event_test(tmp_path, ExternalEventFlow, 'ExternalEventFlow', call_count_stop_external, 1, 1, "finished")


def test_external_event_call_signal_external(tmp_path):
    event_test(tmp_path, ExternalEventFlow, 'ExternalEventFlow', call_signal_external, 1, 1, "finished")


def test_external_event_kill_context_in_stop(tmp_path):
    event_test(tmp_path, ExternalEventFlow, 'ExternalEventFlow', kill_context_in_stop, 1, 1, "finished")


def test_external_event_kill_context_after_stop(tmp_path):
    event_test(tmp_path, ExternalEventFlow, 'ExternalEventFlow', kill_context_after_stop, 1, 1, "finished")


#################################################################################################################

class OtherClass:
    @event
    def count(self):
        global event_counter
        event_counter += 1

    def not_event_count(self):
        global not_event_counter
        not_event_counter += 1


class EventInOtherClassEventFlow:

    @external_event(STOP_EVENT_NAME)
    def stop(self): ...

    def __call__(self):
        other_class = OtherClass()
        other_class.count()
        other_class.not_event_count()
        self.stop()
        return "finished"


def test_event_other_class_call_count_stop_external(tmp_path):
    event_test(tmp_path, EventInOtherClassEventFlow, 'EventInOtherClassEventFlow', call_count_stop_external, 1, 1,
               "finished")


def test_event_other_class_call_signal_external(tmp_path):
    event_test(tmp_path, EventInOtherClassEventFlow, 'EventInOtherClassEventFlow', call_signal_external, 1, 1,
               "finished")


def test_event_other_class_kill_context_in_stop(tmp_path):
    event_test(tmp_path, EventInOtherClassEventFlow, 'EventInOtherClassEventFlow', kill_context_in_stop, 1, 1,
               "finished")


def test_event_other_class_kill_context_after_stop(tmp_path):
    event_test(tmp_path, EventInOtherClassEventFlow, 'EventInOtherClassEventFlow', kill_context_after_stop, 1, 1,
               "finished")


#################################################################################################################

class InternalNestedEventFlow:

    @event
    def another_event(self):
        return self.count()

    @event
    def count(self):
        global event_counter
        event_counter += 1
        return event_counter

    def not_event_count(self):
        global not_event_counter
        not_event_counter += 1

    @external_event(STOP_EVENT_NAME)
    def stop(self): ...

    def __call__(self):
        self.another_event()
        second_count = self.count()
        self.stop()
        return second_count


def test_internal_nested_call_count_stop_external(tmp_path):
    event_test(tmp_path, InternalNestedEventFlow, 'InternalNestedEventFlow', call_count_stop_external, 2, 0, 2)


def test_internal_nested_call_signal_external(tmp_path):
    event_test(tmp_path, InternalNestedEventFlow, 'InternalNestedEventFlow', call_signal_external, 2, 0, 2)


def test_internal_nested_kill_context_in_stop(tmp_path):
    event_test(tmp_path, InternalNestedEventFlow, 'InternalNestedEventFlow', kill_context_in_stop, 2, 0, 2)


def test_internal_nested_kill_context_after_stop(tmp_path):
    event_test(tmp_path, InternalNestedEventFlow, 'InternalNestedEventFlow', kill_context_after_stop, 2, 0, 2)
