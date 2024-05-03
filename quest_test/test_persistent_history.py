from src.quest.persistence import InMemoryBlobStorage, ListPersistentHistory, LinkedPersistentHistory
from src.quest.types import StepStartRecord, ResourceEntry, TaskEvent, EventRecord
from datetime import datetime, UTC

step_rec = StepStartRecord()
step_rec['type'] = 'start'; step_rec['timestamp'] = datetime.now(UTC).isoformat()
step_rec['step_id'] = 'record_1_identifier'; step_rec['task_id'] = 'record_1_task'

step_rec2 = StepStartRecord()
step_rec2['type'] = 'start'; step_rec2['timestamp'] = datetime.now(UTC).isoformat()
step_rec2['step_id'] = 'record_2_identifier'; step_rec2['task_id'] = 'record_2_task'

task_rec = TaskEvent()
task_rec['type'] = 'start_task'; task_rec['timestamp'] = datetime.now(UTC).isoformat()
task_rec['step_id'] = 'record_3_identifier'; task_rec['task_id'] = 'record_3_task'

records: list[EventRecord] = [step_rec, step_rec2, task_rec]

def test_linked_history():
    storage = InMemoryBlobStorage()
    linked_history = LinkedPersistentHistory('linked_history', storage)

    # append API
    for record in records:
        linked_history.append(record)
    
    # iter API
    rec_count = 0
    for record in linked_history:
        assert record is records[rec_count]
        rec_count += 1

    # reversed API
    rec_count = 2
    for record in reversed(linked_history):
        assert record is records[rec_count]
        rec_count -= 1
    
    # remove API
    linked_history.remove(step_rec2)
    for record in linked_history:
        assert record is not step_rec2

    linked_history.remove(step_rec)
    for record in linked_history:
        assert record is not step_rec

    linked_history.remove(task_rec)
    counter = 0
    for record in linked_history:
        counter += 1
    assert counter == 0

def test_list_history():
    storage = InMemoryBlobStorage()
    linked_history = ListPersistentHistory('linked_history', storage)

    # append API
    for record in records:
        linked_history.append(record)
    
    # iter API
    rec_count = 0
    for record in linked_history:
        assert record is records[rec_count]
        rec_count += 1

    # reversed API
    rec_count = 2
    for record in reversed(linked_history):
        assert record is records[rec_count]
        rec_count -= 1
    
    # remove API
    linked_history.remove(step_rec2)
    for record in linked_history:
        assert record is not step_rec2

    linked_history.remove(step_rec)
    for record in linked_history:
        assert record is not step_rec

    linked_history.remove(task_rec)
    counter = 0
    for record in linked_history:
        counter += 1
    assert counter == 0