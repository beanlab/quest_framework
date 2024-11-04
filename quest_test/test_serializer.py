import asyncio
from dataclasses import dataclass
import pytest

from src.quest.historian import Historian
from src.quest.serializer import MasterSerializer, StepSerializer
from src.quest.wrappers import step
from src.quest.quest_types import EventRecord


# Define custom class
@dataclass
class Stuff:
    name: str
    number: int

    def make_statement(self):
        return (self.name + ' ') * self.number


# TypeSerializer for Stuff
class StuffSerializer:
    async def serialize(self, obj: Stuff):
        factory = Stuff
        args = (obj.name, obj.number)
        kwargs = {}
        return factory, args, kwargs

    async def deserialize(self, data):
        pass


# Create MasterSerializer with StuffSerializer
type_serializers = {Stuff: StuffSerializer()}
serializer = MasterSerializer(type_serializers)


@step
async def create_stuff(name: str, number: int) -> Stuff:
    return Stuff(name, number)


pause_event = asyncio.Event()


async def workflow():
    stuff = await create_stuff('hello', 2)
    await pause_event.wait()
    return stuff.make_statement()


@pytest.mark.asyncio
async def test_master_serializer():
    # Create historian with custom serializer
    history = []
    historian = Historian('test_workflow', workflow, history, serializer=serializer)

    workflow_task = historian.run()

    await asyncio.sleep(0.1)

    await historian.suspend()
    assert len(history) > 0

    # Find serialization record for 'create_stuff' step
    serialized_data = None
    for record in history:
        if record['type'] == 'end' and record['step_id'].endswith('create_stuff'):
            serialized_data = record['result']
            break

    # Verify serialized data has expected structure
    assert serialized_data is not None
    assert isinstance(serialized_data, dict)
    assert '_ms_factory' in serialized_data
    expected_factory_name = f"{Stuff.__module__}.{Stuff.__name__}"
    assert serialized_data['_ms_factory'] == expected_factory_name

    # Verify deserialized data
    deserialized_obj = await serializer.deserialize(serialized_data)
    expected_obj = Stuff(name='hello', number=2)
    assert deserialized_obj == expected_obj
    assert deserialized_obj.make_statement() == expected_obj.make_statement()

    pause_event.set()
    workflow_task = historian.run()

    result = await workflow_task

    assert result == 'hello hello '


# explicitly test serializer.serialize() and serializer.deserialize()
@pytest.mark.asyncio
async def test_serializer():
    original_obj = Stuff(name='hello', number=2)

    serialized_data = await serializer.serialize(original_obj)

    assert isinstance(serialized_data, dict)
    assert '_ms_factory' in serialized_data
    expected_factory_name = f"{Stuff.__module__}.{Stuff.__name__}"
    assert serialized_data['_ms_factory'] == expected_factory_name
    assert list(serialized_data['args']) == ['hello', 2]
    assert serialized_data['kwargs'] == {}

    deserialized_obj = await serializer.deserialize(serialized_data)

    assert deserialized_obj == original_obj
    assert deserialized_obj.make_statement() == original_obj.make_statement()
