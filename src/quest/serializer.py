from typing import Any, Dict, Protocol, TypeVar, TypedDict

T = TypeVar('T')


class SerializedData(TypedDict):
    _ms_type: str
    args: Any
    kwargs: Any


class StepSerializer(Protocol):
    async def serialize(self, obj: Any) -> Any:
        ...

    async def deserialize(self, data: Any) -> Any:
        ...


class TypeSerializer(Protocol[T]):
    async def serialize(self, obj: T) -> tuple[tuple, dict]:
        ...

    async def deserialize(self, *args, **kwargs) -> T:
        ...


class NoopSerializer(StepSerializer):
    async def serialize(self, obj: Any) -> Any:
        # object already JSON-serializable
        return obj

    async def deserialize(self, data: Any) -> Any:
        return data


class MasterSerializer(StepSerializer):
    def __init__(self, type_serializers: Dict[type, TypeSerializer[Any]]):
        self._type_serializers = {str(tp): ser for tp, ser in type_serializers.items()}

    async def serialize(self, obj: Any) -> Any:
        # Check if it is a known type - directly serializable to JSON
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        elif isinstance(obj, dict):
            return {await self.serialize(k): await self.serialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [await self.serialize(v) for v in obj]
        elif isinstance(obj, tuple):
            return tuple([await self.serialize(v) for v in obj])

        # Check if custom serializer is registered for the object's type
        obj_type = str(type(obj))
        serializer = self._type_serializers.get(obj_type)
        if serializer:
            args, kwargs = await serializer.serialize(obj)
            return SerializedData(
                _ms_type=obj_type,
                args=args,
                kwargs=kwargs
            )

        # Default
        return obj

    # Reconstruct original python object using dictionary
    async def deserialize(self, data: Any) -> Any:
        # Check data - JSON-serializable type
        if isinstance(data, (int, float, str, bool, type(None))):
            return data
        elif isinstance(data, list):
            return [await self.deserialize(v) for v in data]
        elif isinstance(data, tuple):
            return tuple([await self.deserialize(v) for v in data])
        elif isinstance(data, dict):
            if '_ms_type' in data:
                obj_type = data['_ms_type']
                serializer = self._type_serializers[obj_type]
                args = data.get('args', [])
                kwargs = data.get('kwargs', {})
                return await serializer.deserialize(*args, **kwargs)
            else:
                return {await self.deserialize(k): await self.deserialize(v) for k, v in data.items()}
        else:
            # Default
            return data
