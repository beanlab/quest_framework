from typing import Any, Dict, Protocol, TypeVar, Callable, Tuple, TypedDict
from utils import get_obj_name, import_object
import inspect

T = TypeVar('T')


class TypeFactory(Protocol[T]):
    def __call__(self, *args: Any, **kwargs: Any) -> T:
        ...


class SerializedData(TypedDict):
    _ms_factory: str
    args: Any
    kwargs: Any


class StepSerializer(Protocol):
    async def serialize(self, obj: Any) -> Any:
        ...

    async def deserialize(self, data: Any) -> Any:
        ...


class TypeSerializer(Protocol[T]):
    async def serialize(self, obj: T) -> Tuple[Callable[..., T], Tuple[Any, ...], Dict[str, Any]]:
        ...


class NoopSerializer(StepSerializer):
    async def serialize(self, obj: Any) -> Any:
        # object already JSON-serializable
        return obj

    async def deserialize(self, data: Any) -> Any:
        return data


class MasterSerializer(StepSerializer):
    def __init__(self, type_serializers: Dict[type, TypeSerializer[Any]]):
        self._type_serializers = type_serializers

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
        obj_type = type(obj)
        serializer = self._type_serializers.get(obj_type)
        if serializer:
            factory, args, kwargs = await serializer.serialize(obj)
            if inspect.isfunction(factory) and factory.__name__ == "<lambda>":
                raise ValueError("Factory should not be a lambda function.")
            factory_name = get_obj_name(factory)
            return SerializedData(
                _ms_factory=factory_name,
                args=await self.serialize(args),
                kwargs=await self.serialize(kwargs)
            )

        # Default
        return await NoopSerializer().serialize(obj)

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
            if '_ms_factory' in data:
                # Import factory using full name
                factory_name = data['_ms_factory']
                factory = import_object(factory_name)
                if not callable(factory):
                    raise ValueError(f"Factory {factory_name} is not callable.")
                # Recursively deserialize - args and kwargs might be serialized object
                args = await self.deserialize(data.get('args', []))
                kwargs = await self.deserialize(data.get('kwargs', {}))

                # Reconstruct object
                return factory(*args, **kwargs)
            else:
                return {await self.deserialize(k): await self.deserialize(v) for k, v in data.items()}
        else:
            # Default
            return await NoopSerializer().deserialize(data)
