from typing import Any, Dict, Protocol, TypeVar, Callable
from utils import get_obj_name, import_object

T = TypeVar('T')


class StepSerializer(Protocol):
    async def serialize(self, obj: Any) -> Any:
        ...

    async def deserialize(self, data: Any) -> Any:
        ...


class TypeSerializer(Protocol[T]):
    async def serialize(self, obj: T) -> Dict[str, Any]:
        ...

    async def deserialize(self, data: Dict[str, Any]) -> T:
        ...


class NoopSerializer(StepSerializer):
    async def serialize(self, obj: Any) -> Any:
        # object already JSON-serializable
        return obj

    async def deserialize(self, data: Any) -> Any:
        return data


class MasterSerializer(StepSerializer):
    def __init__(self, type_serializers: Dict[type, TypeSerializer[Any]] = None):
        self._type_serializers = type_serializers or {}
        self._default_serializer = NoopSerializer()

    def register_serializer(self, obj_type: type, serializer: TypeSerializer[Any]):
        self._type_serializers[obj_type] = serializer

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
        serializer = self._get_serializer_for_type(type(obj))
        if serializer is not self._default_serializer:
            factory, args, kwargs = await serializer.serialize(obj)
            factory_name = get_obj_name(factory)
            return {
                '_ms_factory': factory_name,
                'args': await self.serialize(args),
                'kwargs': await self.serialize(kwargs)
            }

        # serializer = _default_serializer
        return await serializer.serialize(obj)

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
                # Recursively deserialize - args and kwargs might be serialized object
                args = await self.deserialize(data.get('args', []))
                kwargs = await self.deserialize(data.get('kwargs', {}))

                # Reconstruct object
                serializer = self._get_serializer_for_factory(factory)
                if serializer is not self._default_serializer:
                    return await serializer.deserialize(factory, args, kwargs)
                else:
                    return factory(*args, **kwargs)
        else:
            # Default - return data as-is
            return data

    def _get_serializer_for_type(self, obj_type: type) -> TypeSerializer[Any]:
        # If the object's type registered in self._type_serializers dictionary
        if obj_type in self._type_serializers:
            return self._type_serializers[obj_type]
        # check if obj_type is subclass of registered type
        for registered_type, serializer in self._type_serializers.items():
            if issubclass(obj_type, registered_type):
                return serializer
        # Nothing match
        return self._default_serializer

    def _get_serializer_for_factory(self, factory: Callable) -> TypeSerializer[Any]:
        # Retrieve serializer using output type of the factory
        for obj_type, serializer in self._type_serializers.items():
            # Direct match - factory = MyClass
            # Match with constructor (__init__) - factory = MyClass.__init__
            if factory == obj_type or (callable(obj_type) and factory == obj_type.__init__):
                return serializer
        # Nothing match
        return self._default_serializer
