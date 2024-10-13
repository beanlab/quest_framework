from typing import Any, Dict, Tuple
import importlib

from .utils import get_obj_name, import_object


class MasterSerializer:
    def __init__(self, type_serializers=None):
        # Initialize with a dictionary of custom serializers - default to empty dict
        self._type_serializers = type_serializers or {}

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
        serializer = self._type_serializers.get(type(obj))
        if serializer:
            factory, args, kwargs = serializer(obj)
            # Get full name of the factory (function, class)
            factory_name = get_obj_name(factory)
            return {
                '_ms_factory': factory_name,
                'args': await self.serialize(args),
                'kwargs': await self.serialize(kwargs)
            }
        else:
            # Default - return object as-is
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
                # Import factory using full name
                factory = import_object(data['_ms_type'])
                # Recursively deserialize - args and kwargs might be serialized object
                args = await self.deserialize(data.get('args', []))
                kwargs = await self.deserialize(data.get('kwargs', {}))

                # Check if the factory is coroutine function?

                # Instantiate object
                return factory(*args, **kwargs)
            else:
                return {await self.deserialize(k): await self.deserialize(v) for k, v in data.items()}
        else:
            # Default - return data as-is
            return data

    def register_serializer(self, obj_type: type, serializer_func):
        # Store serializer function using the given object type
        self._type_serializers[obj_type] = serializer_func
