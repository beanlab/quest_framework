from typing import Any, Dict, Tuple
import importlib


class MasterSerializer:
    def serialize(self, obj: Any) -> Any:
        # Check if it is a known type - directly serializable to JSON
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        elif isinstance(obj, dict):
            return {self.serialize(k): self.serialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.serialize(v) for v in obj]
        elif isinstance(obj, tuple):
            return tuple(self.serialize(v) for v in obj)
        # Unknown type (Object) - serialize using type and constructor arguments
        else:
            args, kwargs = self._get_constructor_args(obj)
            return {
                '_ms_type': obj.__module__ + '.' + obj.__class__.__name__,
                'args': self.serialize(args),
                'kwargs': self.serialize(kwargs)
            }

    # Reconstruct original python object using dictionary
    def deserialize(self, data: Any) -> Any:
        # Check data - JSON-serializable type
        if isinstance(data, (int, float, str, bool, type(None))):
            return data
        elif isinstance(data, list):
            return [self.deserialize(v) for v in data]
        elif isinstance(data, tuple):
            return tuple(self.deserialize(v) for v in data)
        elif isinstance(data, dict):
            if '_ms_type' in data:
                # Reconstruct object using the type and constructor arguments in the dictionary
                cls = self._import_class(data['_ms_type'])
                # Recursively deserialize - args and kwargs might be serialized object
                args = self.deserialize(data.get('args', []))
                kwargs = self.deserialize(data.get('kwargs', {}))
                # Instantiate object
                return cls(*args, **kwargs)
            else:
                return {self.deserialize(k): self.deserialize(v) for k, v in data.items()}
        else:
            return data

    # Retrieves the constructor arguments from the object's __dict__
    def _get_constructor_args(self, obj: Any) -> Tuple[Tuple, Dict]:
        args = ()
        if hasattr(obj, '__getstate__'):
            kwargs = obj.__getstate__()
        else:
            kwargs = obj.__dict__.copy()
        return args, kwargs

    # Imports and returns the class with full module and class name(string)
    def _import_class(self, full_class_string: str):
        # split class string -> module path, class name
        module_path, class_name = full_class_string.rsplit('.', 1)
        module = importlib.import_module(module_path)
        # Retrieve class from module
        cls = getattr(module, class_name)
        # class object
        return cls
