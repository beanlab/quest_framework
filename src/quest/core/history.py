from typing import Protocol, Reversible, TypeVar

T = TypeVar('T')


class History[T](Protocol, Reversible):
    def append(self, item: T): ...

    def remove(self, item: T): ...

    def clear(self): ...

    # Reversible includes __iter__ and __reversed__
