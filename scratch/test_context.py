from contextvars import Context
from typing import Generator, Any


class ContextDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __enter__(self):
        for key, value in self.items():
            if hasattr(value, '__enter__'):
                self[key] = value.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for key, value in self.items():
            if hasattr(value, '__exit__'):
                value.__exit__(exc_type, exc_val, exc_tb)


class ContextList(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __enter__(self):
        for index, value in enumerate(self):
            if hasattr(value, '__enter__'):
                self[index] = value.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for value in self:
            if hasattr(value, '__exit__'):
                value.__exit__(exc_type, exc_val, exc_tb)


def these(collection_of_contexts: dict | list | Generator[Context, Any, None]):
    if isinstance(collection_of_contexts, dict):
        return ContextDict(collection_of_contexts)
    else:
        return ContextList(collection_of_contexts)


class Context:
    def __init__(self, name, enter, exit):
        self.name = name
        self.enter = enter
        self.exit = exit

    def __enter__(self):
        print(self.name, 'entered')
        self.enter(self.name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(self.name, 'exited')
        self.exit(self.name)


def contexts(names, enter, exit):
    return these(Context(name, enter, exit) for name in names)
    # For example, if the pattern of making an identical set of queues or states
    # for a collection of identities is common, we can have wrappers like this one


def test_context_dict():
    entered = []
    exited = []
    values = []
    with these({k: Context(k, entered.append, exited.append) for k in 'abcd'}) as data:
        for k, v in data.items():
            values.append(v.name)

    expected = list('abcd')
    assert entered == expected
    assert exited == expected
    assert values == expected


def test_context_list():
    entered = []
    exited = []
    with these(Context(k, entered.append, exited.append) for k in 'abcd') as data:
        values = [v.name for v in data]

    expected = list('abcd')
    assert entered == expected
    assert exited == expected
    assert values == expected


def test_generator():
    entered = []
    exited = []
    with contexts('abcd', entered.append, exited.append) as data:
        values = [v.name for v in data]

    expected = list('abcd')
    assert entered == expected
    assert exited == expected
    assert values == expected