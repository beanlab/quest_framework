from typing import Callable, Protocol


class Render(Protocol):
    def __call__(self, text: str, offsets: list[tuple], groups: list | dict) -> str: ...



def render_binary_groups(text: str, offsets: list[tuple], groups: list | dict) -> str:
    pass


class FancyRenderer:
    def __init__(self, dependency):
        self.dependency = dependency

    def __call__(self, text: str, offsets: list[tuple], groups: list | dict):
        pass


def cluster_and_display(text: str, offsets: list[tuple], render: Render) -> str:
    """
    Cluster the chunks indicated by offets and return an HTML string rendering each chunk in the associated cluster
    """
    clusters = ...
    return render(text, offsets, clusters)


cluster_and_display('foo bar', [(0, 1, 1, 1), (1, 2, 1, 1)], render_binary_groups)


class Interface:
    foo: int


class Foo(Interface):
    foo: int


class Bar:
    foo: int


class HasFoo(Protocol):
    foo: int


def quux(thing: HasFoo):
    return thing.foo


quux(Bar())
quux(Foo())
