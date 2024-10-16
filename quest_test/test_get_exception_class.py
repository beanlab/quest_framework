import pytest

from quest_test.custom_errors.custom_error import MyError
from src.quest.historian import get_exception_class


class CustomExceptionOne(Exception):
    pass


class CustomExceptionTwo(Exception):
    pass


def test_standard_exceptions():
    assert get_exception_class('builtins.ValueError') == ValueError
    assert get_exception_class('builtins.KeyError') == KeyError
    assert get_exception_class('builtins.IndexError') == IndexError
    assert get_exception_class('builtins.BufferError') == BufferError
    assert get_exception_class('builtins.MemoryError') == MemoryError
    assert get_exception_class('builtins.OverflowError') == OverflowError
    assert get_exception_class('builtins.RecursionError') == RecursionError
    assert get_exception_class('builtins.RuntimeError') == RuntimeError


def test_custom_exceptions():
    exception_class = get_exception_class('quest_test.custom_errors.custom_error.MyError')
    with pytest.raises(exception_class) as exc_info:
        raise exception_class("This is a custom error message")
    assert str(exc_info.value) == "This is a custom error message"


def test_weird_custom_exception():
    exception_class = get_exception_class('quest_test.custom_errors.custom_error.WeirdCustomException')
    with pytest.raises(exception_class) as exc_info:
        raise exception_class("This is a weird custom error", 404)
        assert str(exc_info.value) == "This is a weird custom error (Error Code: 404)"
        assert exc_info.value.get_code() == 404
