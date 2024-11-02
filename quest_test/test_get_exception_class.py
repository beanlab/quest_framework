import pytest

from quest_test.custom_errors.custom_error import MyError
from quest.historian import _get_exception_class


class CustomExceptionOne(Exception):
    pass


class CustomExceptionTwo(Exception):
    pass


def test_standard_exceptions():
    assert _get_exception_class('builtins.ValueError') == ValueError
    assert _get_exception_class('builtins.KeyError') == KeyError
    assert _get_exception_class('builtins.IndexError') == IndexError
    assert _get_exception_class('builtins.BufferError') == BufferError
    assert _get_exception_class('builtins.MemoryError') == MemoryError
    assert _get_exception_class('builtins.OverflowError') == OverflowError
    assert _get_exception_class('builtins.RecursionError') == RecursionError
    assert _get_exception_class('builtins.RuntimeError') == RuntimeError


def test_custom_exceptions():
    exception_class = _get_exception_class('quest_test.custom_errors.custom_error.MyError')
    with pytest.raises(exception_class) as exc_info:
        raise exception_class("This is a custom error message")
    assert str(exc_info.value) == "This is a custom error message"


def test_weird_custom_exception():
    exception_class = _get_exception_class('quest_test.custom_errors.custom_error.WeirdCustomException')
    with pytest.raises(exception_class) as exc_info:
        raise exception_class("This is a weird custom error", 404)
        assert str(exc_info.value) == "This is a weird custom error (Error Code: 404)"
        assert exc_info.value.get_code() == 404
