# Quest Testing Philosophy and Guidance

Quest is not a trivial piece of software, and much of its complexity comes from nuanced async interactions. Because of this, a test that mocks low-level functionality is of little value, because it is the low-level functionality that really matters.

Tests should be designed to exercise the actual API. Tests serve both to demonstrate how to use the framework, as well as to ensure that those use cases function correctly.

The public API in which all tests should be written, and which should be thoroughly tested, are what can be imported from `quest` directly (i.e. what are made available through `src/quest/__init__.py`).

Each test should include a comment that describes what scenario the test is exercising. 