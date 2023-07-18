import pytest

from src.quest import step
from src.quest.historian import Historian
from src.quest.wrappers import task


@step
async def plus_foo(text):
    return text + 'foo'


@task
async def three_foo(text):
    text = await plus_foo(text)
    text = await plus_foo(text)
    text = await plus_foo(text)
    return text


async def fooflow(text1, text2):
    first = three_foo(text1)
    second = three_foo(text2)
    return await first, await second


@pytest.mark.asyncio
async def test_step_concurrency():
    historian = Historian(
        'test',
        fooflow,
        [],
        {}
    )

    assert await historian.run('abc', 'xyz') == ('abcfoofoofoo', 'xyzfoofoofoo')



@task
@step
async def double(text):
    return text + text


async def doubleflow(text):
    first = double(text)
    second = double(text[:3])
    return await first + await second


@pytest.mark.asyncio
async def test_step_tasks():
    historian = Historian(
        'test',
        doubleflow,
        [],
        {}
    )

    assert await historian.run('abcxyz') == 'abcxyzabcxyzabcabc'
