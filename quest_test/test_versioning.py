import asyncio

import pytest
from src.quest import queue, Historian, version, get_version, task, step, state

V2 = '2023-08-25 append "2" to words'


@pytest.mark.asyncio
async def test_versioning():
    async def application():
        phrase = []
        async with queue('words', None) as words:
            while len(phrase) < 3:
                word = await words.get()
                phrase.append(word)
        return phrase

    history = []
    historian = Historian('test', application, history)
    historian.run()
    await asyncio.sleep(0.1)
    await historian.record_external_event('words', None, 'put', 'foo')
    await historian.record_external_event('words', None, 'put', 'bar')
    await asyncio.sleep(0.01)

    # Application shuts down
    await historian.suspend()

    # New version of application is deployed
    @version(V2)
    async def application():
        phrase = []
        async with queue('words', None) as words:
            while len(phrase) < 3:
                word = await words.get()
                if get_version() < V2:
                    phrase.append(word)
                else:
                    phrase.append(word + '2')
        return phrase

    historian = Historian('test', application, history)
    result = historian.run()
    await asyncio.sleep(0.1)
    await historian.record_external_event('words', None, 'put', 'baz')

    assert (await result) == ['foo', 'bar', 'baz2']


CASING_V2 = '2023-08-26 title case'


@pytest.mark.asyncio
async def test_multi_versioning():
    async def application():
        phrase = []
        async with queue('words', None) as words:
            while len(phrase) < 3:
                word = await words.get()
                phrase.append(word)
        return phrase

    history = []
    historian = Historian('test', application, history)
    historian.run()
    await asyncio.sleep(0.1)
    await historian.record_external_event('words', None, 'put', 'foo')
    await asyncio.sleep(0.01)

    # Application shuts down
    await historian.suspend()

    # "foo" should be preserved as-is

    # New version of application is deployed
    # Make word upper and append '2'
    async def fix_case(word):
        return word.upper()

    @version(V2)
    async def application():
        phrase = []
        async with queue('words', None) as words:
            while len(phrase) < 3:
                word = await words.get()
                if get_version() < V2:
                    # i.e. "foo" gets replayed this way
                    phrase.append(word)
                else:
                    # new words will go this way
                    phrase.append(await fix_case(word) + '2')
        return phrase

    historian = Historian('test', application, history)
    result = historian.run()
    await asyncio.sleep(0.1)
    await historian.record_external_event('words', None, 'put', 'bar')
    await asyncio.sleep(0.01)
    await historian.suspend()

    # "bar" should become "BAR2"

    # Yet another version of application is deployed
    # Use title-case instead of upper
    @version(CASING_V2)
    async def fix_case(word: str) -> str:
        if get_version() < CASING_V2:
            # 'bar' is replayed here ('foo' never reaches this function)
            return word.upper()
        else:
            # new words run through this code
            return word.title()

    @version(V2)
    async def application():
        phrase = []
        async with queue('words', None) as words:
            while len(phrase) < 3:
                word = await words.get()
                if get_version() < V2:
                    phrase.append(word)
                else:
                    phrase.append(await fix_case(word) + '2')
        return phrase

    historian = Historian('test', application, history)
    result = historian.run()
    await asyncio.sleep(0.1)
    await historian.record_external_event('words', None, 'put', 'baz')
    await asyncio.sleep(0.1)

    assert (await result) == ['foo', 'BAR2', 'Baz2']


@pytest.mark.asyncio
async def test_named_versions():
    """
    This test exercises two things. Understand them well.

    First: named versions.
    You can version separate parts of the code independently.
    Name the parts of the code and provide a separate version for each.

    Second: versioning code that awaits
    If a task is on an await when the workflow suspends,
    and then that line of code is versioned,
    the task will resume from the ORIGINAL line version, not the new version.

    """

    @task
    async def get_numbers(name):
        async with queue('numbers', name) as nums:
            return [await nums.get(), await nums.get(), await nums.get()]

    async def application(name1, name2, name3):
        t1 = get_numbers(name1)
        t2 = get_numbers(name2)
        t3 = get_numbers(name3)
        return (await t1) + (await t2) + (await t3)

    history = []

    name1 = 'foo'
    name2 = 'bar'
    name3 = 'baz'

    historian = Historian('test', application, history)
    result = historian.run(name1, name2, name3)
    await asyncio.sleep(0.1)

    await historian.record_external_event('numbers', name1, 'put', 1)
    await asyncio.sleep(0.1)

    await historian.suspend()

    # New version!
    @task
    @version(first_num='2')
    async def get_numbers(name):
        async with queue('numbers', name) as nums:
            result = []

            if get_version('first_num') < '2':
                result.append(await nums.get())
            else:
                result.append(await nums.get() + 2)

            result.append(await nums.get())
            result.append(await nums.get())

            return result

    # historian = Historian('test', application, history)
    result = historian.run(name1, name2, name3)
    await asyncio.sleep(0.1)

    await historian.record_external_event('numbers', name1, 'put', 1)  # 2nd number
    await historian.record_external_event('numbers', name2, 'put', 1)  # 1st number
    await asyncio.sleep(0.1)

    await historian.suspend()

    # New Version!
    @task
    @version(first_num='2', third_num='2')
    async def get_numbers(name):
        async with queue('numbers', name) as nums:
            result = []

            if get_version('first_num') < '2':
                result.append(await nums.get())
            else:
                result.append(await nums.get() + 2)

            result.append(await nums.get())

            if get_version('third_num') < '2':
                result.append(await nums.get())
            else:
                result.append(await nums.get() + 3)

            return result

    result = historian.run(name1, name2, name3)
    await asyncio.sleep(0.1)

    await historian.record_external_event('numbers', name1, 'put', 1)  # 3rd number
    await historian.record_external_event('numbers', name2, 'put', 1)  # 2nd number
    await historian.record_external_event('numbers', name3, 'put', 1)  # 1st number
    await asyncio.sleep(0.1)

    await historian.suspend()

    # And another version!
    @task
    @version(first_num='2', third_num='3')
    async def get_numbers(name):
        async with queue('numbers', name) as nums:
            result = []

            if get_version('first_num') < '2':
                result.append(await nums.get())
            else:
                result.append(await nums.get() + 2)

            result.append(await nums.get())

            if get_version('third_num') < '2':
                result.append(await nums.get())
            elif get_version('third_num') < '3':
                result.append(await nums.get() + 3)
            else:
                result.append(await nums.get() + 4)

            return result

    result = historian.run(name1, name2, name3)
    await asyncio.sleep(0.1)

    await historian.record_external_event('numbers', name2, 'put', 1)  # 3rd number
    await historian.record_external_event('numbers', name3, 'put', 1)  # 2nd number
    await historian.record_external_event('numbers', name3, 'put', 1)  # 3rd number
    await asyncio.sleep(0.1)

    assert (await result) == [
        1, 1, 1,  # name 1
        1, 1, 4,  # name 2
        1, 1, 5,  # name 3
    ]
