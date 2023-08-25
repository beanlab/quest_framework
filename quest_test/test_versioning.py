import asyncio

import pytest

from src.quest import queue, Historian, version, get_version

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


CASING_V2 = '2023-08-25 title case'


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
                    phrase.append(word)
                else:
                    phrase.append(await fix_case(word) + '2')
        return phrase

    historian = Historian('test', application, history)
    result = historian.run()
    await asyncio.sleep(0.1)
    await historian.record_external_event('words', None, 'put', 'bar')

    # Yet another version of application is deployed
    # Use title-case instead of upper
    @version(CASING_V2)
    async def fix_case(word: str) -> str:
        if get_version() < CASING_V2:
            return word.upper()
        else:
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

    assert (await result) == ['foo', 'BAR2', 'Baz2']
