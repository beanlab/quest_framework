import asyncio
import random


class StreamContextManager:
    def __enter__(self):
        print('aenter called')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('aexit called', exc_type, exc_val, exc_tb)


class ResourceStream:
    def __init__(self):
        pass

    async def __aiter__(self):
        with StreamContextManager():
            for i in range(10):
                yield random.randint(1, 100)

async def client():
    index = 0
    try:
        async for num in ResourceStream():
            if index == 3:
                raise Exception('foobarbaz')
            print(num)
            index += 1
    except Exception as ex:
        assert 'foobarbaz' in str(ex)
    print('leaving client')


async def main():
    await client()


if __name__ == "__main__":
    asyncio.run(main())
