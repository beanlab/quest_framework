import asyncio
import random


class Historian:
    def __init__(self):
        self._resource_stream = ResourceStream()

    async def run_workflow(self):
        for i in range(10):
            print('simulating workflow...')
            await asyncio.sleep(5)

    def get_resource_stream(self):
        return self._resource_stream


class StreamContextManager:
    def __init__(self, identity):
        self._identity = identity

    def __enter__(self):
        print(f'opening resource stream instance for {self._identity}')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f'closing resource stream instance for {self._identity}')
        print(f'exc_type: {exc_type}')
        print(f'exc_val: {exc_val}')
        print(f'exc_tb: {exc_tb}')

class ResourceStream:
    async def stream(self, identity):
        with StreamContextManager(identity):
            for i in range(10):
                print(f'about to yield the {i}th num to {identity}')
                yield random.randint(1, 100)


async def client1():
    index = 0
    try:
        resource_stream = historian.get_resource_stream()
        async for num in resource_stream.stream('client1'):
            if index == 3:
                raise Exception('foobarbaz')
            print(f'client 1 received: {num}')
            await asyncio.sleep(2)
            index += 1
    except Exception as ex:
        print('Client 1 errored and left the call to stream')
        assert 'foobarbaz' in str(ex)

    await asyncio.sleep(10)
    print('Client 1 done sleeping')


async def client2():
    resource_stream = historian.get_resource_stream()
    async for num in resource_stream.stream('client2'):
        print(f'client 2 received: {num}')
        await asyncio.sleep(2)


historian = Historian()


async def main():
    await asyncio.gather(historian.run_workflow(), client1(), client2())


if __name__ == "__main__":
    asyncio.run(main())
