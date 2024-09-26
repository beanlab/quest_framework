import asyncio

my_queue = asyncio.Queue()


async def fetch_data():
    input_num = input('Please enter your number: ')
    return f'The given number is: {input_num}'


async def async_generator():
    for number in range(8):
        await my_queue.get()
        print('Got resource update from queue!')
        print('Yielding data from resource update')
        yield number


async def resource_queue_puts():
    while True:
        print('Simulating time between resource events...')
        await asyncio.sleep(8)
        await my_queue.put('task')
        print('Resource update happened! Put onto queue!')


async def main():
    put_queue_task = asyncio.create_task(resource_queue_puts())
    # Use the async generator
    index = 0
    async for data in async_generator():
        if index == 0:
            assert data == 1
            print(f'Resource update: {data}. Assert passed')
        elif index == 1:
            assert data == 2
            print(f'Resource update: {data}. Assert passed')
        index += 1


if __name__ == "__main__":
    asyncio.run(main())
