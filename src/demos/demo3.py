import asyncio

from quest import State, IdentityQueue, Queue, Workflow


async def workflow():
    async with (
        State(None, 'message', 'Enter a name'),
        IdentityQueue(None, 'name') as name_q
    ):
        ident, name = await name_q.get()

    async with (
        State(None, 'message', 'Nothing more for you'),
        State(ident, 'message', f'You entered: {name}. Now enter a number.'),
        Queue(ident, 'number') as number_q
    ):
        number = await number_q.get()

    async with State(ident, 'message', f'You entered {number}'):
        await asyncio.sleep(3)


async def wait_for_action(work: Workflow, identity, action_name):
    async with work.resource_stream([identity]) as stream:
        async for resources in stream:
            print('RESOURCES', resources)
            if (identity, action_name) in resources.actions:
                return resources


async def main():
    async with Workflow('demo', workflow) as work:
        resources = await wait_for_action(work, None, 'name.put')
        print(resources)
        name = input('Name: ')
        ident = await work.invoke_action(None, 'name.put', name)
        print('IDENT', ident)

        resources = await wait_for_action(work, ident, 'number.put')
        print(resources)
        number = int(input('Number: '))
        await work.invoke_action(ident, 'number.put', number)
        
        async with work.resource_stream([None, ident]) as stream:
            async for resources in stream:
                print(resources)


if __name__ == '__main__':
    from quest.logs import set_suggested_log_format, enable_debug_logging

    set_suggested_log_format()
    # enable_debug_logging()
    asyncio.run(main())
