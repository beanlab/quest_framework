import asyncio

from quest import Workflow, IDENTITY


class NewIdentity(BaseException):
    def __init__(self, ident):
        self.identity = ident
        super().__init__(ident)


async def admin_view(work: Workflow):
    identities: list[IDENTITY] = [None]
    while True:
        try:
            async with work.resource_stream(identities) as stream:
                async for resources in stream:
                    print('STATE')
                    for ident, name, value in resources.state:
                        print(ident, name, value)
                    print()

                    print('ACTIONS')
                    for index, (ident, name) in enumerate(resources.actions):
                        print(f'{index})', ident, name)
                    print()

                    if not resources.actions:
                        continue

                    while True:
                        choice = input('Action: ')
                        parts = choice.strip().split()
                        if not parts:
                            # quest_logger.debug('Next')
                            break

                        index, *args = parts
                        try:
                            index = int(index)
                        except ValueError:
                            print('Please enter a valid action number.')
                            continue

                        if not 0 <= index < len(resources.actions):
                            print('Action number is out of range.')
                            continue
                        break

                    if not parts:
                        continue

                    ident, name = resources.actions[index]
                    print('ARGS', args)
                    result = await work.invoke_action(ident, name, *args)
                    if isinstance(result, asyncio.Task):
                        assert False, result
                    if name == 'name.put' and result not in identities:  # identity queue
                        raise NewIdentity(result)
            break

        except NewIdentity as new_id:
            identities.append(new_id.identity)
