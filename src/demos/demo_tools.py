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

                    choice = input('Action: ')
                    if not choice:
                        # quest_logger.debug('Next')
                        continue
                    index, *args = choice.split()
                    ident, name = resources.actions[int(index)]
                    result = await work.invoke_action(ident, name, *args)
                    if name == 'name.put' and result not in identities:  # identity queue
                        raise NewIdentity(result)
            break

        except NewIdentity as new_id:
            identities.append(new_id.identity)
