import asyncio


class VersionHandler:
    def __init__(self, unique_id_provider):
        self.unique_id_provider = unique_id_provider

    def do_version_thing(self):
        self.unique_id_provider.get_unique_id()


class StepHandler:
    def __init__(self, unique_id_provider):
        self.unique_id_provider = unique_id_provider


class Historian:
    def __init__(self, step_handler: StepHandler, version_handler: VersionHandler):
        self.step_handler = step_handler
        self.version_handler = version_handler


def main():
    unique_id_provider = None
    step_handler = StepHandler(unique_id_provider)
    version_handler = VersionHandler(unique_id_provider)
    historian = Historian(step_handler, version_handler)





# scrap above



async def main():
    workflow = None
    identity = 'foobar'

    # Poll
    while True:
        resources = workflow.get_resources(identity)
        # do something
        await asyncio.sleep(1)

    # Async stream
    async for resources in workflow.get_resource_stream(identity):
        # respond to the latest resources
        pass


async def stream(identity):
    queue = self._get_resource_queue(identity)
    while True:
        yield await queue.get()


async def other():
    queue.put({'state': 'foo'})
    # stuff
    await asyncio.sleep(1)
    queue.put({'state': 'bar'})
