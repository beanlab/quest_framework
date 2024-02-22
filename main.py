import asyncio
import json
import logging
import shutil
import uuid
from pathlib import Path
import signal

from src.quest import step, create_filesystem_historian
from src.quest.external import state, queue

logging.basicConfig(level=logging.DEBUG)
INPUT_EVENT_NAME = 'input'


@step
async def display(text: str):
    print(text)


@step
async def get_input(prompt: str):
    async with state('prompt', None, prompt), queue('input', None) as input:
        await display(prompt)
        return await input.get()


async def register_user(welcome_message):
    await display(welcome_message)
    name = await get_input('Name: ')
    sid = await get_input('Student ID: ')
    return f'Name: {name}, ID: {sid}'


async def main():
    saved_state = Path('saved-state')

    # Remove data
    # shutil.rmtree(saved_state, ignore_errors=True)

    workflow_id = str(uuid.uuid4())

    try:
        historian = create_filesystem_historian(
            saved_state, 'demo', register_user
        )

        workflow_task = historian.run('Howdy')
        await asyncio.sleep(4)

        resources = await historian.get_resources(None)
        assert resources['prompt']['value'] == 'Name: '

        await historian.record_external_event('input', None, 'put', 'Foo')
        await asyncio.sleep(0.1)
        await historian.suspend()

        workflow_task = historian.run()
        await asyncio.sleep(0.1)

        resources = await historian.get_resources(None)
        assert resources['prompt']['value'] == 'Student ID: '

        await historian.record_external_event('input', None, 'put', '123')

        assert await workflow_task == 'Name: Foo, ID: 123'

    except asyncio.CancelledError as ex:
        print("asyncio.CancelledError EXCEPTION RECEIVED");
    except KeyboardInterrupt as ex:
        print("KeyboardInterrupt EXCEPTION RECEIVED")

    finally:
        for file in sorted(saved_state.iterdir()):
            content = json.loads(file.read_text())
            print(json.dumps(content, indent=2))


if __name__ == '__main__':
    loop = asyncio.new_event_loop()

    # TODO: remove these since they aren't used
    async def shutdown_sequence(the_loop):
        print("TASKS:");
        tasks = [
            t for t in asyncio.all_tasks()
            if t is not asyncio.current_task()];

        for task in tasks:
            print(task);
            task.cancel();
    
    def handle_signal(loop, context):
        print("Custom exception handler reached.");
        print("Shutting down...");
        asyncio.create_task(shutdown_sequence(loop));

    # NOTES: 
    # look at trying to catch some exceptions in the Workflow Manager in the __aexit__ functions. The SIGINT will cause it to leave its contedxt and then __aexit__ could handle it gracefully
    # try again with the handle_step exception handling: don't record anything if it was caused by a KeyboardInterrupt (or is a KeyboardInterrupt) 

    try:
        loop.run_until_complete(main())
    finally:
        loop.stop()
        loop.close()

# TODO NEXT: the only issue is that an assertion error is being recorded. Could I was an event or context
            # variable to indicate when a KeyboardInterrupt has been thrown? Maybe I could do this in a signal
            # handler, rather than in the first place that the exception is caught. That way, I could check if 
            # the event is set before executing the code for any recording of json files. At that point, no
            # exceptions handlers would record anything because the entire program would know that Ctrl+C was
            # fired and that it wasn't an actual error.
