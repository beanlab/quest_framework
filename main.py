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
        print("EXCEPTION RECEIVED");
        # close the loop or something? 

    # finally:
    #     for file in sorted(saved_state.iterdir()):
    #         content = json.loads(file.read_text())
    #         print(json.dumps(content, indent=2))


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    # signals = (signal.SIGINT, signal.SIGTERM) # do we also want signal.SIGHUP ?

    def shutdown_sequence(the_loop, context):
        print("Custom exception handler reached.");
        print("TASKS:");
        tasks = [
            t for t in asyncio.all_tasks()
            if t is not asyncio.current_task()];

        for task in tasks:
            print(task);
            task.cancel();
    
        # raise Exception("Coming from SIGINT handler");
        
        # await asyncio.gather(*tasks, return_exceptions=True);
        # asyncio.get_running_loop().stop();
    

    # loop.set_exception_handler(shutdown_sequence);
    # loop.add_signal_handler(signal.CTRL_C_EVENT, shutdown_sequence);
    signal.signal(signal.SIGINT, shutdown_sequence);

    try:
        loop.run_until_complete(main())
    finally:
        loop.stop()
        loop.close()
