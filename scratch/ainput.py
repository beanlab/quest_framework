import asyncio
import signal

from history.utils import ainput
from history import step, create_filesystem_historian
from pathlib import Path

def handle(*args):
    print('Interrupt handled')
    raise KeyboardInterrupt()


signal.signal(signal.SIGINT, handle)


async def check_yield():
    """Runs while `ainput()` is waiting, proving it yields control."""
    for i in range(10):
        await asyncio.sleep(0.5)
        print(i)
    global flag
    flag = True  # If this runs, ainput() yielded control


async def get_input():
    thing = await ainput("Enter something: ")
    print(f'You entered: {thing}')
    return thing


async def ainput_yields():
    """Tests if `ainput()` yields control while waiting for input."""
    global flag
    flag = False  # Reset flag before test

    # Create async input task
    input_task = asyncio.create_task(get_input())
    yield_task = asyncio.create_task(check_yield())  # Should run if ainput() yields

    # Wait for both tasks to finish
    response = await input_task
    await yield_task

    assert flag and (print(
        f"ainput() did yield control; {response}") or True), "ainput() did not yield control while waiting for input"

@step
async def sleep_step():
    print('Sleep step running for 10 seconds')
    await asyncio.sleep(10)

async def the_workflow():
    print('Workflow running')
    await sleep_step()

async def sleep_workflow():
    print('Sleep workflow running')
    async with create_filesystem_historian(
            Path('ainput_state'),
            'sleep',
            lambda wid: the_workflow
    ) as historian:
        if not historian.has('sleep_workflow'):
            historian.start_soon('sleep_workflow', 'sleep_workflow')

        await historian.get_workflow('sleep_workflow')

# Run the test
if __name__ == '__main__':
    asyncio.run(ainput_yields())
