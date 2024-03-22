import asyncio
from pathlib import Path
import shutil
from src.quest import step, create_filesystem_historian

# try code similar to int_handle_testing, except that it just iterates which pauses inbetween, and then the user chooses when to throw the Ctrl+C interrupt. 
    # Just loop, output what iteration you're on, then sleep for 5 seconds

MAX_ITERATIONS = 10

@step
async def enter_loop(iteration: int):
    await asyncio.sleep(0.5)
    msg: str = f'Starting iteration {iteration} and sleeping for 3 seconds'
    return msg

@step
async def exit_loop(iteration: int):
    await asyncio.sleep(3)
    msg: str = f'Ending iteration {iteration} and sleeping for .5 seconds\n'
    return msg

async def run_iterations():
    for a in range(MAX_ITERATIONS):
        enter = await enter_loop(a)
        print(enter)
        exit = await exit_loop(a)
        print(exit)

    print("run_iterations() completing successfully after 10 loops and waiting for an additional 5 seconds\n")
    await asyncio.sleep(5)

async def run_workflow():
    print("Running a max of 10 iterations:\n\n")

    historian = create_filesystem_historian(Path('saved-state'), 'manual-sigint-testing', run_iterations)

    task: asyncio.Task = historian.run()
    await task

    print("Workflow completed all 10 iterations successfully")

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run_workflow())
    
    finally:
        loop.stop()
        loop.close()