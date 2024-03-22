import asyncio
from pathlib import Path
import shutil
from src.quest import step, create_filesystem_historian
import random
import sys

# this code is intended to run in such a way that it is easy for the developer to interrupt the program with Ctrl+C and observe how it resumes

MAX_ITERATIONS = 10
random_breakpoint = 0

@step
async def enter_loop(iteration: int):
    if iteration == random_breakpoint:
        raise KeyboardInterrupt
    await asyncio.sleep(0.5)
    msg: str = f'Starting iteration {iteration} and sleeping for 1 seconds'
    return msg

@step
async def exit_loop(iteration: int):
    await asyncio.sleep(1)
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
    print("Running a max of 10 iterations:\n")

    historian = create_filesystem_historian(Path('saved-state'), 'manual-sigint-testing', run_iterations)

    task: asyncio.Task = historian.run()
    await task

    print("Workflow completed all 10 iterations successfully")

if __name__ == '__main__':
    print("\nSpecify \"-r\" in the argument to clear the history first")

    args = sys.argv
    if len(args) > 1 and args[1] == "-r":
        shutil.rmtree("saved-state", ignore_errors=True)
        print("json files removed.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    random_breakpoint = random.randint(0, 10) # selecting 10 means no interrup will be raised
    print(f"Running with random_breakpont set to {random_breakpoint}")

    try:
        loop.run_until_complete(run_workflow())
    
    finally:
        loop.stop()
        loop.close()