import asyncio
from pathlib import Path
import shutil
from src.quest import step, create_filesystem_historian

# try code similar to int_handle_testing, except that it just iterates which pauses inbetween, and then the user chooses when to throw the Ctrl+C interrupt. 
    # Just loop, output what iteration you're on, then sleep for 5 seconds

MAX_ITERATIONS = 10

@step
async def inner_loop_function(verb: str, iteration: int):
    msg: str = f'{verb} iteration {iteration} and sleeping for 5 seconds\n'
    print(msg)
    return msg

async def run_iterations():
    for a in range(MAX_ITERATIONS):
        await inner_loop_function("Starting", a)
        await asyncio.sleep(5)
        await inner_loop_function("Ending", a)

    print("run_iterations() completing successfully after 10 loops and waiting for an additional 5 seconds\n")
    await asyncio.sleep(5)

async def run_workflow():
    print("Running a max of 10 iterations:\n\n")

    historian = create_filesystem_historian('saved-state', 'manul-sigint-testing', run_iterations)

    task: asyncio.Task = historian.run()
    await task

    print("Workflow completed all 10 iterations successfully")

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run_iterations())
    
    finally:
        loop.stop()
        loop.close()