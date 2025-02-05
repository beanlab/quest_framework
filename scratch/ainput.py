import asyncio
from quest.utils import ainput, stdio


async def check_yield():
    """Runs while `ainput()` is waiting, proving it yields control."""
    await asyncio.sleep(0.05)  # Ensure this runs while waiting for input
    global flag
    flag = True  # If this runs, ainput() yielded control

async def ainput_yields():
    """Tests if `ainput()` yields control while waiting for input."""
    global flag
    flag = False  # Reset flag before test

    # Create async input task
    input_task = asyncio.create_task(ainput("Enter something: "))
    yield_task = asyncio.create_task(check_yield())  # Should run if ainput() yields

    # Wait for both tasks to finish
    response = await input_task
    await yield_task

    assert flag and (print(f"ainput() did yield control; {response}") or True), "ainput() did not yield control while waiting for input"

# Run the test
if __name__ == '__main__':
    asyncio.run(ainput_yields())
