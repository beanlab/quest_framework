import asyncio
from demos.flexible_multi_guess import run_flexible_multi_guess
import sys

if __name__ == '__main__':
    print("\nRunning flexible_multi_guess.py\n")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    args = sys.argv
    if len(args) < 2:
        args = []
    else:
        args = args[1:]
    try:
        result = loop.run_until_complete(run_flexible_multi_guess(list(args)))
        print(f"Result returned by the program was:\n{result}")
    except Exception as ex:
        print("\n flexible_multi_guess.py exiting with an exception\n")
        print(ex)
    finally:
        loop.close()