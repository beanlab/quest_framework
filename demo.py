import asyncio
from demos.flexible_multi_guess import run_flexible_multi_guess
from main_old import main

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("\nRunning main_old.py\n")
    try:
        result = loop.run_until_complete(main())
    except Exception as ex:
        print("\nmain_old.py exiting with an exception\n")
    finally:
        loop.close()

    print("\nRunning flexible_multi_guess.py\n")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        result = loop.run_until_complete(run_flexible_multi_guess())
        print(f"Result returned by the program was:\n{result}")
    except Exception as ex:
        print("\n flexible_multi_guess.py exiting with an exception\n")
    finally:
        loop.close()