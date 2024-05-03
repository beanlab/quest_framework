import sys
import signal
import subprocess
import time
import platform
import random
import pytest

printing = False
out_file = open("kill-test_stdout.txt", "w")
err_file = open("kill-test_stderr.txt", "w")
wait_time = -1

def get_subprocess(is_fresh=False):
    global printing, out_file, err_file, wait_time
    out_file.close(); err_file.close()
    out_file = open("kill-test_stdout.txt", "w")
    err_file = open("kill-test_stderr.txt", "w")
    if(is_fresh): # delete json files after a full clean run
        if printing: 
            p = subprocess.Popen([sys.executable, "./main.py", "--no-destruct"], stdout=out_file, stderr=out_file)
        else: 
            p = subprocess.Popen([sys.executable, "./main.py", "--no-print", "--no-destruct"], stdout=out_file, stderr=out_file)
    else:
        if printing:
            p = subprocess.Popen([sys.executable, "./main.py", "--restart", "--no-destruct"], stdout=out_file, stderr=out_file)
        else:
            p = subprocess.Popen([sys.executable, "./main.py", "--restart", "--no-print", "--no-destruct"], stdout=out_file, stderr=out_file)
    
    if wait_time > 0:
        wait = wait_time
    else:
        wait = (random.randint(1, 501) - .9) / 100
    print(f'waiting {wait}')
    time.sleep(wait)
    return p

def check_full_run():
    print("--------Clean Test--------")
    q = get_subprocess(is_fresh=True)
    q.wait()
    print(f'return: {q.returncode}')
    assert q.returncode == 0

@pytest.mark.parametrize("args", [["--all"]])
def test_termination_response(args: list[str]):
    if "Windows" in platform.platform():
        print("This  file must be run on a non-windows platform")
        return
    
    global printing
    all = False
    if "--all" in args: all = True
    if "--print" in args: printing = True

    try:
        repetitions = 1
        if "--reps" in args:
            index = args.index("--reps") + 1
            if index in range(len(args)):
                repetitions = int(args[index])

        global wait_time
        if "--wait" in args:
            index = args.index("--wait") + 1
            if index in range(len(args)):
                wait_time = float(args[index])

        for i in range(repetitions):
            print(f'Iteration: {i + 1}')
            # TODO: convert each test to run in asyncio so that you can run multiple subprocesses at once

            if all or "--term" in args:
                print("--------Termination Test--------")
                p = get_subprocess()
                p.terminate()
                p.wait()
                print(p.returncode)

                check_full_run()

            if all or "--kill" in args:
                print("--------Kill Test--------")
                r = get_subprocess()
                r.kill()
                r.wait()
                print(r.returncode)

                check_full_run()

            if all or "--int" in args:
                print("--------SIGINT Test--------")
                p = get_subprocess()
                p.send_signal(signal.SIGINT)
                p.wait()

                check_full_run()

            if all or "--abrt" in args:
                print("--------SIGABRT Test--------")
                p = get_subprocess()
                p.send_signal(signal.SIGABRT)
                p.wait()

                check_full_run()

        print(f'{__file__} has completed its tests successfully')

    except AssertionError as ex:
        print(f'{__file__} received the following assertion error during execution:\n')
        raise

if __name__ == "__main__":
    test_termination_response(sys.argv)