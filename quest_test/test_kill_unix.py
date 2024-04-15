import sys
import signal
import subprocess
import time
import platform
import random
import pytest

printing = False

def get_subprocess(is_fresh=False):
    global printing
    if(is_fresh): # delete json files after a full clean run
        if printing: 
            p = subprocess.Popen([sys.executable, "./main.py"])
        else: 
            p = subprocess.Popen([sys.executable, "./main.py", "--no-print"])
    else:
        if printing:
            p = subprocess.Popen([sys.executable, "./main.py", "--restart"])
        else:
            p = subprocess.Popen([sys.executable, "./main.py", "--restart", "--no-print"])
    wait = random.randint(1, 6) / 4
    print(f'waiting {wait}')
    time.sleep(wait)
    return p

def check_full_run():
    print("--------Clean Test--------")
    q = get_subprocess(is_fresh=True)
    q.wait()
    print(q.returncode)
    assert (q.returncode == 0 or q.returncode == 1)

@pytest.mark.parametrize("args", [["--all"]])
def test_termination_response(args: list[str]):
    if "Windows" in platform.platform():
        print("This  file must be run on a non-windows platform")
        return
    
    global printing
    all = False
    if "--all" in args: all = True
    if "--print" in args: printing = True

    if all or "--term" in args:
        print("--------Termination Test--------")
        p = get_subprocess()
        p.terminate()
        p.wait()
        print(p.returncode)
        assert p.returncode != 0

        check_full_run()

    if all or "--kill" in args:
        print("--------Kill Test--------")
        r = get_subprocess()
        r.kill()
        r.wait()
        print(r.returncode)
        assert p.returncode != 0

        check_full_run()

    if all or "--int" in args:
        print("--------SIGINT Test--------")
        p = get_subprocess()
        p.send_signal(signal.SIGINT)
        p.wait()
        assert p.returncode != 0

        check_full_run()

    if all or "--abrt" in args:
        print("--------SIGABRT Test--------")
        p = get_subprocess()
        p.send_signal(signal.SIGABRT)
        p.wait()
        assert p.returncode != 0

        check_full_run()

if __name__ == "__main__":
    test_termination_response(sys.argv)