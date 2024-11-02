import os
import sys
import signal
import subprocess
import time
import platform
import random
import pytest

# TODO: model this file more like test_kill_unix.py

printing = False

def get_subprocess(is_fresh=False, skip_own_gpid=False):
    global printing
    flags = ""
    if not is_fresh: flags = "--restart"
    if not printing: flags = flags + " --no-print"

    if(is_fresh): # delete json files after a full clean run
        p = subprocess.Popen(f"python ./main.py {flags}")
    elif skip_own_gpid:
        p = subprocess.Popen(f"python ./main.py {flags}")
    else:
        p = subprocess.Popen(f"python ./main.py {flags}", creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    wait = random.randint(1, 6) / 4
    print(f'waiting {wait}')
    time.sleep(wait)
    return p

def check_fresh_run():
    print("--------Clean Test--------")
    q = get_subprocess(is_fresh=True)
    q.wait()
    print(q.returncode)
    assert (q.returncode == 0 or q.returncode == 1)

@pytest.mark.parametrize("args", [["--all"]])
def test_termination_response(args: list[str]):
    if "Windows" not in platform.platform():
        print("This test is meant for a Windows operating system.")
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
        assert p.returncode == 1

        check_fresh_run()

    # note that there is no p.kill() test because kill() wraps terminate() in Windows

    if all or "--break" in args:
        print("--------CTRL_BREAK_EVENT Test--------")
        s = get_subprocess()
        os.kill(s.pid, signal.CTRL_BREAK_EVENT)
        s.wait()
        print(f'return code: {s.returncode}')
        assert s.returncode != 0

        check_fresh_run()

    if all or "--int" in args:
        print("--------CTRL_C_EVENT Test--------")
        v = get_subprocess(skip_own_gpid=True)
        v.send_signal(signal.CTRL_C_EVENT)
        try:
            v.wait()
        except KeyboardInterrupt:
            pass
        print(f'return code: {v.returncode}')
        assert s.returncode != 0

        check_fresh_run()

if __name__ == '__main__':
    test_termination_response(sys.argv)