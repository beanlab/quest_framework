import os
import sys
import signal
import subprocess
import time
import platform

def get_subprocess(is_fresh=False, skip_own_gpid=False):
    if(is_fresh): # delete json files after a full clean run
        p = subprocess.Popen([sys.executable, "./main.py", "--no-delete"])
    elif skip_own_gpid:
        subprocess.Popen([sys.executable, "./main.py"])
    else:
        p = subprocess.Popen([sys.executable, "./main.py"])
    time.sleep(1.5)
    return p

def check_full_run():
    print("--------Clean Test--------")
    q = get_subprocess(is_fresh=True)
    q.wait()
    print(q.returncode)
    # assert q.returncode == 0

def run_test():
    print("--------Termination Test--------")
    p = get_subprocess()
    p.terminate()
    p.wait()
    print(p.returncode)
    # assert p.returncode == 1

    check_full_run()

    print("--------Kill Test--------")
    r = get_subprocess()
    r.kill()
    r.wait()
    print(r.returncode)
    # assert r.returncode == 1    

    check_full_run()

    print("--------SIGINT Test--------")
    p = get_subprocess()
    p.send_signal(signal.SIGINT)
    p.wait()
    # assert p.returncode == 1

    check_full_run()

    print("--------SIGABRT Test--------")
    p = get_subprocess()
    p.send_signal(signal.SIGABRT)
    p.wait()
    # assert p.returncode == 1

    check_full_run()

run_test()