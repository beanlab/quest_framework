import sys
import signal
import subprocess
import time
import platform
import random

def get_subprocess(is_fresh=False, skip_own_gpid=False):
    if(is_fresh): # delete json files after a full clean run
        p = subprocess.Popen([sys.executable, "./main.py", "--no-delete", "--no-print"])
    elif skip_own_gpid:
        subprocess.Popen([sys.executable, "./main.py", "--no-print"])
    else:
        p = subprocess.Popen([sys.executable, "./main.py", "--no-print"])
    time.sleep(random.randint(1, 8) % 4)
    return p

def check_full_run():
    print("--------Clean Test--------")
    q = get_subprocess(is_fresh=True)
    q.wait()
    print(q.returncode)
    # assert q.returncode == 0

def run_test(args: list[str]):
    if "Windows" in platform.platform():
        print("This  file must be run on a non-windows platform")
        return
    
    all = False
    if "--all" in args: all = True

    if all or "--term" in args:
        print("--------Termination Test--------")
        p = get_subprocess()
        p.terminate()
        p.wait()
        print(p.returncode)
        # assert p.returncode == 1

        check_full_run()

    if all or "--kill" in args:
        print("--------Kill Test--------")
        r = get_subprocess()
        r.kill()
        r.wait()
        print(r.returncode)
        # assert r.returncode == 1    

        check_full_run()

    if all or "--int" in args:
        print("--------SIGINT Test--------")
        p = get_subprocess()
        p.send_signal(signal.SIGINT)
        p.wait()
        # assert p.returncode == 1

        check_full_run()

    if all or "--abrt" in args:
        print("--------SIGABRT Test--------")
        p = get_subprocess()
        p.send_signal(signal.SIGABRT)
        p.wait()
        # assert p.returncode == 1

        check_full_run()

if __name__ == "__main__":
    run_test(sys.argv)