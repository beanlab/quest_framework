import signal
import subprocess
import time
import platform

def get_subprocess(is_fresh=False):
    if(is_fresh): # delete json files on first run of main.py
        p = subprocess.Popen("python ./main.py --no-delete")
    else:
        p = subprocess.Popen("python ./main.py")
    time.sleep(1)
    return p

def check_fresh_run():
    print("--------Clean Test--------")
    p = get_subprocess(is_fresh=True)
    p.wait()
    print(p.returncode)
    assert p.returncode == 0

def run_test():
    print("--------Termination Test--------")
    p = get_subprocess()
    p.terminate()
    p.wait()
    assert p.returncode == 1

    check_fresh_run()

    print("--------Kill Test--------")
    p = get_subprocess()
    p.kill()
    p.wait()
    print(p.returncode)
    assert p.returncode == 1

    check_fresh_run()

    if "Windows" in platform.platform():
        return
    
    print("--------SIGINT Test--------")
    p = get_subprocess()
    p.send_signal(signal.SIGINT)
    p.wait()
    assert p.returncode == 1

    check_fresh_run()

    print("--------SIGABRT Test--------")
    p = get_subprocess()
    p.send_signal(signal.SIGABRT)
    p.wait()
    assert p.returncode == 1

    check_fresh_run()

run_test()