import signal
import subprocess
import time
import platform

def get_subprocess(is_fresh=False):
    if(is_fresh): # delete json files after a full clean run
        p = subprocess.Popen("python ./main.py --no-delete")
    else:
        p = subprocess.Popen("python ./main.py")
    time.sleep(1.5)
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

    print("--------CTRL_C_EVENT Test--------")
    p = get_subprocess()
    p.send_signal(signal.CTRL_C_EVENT)
    try: # parent process also receives the CTL_C_EVENT, so we need to swallow the exception
        p.wait()
    except KeyboardInterrupt:
        pass
    print(p.returncode)
    assert p.returncode == 1

    return

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