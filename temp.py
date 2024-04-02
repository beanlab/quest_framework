import os
import signal
import subprocess
import time
import platform

def get_subprocess(is_fresh=False, skip_own_gpid=False):
    if(is_fresh): # delete json files after a full clean run
        p = subprocess.Popen("python ./main.py --no-delete")
    elif skip_own_gpid:
        p = subprocess.Popen("python ./main.py")
    else:
        p = subprocess.Popen("python ./main.py", creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    time.sleep(1.5)
    return p

def check_fresh_run():
    print("--------Clean Test--------")
    q = get_subprocess(is_fresh=True)
    q.wait()
    print(q.returncode)
    assert q.returncode == 0

def run_test():
    print("--------Termination Test--------")
    p = get_subprocess()
    p.terminate()
    p.wait()
    assert p.returncode == 1

    check_fresh_run()

    print("--------Kill Test--------")
    r = get_subprocess()
    r.kill()
    r.wait()
    print(r.returncode)
    assert r.returncode == 1    

    check_fresh_run()

    if "Windows" in platform.platform():
        print("--------CTRL_BREAK_EVENT Test--------")
        s = get_subprocess()
        os.kill(s.pid, signal.CTRL_BREAK_EVENT)
        s.wait()
        print(f'return code: {s.returncode}')
        # assert s.returncode == 1
        
        check_fresh_run()

        print("--------CTRL_C_EVENT Test--------")
        v = get_subprocess(skip_own_gpid=True)
        v.send_signal(signal.CTRL_C_EVENT)
        try:
            v.wait()
        except KeyboardInterrupt:
            pass
        print(f'return code: {v.returncode}')
        assert v.returncode == 1

        # check_fresh_run()

    else:
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