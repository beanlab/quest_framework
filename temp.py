import signal
import subprocess
import time

def run_test():
    p = subprocess.Popen("python ./main.py") 
    time.sleep(1)
    p.terminate()
    p.wait()
    print(p.returncode)

    p = subprocess.Popen("python ./main.py") 
    time.sleep(1)
    p.kill()
    p.wait()
    print(p.returncode)

run_test()