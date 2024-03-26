import subprocess

done = subprocess.run("python ./main.py")
try:
    print(done.returncode)

except subprocess.CalledProcessError as ex:
    print(ex)