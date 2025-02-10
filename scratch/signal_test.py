import signal
import threading
import time
import os

# Signal handler function
def handle_signal(signum, frame):
    print(f"[Main Thread] Received signal {signum} in thread {threading.current_thread().name}")

# Register signal handler (only works in the main thread)
signal.signal(signal.SIGINT, handle_signal)

# A worker thread function that runs a loop
def worker():
    print(f"[Worker Thread] Thread {threading.current_thread().name} running")
    time.sleep(10)
    print(f"[Worker Thread] Thread {threading.current_thread().name} exiting")

# Create and start a worker thread
worker_thread = threading.Thread(target=worker, name="WorkerThread")
worker_thread.start()

print(f"[Main Thread] Process ID: {os.getpid()}, Main thread: {threading.current_thread().name}")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("[Main Thread] KeyboardInterrupt received")

# Wait for the worker thread to finish
worker_thread.join()
print("[Main Thread] Exiting")
