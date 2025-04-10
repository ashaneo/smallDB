import time
import os

# Get the current working directory
cwd = os.getcwd()


LOG_FILE = './logs/write_ahead_log.log'

def write_log(entry):
    timestamp = time.time()
    with open(LOG_FILE, 'a') as log:
        log.write(f"{timestamp}: {entry}\n")
        log.flush()
