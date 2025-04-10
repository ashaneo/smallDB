import time
import os

LOG_FILE = './logs/write_ahead_log.log'

def write_log(entry):
    timestamp = time.time()
    with open(LOG_FILE, 'a') as log:
        log.write(f"{timestamp}: {entry}\n")
        log.flush()
        os.fsync(log.fileno())  # immediate durability
