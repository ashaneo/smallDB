import threading, time
from .database import async_flush, pending_transactions
import os

def flush_periodically(interval=5):
    while True:
        if pending_transactions:
            async_flush()
        time.sleep(interval)

flush_thread = threading.Thread(target=flush_periodically, args=(5,), daemon=True)
flush_thread.start()

def async_flush():
    with open('./logs/write_ahead_log.log', 'a') as f:
        f.flush()
        os.fsync(f.fileno())  # flush WAL

    # You could also flush .dat files here if needed
    pending_transactions.clear()
    print("[async_flush] Flushed to disk.")
