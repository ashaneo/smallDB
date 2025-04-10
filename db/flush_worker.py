import threading, time
from .database import async_flush, pending_transactions

def flush_periodically(interval=5):
    while True:
        if pending_transactions:
            async_flush()
        time.sleep(interval)

flush_thread = threading.Thread(target=flush_periodically, args=(5,), daemon=True)
flush_thread.start()
