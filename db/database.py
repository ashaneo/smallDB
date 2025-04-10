import json, os, time
from .logger import write_log

DATA_DIR = './data/'
SCHEMA_FILE = './metadata/schema.json'

with open(SCHEMA_FILE) as f:
    schema = json.load(f)

pending_transactions = []

def insert(table, row, mode='fast'):
    print("Insert func running")
    table_file = os.path.join(DATA_DIR, f"{table}.dat")
    txn_id = int(time.time() * 1000)

    write_log(f"BEGIN {txn_id}")
    write_log(f"INSERT {txn_id} {table} {row}")
    write_log(f"COMMIT {txn_id}")

    with open(table_file, 'a') as f:
        f.write(json.dumps(row) + "\n")

    if mode == 'safe':
        os.sync()
    else:
        pending_transactions.append(txn_id)

    print(f"Transaction {txn_id} committed ({mode}).")

def async_flush():
    print("Flushing logs asynchronously...")
    os.sync()
    pending_transactions.clear()
