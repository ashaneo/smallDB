import json
import os
import time
from .logger import write_log

DATA_DIR = './data/'
SCHEMA_FILE = './metadata/schema.json'

with open(SCHEMA_FILE) as f:
    schema = json.load(f)

def insert(table, row):
    if table not in schema["tables"]:
        raise ValueError("Table not found in schema")

    table_file = os.path.join(DATA_DIR, f"{table}.dat")
    txn_id = int(time.time() * 1000)

    # Write-Ahead Logging (Immediate durability)
    write_log(f"BEGIN {txn_id}")
    write_log(f"INSERT {txn_id} {table} {row}")
    write_log(f"COMMIT {txn_id}")

    # Immediately persist data (no eventual durability)
    with open(table_file, 'a') as f:
        f.write(json.dumps(row) + "\n")
        f.flush()
        os.fsync(f.fileno())  # ensure durability immediately

    print(f"Transaction {txn_id} committed and persisted.")
