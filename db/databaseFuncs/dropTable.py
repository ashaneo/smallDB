import json
import os
import time
from ..logger import write_log

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("BASE_DIR")
print(BASE_DIR)

DATA_DIR = os.path.join(BASE_DIR, 'data')
SCHEMA_FILE = os.path.join(BASE_DIR, 'metadata', 'schema.json')

with open(SCHEMA_FILE) as f:
    schema = json.load(f)

def load_schema():
    with open(SCHEMA_FILE) as f:
        return json.load(f)

def save_schema(schema):
    with open(SCHEMA_FILE, 'w') as f:
        json.dump(schema, f, indent=4)
        f.flush()
        os.fsync(f.fileno())

def drop_table(table_name):
    schema = load_schema()

    # Check if table exists
    if table_name not in schema["tables"]:
        print(f"Table '{table_name}' does not exist.")
        return

    # Remove the table from schema
    del schema["tables"][table_name]
    save_schema(schema)

    # Delete the data file
    table_file_path = os.path.join(DATA_DIR, f"{table_name}.dat")
    if os.path.exists(table_file_path):
        os.remove(table_file_path)

    # Log the drop operation (with timestamp for future recovery support)
    txn_id = int(time.time() * 1000)
    write_log(f"BEGIN {txn_id}")
    write_log(f"DROP_TABLE {txn_id} {table_name}")
    write_log(f"COMMIT {txn_id}")

    print(f"Table '{table_name}' dropped successfully.")
