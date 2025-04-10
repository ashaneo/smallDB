import json
import os
import time
from .logger import write_log

DATA_DIR = './data/'
SCHEMA_FILE = './metadata/schema.json'

with open(SCHEMA_FILE) as f:
    schema = json.load(f)


### Insert

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

#Create Table
def load_schema():
    with open(SCHEMA_FILE) as f:
        return json.load(f)

def save_schema(schema):
    with open(SCHEMA_FILE, 'w') as f:
        json.dump(schema, f, indent=4)
        f.flush()
        os.fsync(f.fileno())

def create_table(table_name, columns, primary_key):
    schema = load_schema()

    if table_name in schema["tables"]:
        print(f"Table '{table_name}' already exists. Skipping creation.")
        return  # simply return without raising exception

    schema["tables"][table_name] = {
        "columns": columns,
        "primary_key": primary_key
    }

    save_schema(schema)

    table_file_path = os.path.join(DATA_DIR, f"{table_name}.dat")
    with open(table_file_path, 'w') as table_file:
        table_file.flush()
        os.fsync(table_file.fileno())

    write_log(f"CREATE TABLE {table_name} {json.dumps(columns)} PRIMARY_KEY {primary_key}")

    print(f"Table '{table_name}' created successfully.")

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
