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