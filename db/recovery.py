import json
import os

LOG_FILE = './logs/write_ahead_log.log'
DATA_DIR = './data/'
SCHEMA_FILE = './metadata/schema.json'

def load_schema():
    with open(SCHEMA_FILE) as f:
        return json.load(f)

def save_schema(schema):
    with open(SCHEMA_FILE, 'w') as f:
        json.dump(schema, f, indent=4)
        f.flush()
        os.fsync(f.fileno())

def recover():
    # Ensure the database is consistent after a crash or restart.
    # It reads the Write-Ahead Log (WAL), finds committed transactions, and replays only the safe operations.
    print("Starting recovery...")

    committed = set()  # Will store all committed transaction IDs

    # === STEP 1: Identify committed transactions ===
    with open(LOG_FILE, 'r') as log:
        for line in log:
            _, entry = line.strip().split(": ", 1)  # Split timestamp and WAL entry
            # Example line: "1712748670.123456: COMMIT 1234567890000"
            parts = entry.split()  # Split entry like: ["COMMIT", "1234567890000"]
            if parts[0] == "COMMIT":
                committed.add(parts[1])  # Store transaction ID

    print("=== Committed Transactions ===")
    print(committed)
    print("=== End of Committed ===")

    applied_transactions = set()  # Track what we've already replayed
    schema = load_schema()  # Load current schema into memory

    # === STEP 2: Replay CREATE_TABLE and INSERT for committed transactions ===
    with open(LOG_FILE, 'r') as log:
        for line in log:
            _, entry = line.strip().split(": ", 1)  # Separate timestamp from WAL entry
            parts = entry.split(maxsplit=4)  # Allow up to 5 pieces: operation, txn_id, table_name, json, maybe primary_key

            operation = parts[0]

            # === Replay CREATE_TABLE ===
            if operation == "CREATE_TABLE":
                txn_id = parts[1]
                table_name = parts[2]
                columns = json.loads(parts[3])  # Columns list is stored as JSON
                primary_key = parts[4]

                if txn_id in committed and txn_id not in applied_transactions:
                    if table_name not in schema["tables"]:
                        print(f"Recovering table '{table_name}'...")
                        schema["tables"][table_name] = {
                            "columns": columns,
                            "primary_key": primary_key
                        }
                        save_schema(schema)

                        # Create empty .dat file for the table
                        table_file_path = os.path.join(DATA_DIR, f"{table_name}.dat")
                        with open(table_file_path, 'w') as table_file:
                            table_file.flush()
                            os.fsync(table_file.fileno())

                    applied_transactions.add(txn_id)

            # === Replay INSERT ===
            elif operation == "INSERT":
                txn_id = parts[1]
                table = parts[2]
                row_json = parts[3]  # JSON string might have spaces, hence split(maxsplit=3/4) earlier

                if txn_id in committed and txn_id not in applied_transactions:
                    table_file = os.path.join(DATA_DIR, f"{table}.dat")
                    print("=== Table File ===")
                    print(table_file)
                    print("=== End of Table File ===")

                    # Convert string JSON to Python dict, ensuring double quotes
                    # Try to parse the JSON safely

                    print(f"Processing INSERT log: txn={txn_id}, table={table}, row={row_json}")

                    try:
                        # First, fix any legacy formatting issues (single quotes)
                        row_json_clean = row_json.strip()

                        # Optionally handle Python-style dicts (single quotes)
                        if row_json_clean.startswith("{") and "'" in row_json_clean and '"' not in row_json_clean:
                            row_json_clean = row_json_clean.replace("'", "\"")

                        row_data = json.loads(row_json_clean)

                    except json.JSONDecodeError as e:
                        print("⚠️ JSON parsing error during recovery!")
                        print(f"Offending JSON string: {row_json}")
                        print(f"Error: {e}")
                        continue  # skip this bad entry and move on


                    # Append row to table file
                    with open(table_file, 'a') as f:
                        f.write(json.dumps(row_data) + "\n")
                        f.flush()
                        os.fsync(f.fileno())

                    applied_transactions.add(txn_id)
                
                # === Replay DROP_TABLE ===
                elif operation == "DROP_TABLE":
                    txn_id = parts[1]
                    table_name = parts[2]

                    if txn_id in committed and txn_id not in applied_transactions:
                        print(f"Recovering drop for table '{table_name}'...")

                        # Remove from schema if exists
                        if table_name in schema["tables"]:
                            del schema["tables"][table_name]
                            save_schema(schema)

                        # Remove the .dat file if it exists
                        table_file = os.path.join(DATA_DIR, f"{table_name}.dat")
                        if os.path.exists(table_file):
                            os.remove(table_file)

                        applied_transactions.add(txn_id)


    print("=== Applied Transactions ===")
    print(applied_transactions)
    print("=== End of Applied ===")

    print("Recovery completed successfully.")
