import json
import os

LOG_FILE = './logs/write_ahead_log.log'
DATA_DIR = './data/'

def recover():
    print("Starting recovery...")
    committed = set()

    # Step 1: Identify committed transaction IDs
    with open(LOG_FILE, 'r') as log:
        for line in log:
            _, entry = line.strip().split(": ", 1)
            parts = entry.split()
            if parts[0] == "COMMIT":
                committed.add(parts[1])

    applied_transactions = set()

    # Step 2: Replay insert entries of committed transactions
    with open(LOG_FILE, 'r') as log:
        for line in log:
            _, entry = line.strip().split(": ", 1)
            parts = entry.split(maxsplit=3)  # critical fix here
            if parts[0] == "INSERT":
                txn_id = parts[1]
                table = parts[2]
                row_json = parts[3]  # JSON might contain spaces

                if txn_id in committed and txn_id not in applied_transactions:
                    table_file = os.path.join(DATA_DIR, f"{table}.dat")

                    # Correctly convert string to JSON
                    row_data = json.loads(row_json.replace("'", "\""))

                    with open(table_file, 'a') as f:
                        f.write(json.dumps(row_data) + "\n")
                        f.flush()
                        os.fsync(f.fileno())

                    applied_transactions.add(txn_id)

    print("Recovery completed successfully.")
