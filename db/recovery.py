import json, os

LOG_FILE = './logs/write_ahead_log.log'
DATA_DIR = './data/'

def recover():
    print("Starting Recovery...")
    committed = set()

    with open(LOG_FILE, 'r') as log:
        for line in log:
            _, entry = line.strip().split(": ", 1)
            parts = entry.split()

            if parts[0] == "COMMIT":
                committed.add(parts[1])

    with open(LOG_FILE, 'r') as log:
        for line in log:
            _, entry = line.strip().split(": ", 1)
            parts = entry.split()

            if parts[0] == "INSERT" and parts[1] in committed:
                _, _, table, row_json = parts
                table_file = os.path.join(DATA_DIR, f"{table}.dat")
                with open(table_file, 'a') as f:
                    f.write(row_json + "\n")

    print("Recovery complete.")
