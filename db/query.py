import json, os

DATA_DIR = './data/'

def query(table, key, value):
    table_file = os.path.join(DATA_DIR, f"{table}.dat")
    results = []

    with open(table_file, 'r') as f:
        for line in f:
            row = json.loads(line)
            if row.get(key) == value:
                results.append(row)

    return results
