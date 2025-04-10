import json
import os
import operator

DATA_DIR = './data/'

# Mapping of string operators to actual Python operators
OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge
}

def query(table, conditions=[], order_by=None, limit=None, offset=0, select_all=True, fields=None):
    """
    Query rows from a table based on filters and options.

    Parameters:
    - table (str): Table name (must match .dat file)
    - conditions (list of tuples): e.g., [("age", ">", 18), ("name", "==", "Alice")]
    - order_by (str): Optional field to sort results by
    - limit (int): Max number of results to return
    - offset (int): Number of rows to skip before returning
    - select_all (bool): If False, only return selected fields
    - fields (list of str): Fields to return if select_all is False

    Returns:
    - list of matching rows (as dictionaries)
    """
    table_file = os.path.join(DATA_DIR, f"{table}.dat")
    results = []

    # Check if the table file exists
    if not os.path.exists(table_file):
        print(f"Table '{table}' does not exist.")
        return results

    # Read the table line by line
    with open(table_file, 'r') as f:
        for line in f:
            row = json.loads(line.strip())
            match = True

            # Apply conditions
            for key, op, value in conditions:
                if key not in row or op not in OPS or not OPS[op](row[key], value):
                    match = False
                    break

            if match:
                results.append(row)

    # Apply sorting if needed
    if order_by:
        results.sort(key=lambda x: x.get(order_by))

    # Apply projection (select specific fields)
    if not select_all and fields:
        results = [{k: row[k] for k in fields if k in row} for row in results]

    # Apply limit and offset
    if limit is not None:
        results = results[offset:offset + limit]
    else:
        results = results[offset:]

    return results
