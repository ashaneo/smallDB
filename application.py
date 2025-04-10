from db import flush_worker  # Start background flush
from db.database import insert
from db.query import query
from db.recovery import recover

import os

# Get the current working directory
cwd = os.getcwd()


# Run recovery on startup
recover()

print("Hello")

# Insert sample data
insert('table1', {"id": 5, "name": "Eve"}, mode='fast')
insert('table2', {"product_id": 202, "price": 39.99}, mode='safe')

# Query data
print(query('table1', 'id', 5))
