from db.database import *
from db.databaseFuncs.insert import insert
from db.query import query
from db.recovery import recover

# Perform recovery first on startup
recover()

# Insert sample data
# insert('table1', {"id": 1, "name": "Alice"})
# insert('table2', {"product_id": 100, "price": 19.99})

# Creating a new table dynamically
# create_table('table5', ["student_id", "index", "name", "age"], "student_id")

# Drop an existing table
drop_table('table3')

insert('table1', {"id": 200, "name": "Ashan"})

from db.query import query

# # Simple WHERE clause
# print(query("table1", conditions=[("id", "==", 1)]))

# # WHERE with multiple conditions
# print(query("table1", conditions=[("age", ">", 18), ("name", "!=", "Bob")]))

# # Only return selected fields (project)
# print(query("table1", conditions=[], select_all=False, fields=["name", "age"]))

# # With sorting and pagination
# print(query("table1", conditions=[("age", ">", 18)], order_by="age", limit=3, offset=1))
