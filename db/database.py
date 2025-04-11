import json
import os
import time
from .logger import write_log

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.databaseFuncs.insert import insert
from db.databaseFuncs.dropTable import drop_table
from db.databaseFuncs.createTable import create_table

DATA_DIR = './data/'
SCHEMA_FILE = './metadata/schema.json'

with open(SCHEMA_FILE) as f:
    schema = json.load(f)

insert
drop_table
create_table
