import os
import json
from sqlalchemy import create_engine
import pandas as pd

# Build absolute path based on script location
config_path = os.path.join(os.path.dirname(__file__), "..", "config", "postgres_config.json")

with open(config_path) as f:
    cfg = json.load(f)

try:
    pg_engine = create_engine(
        f"postgresql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    print("Connection successful!")
except Exception as e:
    raise ValueError(f"Could not connect to database: {e}")

def extract_data():
    customers = pd.read_sql("SELECT * FROM customers", pg_engine)
    orders = pd.read_sql("SELECT * FROM orders", pg_engine)
    products = pd.read_sql("SELECT * FROM products", pg_engine) 
    return customers, orders, products
