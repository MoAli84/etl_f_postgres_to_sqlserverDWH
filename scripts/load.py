import pyodbc
import json
import os
import pandas as pd
from sqlalchemy import text, Integer, String, Date, DECIMAL
from sqlalchemy import create_engine
import transform as transform
import extract as extract

# Load configuration from JSON file
config_path = os.path.join(os.path.dirname(__file__), "..", "config", "sqlserver_config.json")

with open(config_path, 'r') as f:
    config = json.load(f)

# Extract connection details from config
SERVER = config.get('SERVER', 'DESKTOP-GSMR3GD\\NTI')
DATABASE = config.get('DATABASE', 'ntii')
DRIVER = config.get('DRIVER', '{SQL Server}').strip('{}')  # Remove curly braces if present

# Check available drivers
import pyodbc
available_drivers = pyodbc.drivers()
print(f"Available ODBC drivers: {available_drivers}")

# Try to use ODBC Driver 17 or 18, otherwise fall back to SQL Server
if any('ODBC Driver 17' in d for d in available_drivers):
    driver_name = 'ODBC Driver 17 for SQL Server'
    print(f"✓ Using: {driver_name}")
elif any('ODBC Driver 18' in d for d in available_drivers):
    driver_name = 'ODBC Driver 18 for SQL Server'
    print(f"✓ Using: {driver_name}")
else:
    driver_name = 'SQL Server'
    print(f"⚠ Using old driver: {driver_name} (may cause issues)")
    print("  Consider installing ODBC Driver 17 from:")
    print("  https://go.microsoft.com/fwlink/?linkid=2249004")

# WORKAROUND: Use legacy_schema_aliasing to fix the NVARCHAR(max) issue
sql_engine = create_engine(
    f"mssql+pyodbc://{SERVER}/{DATABASE}?"
    f"driver={driver_name}&"
    "trusted_connection=yes",
    pool_pre_ping=True,
    echo=False,
    # CRITICAL FIX: This prevents the NVARCHAR(max) casting issue
    connect_args={
        "LongAsMax": "Yes"  # Treat LONG types as VARCHAR(max)
    },
    # Use legacy mode to avoid SQLAlchemy 2.0 CAST issues with old drivers
    use_setinputsizes=False
)

print("=" * 70)
print("Testing SQL Server Connection")
print("=" * 70)

# Extract and transform data
customer, order, product = extract.extract_data()
# FIX: Transform returns (customer, order, product) NOT (customer, product, order)
customers_df, orders_df, products_df = transform.transform_data(customer, order, product)


def load_data(customers_df, orders_df, products_df, sql_engine):
    """Load data into SQL Server with proper data type handling"""
    
    # FIX 3: Define explicit data types for each table
    # This prevents the "Invalid precision value" error
    
    customers_dtype = {
        'customer_id': Integer(),
        'city': String(100),
        'state_province': String(100),
        'country': String(100),
        'region': String(50)
    }
    
    products_dtype = {
        'product_id': String(50),
        'product_name': String(200),
        'factory': String(100),
        'division': String(100),
        'unit_price': DECIMAL(10, 2)
    }
    
    orders_dtype = {
        'transaction_id': String(50),
        'customer_id': Integer(),
        'order_id': String(50),
        'order_date': Date(),
        'product_id': String(50),
        'units': Integer(),
        'month': Integer(),
        'total_amount_by_month': Integer(),
        'total_units_by_customer': Integer(),
        'total_orders_by_product': Integer()
    }

    try:
        # WORKAROUND: Create tables manually first, then use raw pyodbc for insert
        # This bypasses SQLAlchemy's problematic table checking with old drivers
        
        print("Creating tables...")
        
        # Build raw pyodbc connection string from config
        conn_str = (
            f"DRIVER={{{driver_name}}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            "Trusted_Connection=yes;"
        )
        
        # Use raw pyodbc to create tables (avoids the NVARCHAR(max) CAST issue)
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        try:
            # Drop tables if they exist (for clean reload)
            cursor.execute("IF OBJECT_ID('fact_orders', 'U') IS NOT NULL DROP TABLE fact_orders")
            cursor.execute("IF OBJECT_ID('dim_products', 'U') IS NOT NULL DROP TABLE dim_products")
            cursor.execute("IF OBJECT_ID('dim_customers', 'U') IS NOT NULL DROP TABLE dim_customers")
            
            # Create dim_customers
            cursor.execute("""
            CREATE TABLE dim_customers (
                customer_id INT PRIMARY KEY,
                city NVARCHAR(100),
                state_province NVARCHAR(100),
                country NVARCHAR(100),
                region NVARCHAR(50)
            )
            """)
            print("✓ Created dim_customers table")

            # Create dim_products
            cursor.execute("""
            CREATE TABLE dim_products (
                product_id NVARCHAR(50) PRIMARY KEY,
                product_name NVARCHAR(200),
                factory NVARCHAR(100),
                division NVARCHAR(100),
                unit_price DECIMAL(10,2)
            )
            """)
            print("✓ Created dim_products table")

            # Create fact_orders
            cursor.execute("""
            CREATE TABLE fact_orders (
                transaction_id NVARCHAR(50) PRIMARY KEY,
                customer_id INT,
                order_id NVARCHAR(50),
                order_date DATE,
                product_id NVARCHAR(50),
                units INT,
                month INT,
                total_amount_by_month INT,
                total_units_by_customer INT,
                total_orders_by_product INT,

                CONSTRAINT fk_orders_customer
                    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),

                CONSTRAINT fk_orders_product
                    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
            )
            """)
            print("✓ Created fact_orders table")
            
            conn.commit()
            
        finally:
            cursor.close()
            conn.close()

        # Now use SQLAlchemy to insert data (tables already exist)
        print("\nLoading data...")
        
        # FIX: Remove method='multi' and use smaller chunksize to avoid COUNT field error
        customers_df[
            ['customer_id', 'city', 'state_province', 'country', 'region']
        ].to_sql(
            'dim_customers', 
            sql_engine, 
            if_exists='append',  # Tables already exist, so append
            index=False,
            dtype=customers_dtype,
            chunksize=500  # Reduced from 1000 to avoid parameter limit
        )
        print(f"✓ Loaded {len(customers_df)} customers")

        products_df[
            ['product_id', 'product_name', 'factory', 'division', 'unit_price']
        ].to_sql(
            'dim_products', 
            sql_engine, 
            if_exists='append',
            index=False,
            dtype=products_dtype,
            chunksize=500
        )
        print(f"✓ Loaded {len(products_df)} products")

        # Validate foreign keys before loading orders
        print("\nValidating foreign keys...")
        
        # Get valid customer IDs from database
        valid_customer_ids = set(
            pd.read_sql(text("SELECT customer_id FROM dim_customers"), sql_engine)['customer_id']
        )
        
        # Get valid product IDs from database
        valid_product_ids = set(
            pd.read_sql(text("SELECT product_id FROM dim_products"), sql_engine)['product_id']
        )
        
        # Check for invalid references
        invalid_customers = orders_df[~orders_df['customer_id'].isin(valid_customer_ids)]
        invalid_products = orders_df[~orders_df['product_id'].isin(valid_product_ids)]
        
        if len(invalid_customers) > 0:
            print(f"⚠ Warning: {len(invalid_customers)} orders have invalid customer_id:")
            print(invalid_customers[['transaction_id', 'customer_id']].head(10))
            print(f"  Filtering out {len(invalid_customers)} invalid orders...")
        
        if len(invalid_products) > 0:
            print(f"⚠ Warning: {len(invalid_products)} orders have invalid product_id:")
            print(invalid_products[['transaction_id', 'product_id']].head(10))
            print(f"  Filtering out {len(invalid_products)} invalid orders...")
        
        # Filter to only valid orders
        valid_orders = orders_df[
            orders_df['customer_id'].isin(valid_customer_ids) & 
            orders_df['product_id'].isin(valid_product_ids)
        ].copy()
        
        print(f"  Valid orders to load: {len(valid_orders)} / {len(orders_df)}")
        
        if len(valid_orders) == 0:
            print("❌ No valid orders to load! Check your data integrity.")
        else:
            # Load valid orders
            valid_orders[
                ['transaction_id', 'customer_id', 'order_id', 'order_date',
                 'product_id', 'units', 'month',
                 'total_amount_by_month',
                 'total_units_by_customer',
                 'total_orders_by_product']
            ].to_sql(
                'fact_orders', 
                sql_engine, 
                if_exists='append',
                index=False,
                dtype=orders_dtype,
                chunksize=500
            )
            print(f"✓ Loaded {len(valid_orders)} orders")

        print("\n" + "="*70)
        print("✅ Data loaded successfully into Data Warehouse")
        print("="*70)
        
        # Verify the data
        print("\nVerifying data...")
        with sql_engine.connect() as conn:
            customers_count = conn.execute(text("SELECT COUNT(*) FROM dim_customers")).scalar()
            products_count = conn.execute(text("SELECT COUNT(*) FROM dim_products")).scalar()
            orders_count = conn.execute(text("SELECT COUNT(*) FROM fact_orders")).scalar()
            
            print(f"  Customers: {customers_count} rows")
            print(f"  Products: {products_count} rows")
            print(f"  Orders: {orders_count} rows")
        
    except Exception as e:
        print(f"\n❌ Error loading data: {e}")
        raise


if __name__ == "__main__":
    # Debug: Print column names to see what we actually have
    print("\n" + "="*70)
    print("DEBUG: Checking DataFrame columns")
    print("="*70)
    
    print("\nCustomers DataFrame columns:")
    print(customers_df.columns.tolist())
    print(f"Shape: {customers_df.shape}")
    
    print("\nProducts DataFrame columns:")
    print(products_df.columns.tolist())
    print(f"Shape: {products_df.shape}")
    
    print("\nOrders DataFrame columns:")
    print(orders_df.columns.tolist())
    print(f"Shape: {orders_df.shape}")
    
    print("\n" + "="*70)
    
    # Now load the data with correct order
    load_data(customers_df, orders_df, products_df, sql_engine)