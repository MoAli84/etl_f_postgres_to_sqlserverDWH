import pandas as pd
import extract as extract

customer, order, product = extract.extract_data()

def transform_data(customer, order, product):
    print("Transforming data...")
    customer = customer.drop_duplicates()
    # handle missing values
    customer = customer.fillna({'city': 'Unknown', 'region': 'Unknown','country': 'Unknown','state_province': 'Unknown'})
    
    
    product = product.drop_duplicates()
    product['division'] = product['division'].replace('', pd.NA)
    product['division'].fillna(product['factory'].map(lambda x: 'Sugar' if x == 'Sugar Shack' else 'Other'),inplace=True )
    product['unit_price'].fillna(product['unit_price'].mean(), inplace=True)
    
    
    order = order.drop_duplicates()
    # handle date column
    order['order_date'] = pd.to_datetime(order['order_date'], errors='coerce')
    order['month']= order['order_date'].dt.month
    order['total_amount_by_month'] = order.groupby('month')['units'].transform('sum')
    order['total_units_by_customer'] = order.groupby('customer_id')['units'].transform('sum')
    # FIXED: Count unique orders per product instead of concatenating order_id strings
    order['total_orders_by_product'] = order.groupby('product_id')['order_id'].transform('nunique')


    return customer, order, product


transform_data(customer, order, product)