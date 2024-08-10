import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sales.csv', 'sales'),
    ('products.csv', 'products'),
    ('delivery.csv', 'delivery'),
    ('payments.csv', 'payments')  # Added payments.csv for specific handling
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='your_host',
    user='your_username',
    password='your_password',
    database='your_database'
)

# Folder containing the CSV files
folder_path = 'path_to_your_folder'

def process_csv(file_path, table_name):
    df = pd.read_csv(file_path)

    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Use DataFrame.to_sql() method to insert data
    df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    process_csv(file_path, table_name)

# Close the connection
conn.close()
