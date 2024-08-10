import sqlite3
import pandas as pd

def create_tables(cursor):
    """Create the necessary tables in the SQLite database."""
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT,
        city TEXT,
        state TEXT
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        order_date TEXT,
        amount REAL,
        payment_type TEXT,
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        category TEXT,
        price REAL
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS order_products (
        order_id INTEGER,
        product_id INTEGER,
        FOREIGN KEY (order_id) REFERENCES orders (order_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        seller_id INTEGER,
        amount REAL
    )
    ''')

def load_csv_to_table(file_path, table_name, conn):
    """Load CSV data into a specified SQLite table."""
    df = pd.read_csv(file_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)

def basic_queries(conn):
    """Execute basic queries and print results."""
    print("1. List all unique cities where customers are located:")
    query = 'SELECT DISTINCT city FROM customers'
    unique_cities = pd.read_sql(query, conn)
    print(unique_cities)

    print("\n2. Count the number of orders placed in 2017:")
    query = 'SELECT COUNT(*) AS order_count FROM orders WHERE strftime("%Y", order_date) = "2017"'
    orders_2017 = pd.read_sql(query, conn)
    print(orders_2017)

    print("\n3. Find the total sales per category:")
    query = '''
    SELECT p.category, SUM(o.amount) AS total_sales
    FROM orders o
    JOIN order_products op ON o.order_id = op.order_id
    JOIN products p ON op.product_id = p.product_id
    GROUP BY p.category
    '''
    sales_per_category = pd.read_sql(query, conn)
    print(sales_per_category)

    print("\n4. Calculate the percentage of orders that were paid in installments:")
    query = '''
    SELECT
        (SUM(CASE WHEN payment_type = 'Installment' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_installments
    FROM orders
    '''
    installments_percentage = pd.read_sql(query, conn)
    print(installments_percentage)

    print("\n5. Count the number of customers from each state:")
    query = 'SELECT state, COUNT(*) AS customer_count FROM customers GROUP BY state'
    customers_by_state = pd.read_sql(query, conn)
    print(customers_by_state)

def intermediate_queries(conn):
    """Execute intermediate queries and print results."""
    print("\n1. Calculate the number of orders per month in 2018:")
    query = '''
    SELECT strftime("%m", order_date) AS month, COUNT(*) AS order_count
    FROM orders
    WHERE strftime("%Y", order_date) = "2018"
    GROUP BY month
    '''
    orders_per_month_2018 = pd.read_sql(query, conn)
    print(orders_per_month_2018)

    print("\n2. Find the average number of products per order, grouped by customer city:")
    query = '''
    WITH order_counts AS (
        SELECT o.order_id, c.city, COUNT(op.product_id) AS product_count
        FROM orders o
        JOIN order_products op ON o.order_id = op.order_id
        JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY o.order_id, c.city
    )
    SELECT city, AVG(product_count) AS avg_products_per_order
    FROM order_counts
    GROUP BY city
    '''
    avg_products_per_order_city = pd.read_sql(query, conn)
    print(avg_products_per_order_city)

    print("\n3. Calculate the percentage of total revenue contributed by each product category:")
    query = '''
    WITH total_revenue AS (
        SELECT SUM(amount) AS total FROM orders
    ),
    category_revenue AS (
        SELECT p.category, SUM(o.amount) AS category_total
        FROM orders o
        JOIN order_products op ON o.order_id = op.order_id
        JOIN products p ON op.product_id = p.product_id
        GROUP BY p.category
    )
    SELECT
        category,
        (category_total / (SELECT total FROM total_revenue)) * 100 AS percentage_revenue
    FROM category_revenue
    '''
    percentage_revenue_per_category = pd.read_sql(query, conn)
    print(percentage_revenue_per_category)

    print("\n4. Identify the correlation between product price and the number of times a product has been purchased:")
    query = '''
    WITH product_sales AS (
        SELECT p.product_id, p.price, COUNT(op.order_id) AS purchase_count
        FROM products p
        JOIN order_products op ON p.product_id = op.product_id
        GROUP BY p.product_id, p.price
    )
    SELECT
        (SELECT SUM((price - (SELECT AVG(price) FROM product_sales)) * (purchase_count - (SELECT AVG(purchase_count) FROM product_sales)))
         / (SQRT(SUM((price - (SELECT AVG(price) FROM product_sales)) * (price - (SELECT AVG(price) FROM product_sales))))
            * SQRT(SUM((purchase_count - (SELECT AVG(purchase_count) FROM product_sales)) * (purchase_count - (SELECT AVG(purchase_count) FROM product_sales))))) AS correlation
    FROM product_sales
    '''
    correlation = pd.read_sql(query, conn)
    print(correlation)

    print("\n5. Calculate the total revenue generated by each seller, and rank them by revenue:")
    query = '''
    SELECT seller_id, SUM(amount) AS total_revenue
    FROM sales
    GROUP BY seller_id
    ORDER BY total_revenue DESC
    '''
    revenue_per_seller = pd.read_sql(query, conn)
    print(revenue_per_seller)

def advanced_queries(conn):
    """Execute advanced queries and print results."""
    print("\n1. Calculate the moving average of order values for each customer over their order history:")
    query = '''
    WITH ranked_orders AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS row_num
        FROM orders
    )
    SELECT customer_id, order_date, amount,
           AVG(amount) OVER (PARTITION BY customer_id ORDER BY row_num ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
    FROM ranked_orders
    '''
    moving_avg = pd.read_sql(query, conn)
    print(moving_avg)

    print("\n2. Calculate the cumulative sales per month for each year:")
    query = '''
    WITH monthly_sales AS (
        SELECT strftime("%Y-%m", order_date) AS year_month, SUM(amount) AS monthly_sales
        FROM orders
        GROUP BY year_month
    )
    SELECT year_month, SUM(monthly_sales) AS cumulative_sales
    FROM monthly_sales
    GROUP BY strftime("%Y", year_month), strftime("%m", year_month)
    ORDER BY year_month
    '''
    cumulative_sales_per_month = pd.read_sql(query, conn)
    print(cumulative_sales_per_month)

    print("\n3. Calculate the year-over-year growth rate of total sales:")
    query = '''
    WITH yearly_sales AS (
        SELECT strftime("%Y", order_date) AS year, SUM(amount) AS total_sales
        FROM orders
        GROUP BY year
    )
    SELECT year,
           total_sales,
           LAG(total_sales) OVER (ORDER BY year) AS previous_year_sales,
           (total_sales - LAG(total_sales) OVER (ORDER BY year)) * 100.0 / LAG(total_sales) OVER (ORDER BY year) AS growth_rate
    FROM yearly_sales
    '''
    yearly_growth_rate = pd.read_sql(query, conn)
    print(yearly_growth_rate)

    print("\n4. Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase:")
    query = '''
    WITH first_purchase AS (
        SELECT customer_id, MIN(order_date) AS first_purchase_date
        FROM orders
        GROUP BY customer_id
    ),
    subsequent_purchases AS (
        SELECT DISTINCT f.customer_id
        FROM first_purchase f
        JOIN orders o ON f.customer_id = o.customer_id
        WHERE o.order_date > f.first_purchase_date AND o.order_date <= date(f.first_purchase_date, '+6 months')
    )
    SELECT
        (COUNT(DISTINCT subsequent_purchases.customer_id) * 100.0 / COUNT(DISTINCT first_purchase.customer_id)) AS retention_rate
    FROM first_purchase
    LEFT JOIN subsequent_purchases ON first_purchase.customer_id = subsequent_purchases.customer_id
    '''
    retention_rate = pd.read_sql(query, conn)
    print(retention_rate)

    print("\n5. Identify the top 3 customers who spent the most money in each year:")
    query = '''
    WITH yearly_spending AS (
        SELECT strftime("%Y", order_date) AS year, customer_id, SUM(amount) AS total_spent
        FROM orders
        GROUP BY year, customer_id
    )
    SELECT year, customer_id, total_spent
    FROM (
        SELECT year, customer_id, total_spent,
               ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_spent DESC) AS rank
        FROM yearly_spending
    )
    WHERE rank <= 3
    '''
    top_customers_per_year = pd.read_sql(query, conn)
    print(top_customers_per_year)

def main():
    # Connect to SQLite database
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Create tables
    create_tables(cursor)

    # Load data into tables
    files_and_tables = [
        ('customers.csv', 'customers'),
        ('orders.csv', 'orders'),
        ('products.csv', 'products'),
        ('order_products.csv', 'order_products'),
        ('sales.csv', 'sales')
    ]
    for file_path, table_name in files_and_tables:
        load_csv_to_table(file_path, table_name, conn)

    # Execute queries
    basic_queries(conn)
    intermediate_queries(conn)
    advanced_queries(conn)

    # Close connection
    conn.close()

if __name__ == '__main__':
    main()
