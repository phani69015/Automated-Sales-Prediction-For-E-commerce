import pandas as pd
import mysql.connector

# Load CSV file
df = pd.read_csv(r'C:\Projects\Walmart\data.csv')

# Database connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Phani@9959",  # Replace with your MySQL root password
    database="Walmart"
)

# Create a cursor object
cur = conn.cursor()

# Insert data into the table
for index, row in df.iterrows():
    sql = """
        INSERT INTO sales_data (Date, Sales, DayOfWeek, Month, IsWeekend, Sales_Lag_7) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.execute(sql, (
        row['Date'], 
        row['Sales'], 
        row['DayOfWeek'], 
        row['Month'], 
        row['IsWeekend'], 
        row['Sales_Lag_7']
    ))

# Commit changes and close the connection
conn.commit()
cur.close()
conn.close()

print("Data inserted successfully!")
