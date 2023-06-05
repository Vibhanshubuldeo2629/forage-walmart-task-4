import pandas as pd
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('C:/Users/vibha/Desktop/RunTest/forage-walmart-task-4/shipment_database.db')
cursor = conn.cursor()

# Read and insert data from Spreadsheet 0 (CSV format)
df0 = pd.read_csv('C:/Users/vibha/Desktop/RunTest/forage-walmart-task-4/data/shipping_data_0.csv')
df0.to_sql('Spreadsheet_0', conn, if_exists='replace', index=False)

# Read data from Spreadsheet 2 (CSV format)
df2 = pd.read_csv('C:/Users/vibha/Desktop/RunTest/forage-walmart-task-4/data/shipping_data_2.csv')

# Read and process data from Spreadsheet 1 (CSV format)
df1 = pd.read_csv('C:/Users/vibha/Desktop/RunTest/forage-walmart-task-4/data/shipping_data_1.csv')

# Combine rows based on shipping identifier and calculate total quantity
grouped_df = df1.groupby('Shipping ID').agg({'Product': ', '.join, 'Quantity': 'sum'}).reset_index()

# Merge with Spreadsheet 2 to get origin and destination
merged_df = pd.merge(grouped_df, df2, on='Shipping ID')

# Insert rows into the database
for _, row in merged_df.iterrows():
    cursor.execute('INSERT INTO Spreadsheet_1 (Shipping ID, Product, Quantity, Origin, Destination) VALUES (?, ?, ?, ?, ?)', 
                   (row['Shipping ID'], row['Product'], row['Quantity'], row['Origin'], row['Destination']))

conn.commit()

# Close the database connection
conn.close()
