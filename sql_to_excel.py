import pandas as pd
import pymysql

# Creating a connection to SQL Database
connection = pymysql.connect(host='localhost', user='root', database='baslerweb_db', password='actowiz', charset='utf8mb4', autocommit=True)
if connection.open:
    print('Database connection Successful!')
else:
    print('Database connection Un-Successful.')
cursor = connection.cursor()  # Creating a cursor to execute SQL Queries

query = '''SELECT * FROM products_links;'''  # Query that will retrieve all data from Database table

data_frame = pd.read_sql(sql=query, con=connection)  # Reading Data from Database table and converting into DataFrame

data_frame.to_excel('basler_web_product_links.xlsx')  # Converting DataFrame into Excel file
