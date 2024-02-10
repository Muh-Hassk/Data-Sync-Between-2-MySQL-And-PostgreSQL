import pyodbc
import psycopg2
import mysql.connector

# Connect to MySQL
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="3124ahlaMNAM",
    database="ShagufTask"
)

# Connect to PostgreSQL
pg_conn = psycopg2.connect(host="localhost", database="ShagufSecondTask", user="postgres", password="3124ahlaMNAM")

# Retrieve data from MySQL
mysql_cursor = mysql_conn.cursor()
mysql_query = "SELECT id, name, city, revenue FROM myapp_shaguftask"
mysql_cursor.execute(mysql_query)
data_to_transfer = mysql_cursor.fetchall()

# Insert data into PostgreSQL
pg_cursor = pg_conn.cursor()
for row in data_to_transfer:
    pg_cursor.execute("INSERT INTO shaguftable (id, name, city, revenue) VALUES (%s, %s, %s, %s)", row)

# Commit changes and close connections
pg_conn.commit()
pg_conn.close()
mysql_conn.close()
