import time
import pyodbc
import psycopg2
import mysql.connector
from mysql.connector import pooling

# Initialize MySQL connection pool
mysql_pool = pooling.MySQLConnectionPool(pool_name="mysql_pool",
                                         pool_size=5,
                                         host="localhost",
                                         user="root",
                                         password="3124ahlaMNAM",
                                         database="ShagufTask")

# PostgreSQL connection parameters
pg_params = {
    'host': 'localhost',
    'database': 'ShagufSecondTask',
    'user': 'postgres',
    'password': '3124ahlaMNAM'
}

def get_mysql_connection():
    return mysql_pool.get_connection()

def mirror_data():
    try:
        # Retrieve data from MySQL
        with get_mysql_connection() as mysql_conn:
            mysql_cursor = mysql_conn.cursor()
            mysql_query = "SELECT id, name, city, revenue FROM myapp_shaguftask"
            mysql_cursor.execute(mysql_query)
            data_to_mirror = mysql_cursor.fetchall()

        # Connect to PostgreSQL
        with psycopg2.connect(**pg_params) as pg_conn:
            pg_cursor = pg_conn.cursor()

            for row in data_to_mirror:
                pg_cursor.execute("SELECT id FROM shaguftable WHERE id = %s", (row[0],))
                existing_record = pg_cursor.fetchone()
                if existing_record:
                    pg_cursor.execute("UPDATE shaguftable SET name = %s, city = %s, revenue = %s WHERE id = %s",
                                      (row[1], row[2], row[3], row[0]))
                    print("Data Updated")
                    print(row[0])
                else:
                    pg_cursor.execute("INSERT INTO shaguftable (id, name, city, revenue) VALUES (%s, %s, %s, %s)", row)
                    print("Data Inserted")
                    print(row[0])

            # Delete records in PostgreSQL that do not exist in MySQL
            pg_cursor.execute("SELECT id FROM shaguftable")
            pg_records = pg_cursor.fetchall()
            pg_ids = [record[0] for record in pg_records]
            mysql_ids = [record[0] for record in data_to_mirror]
            for pg_id in pg_ids:
                if pg_id not in mysql_ids:
                    pg_cursor.execute("DELETE FROM shaguftable WHERE id = %s", (pg_id,))
                    print("Data Deleted")
                    print(pg_id)

            # Commit changes
            pg_conn.commit()
            print("Data mirroring completed successfully.")
    except Exception as e:
        print("An error occurred:", str(e))

# Continuously run the mirroring script
while True:
    mirror_data()
    # Sleep for a specified time before fetching new data again
    time.sleep(20)  # Sleep for 20 seconds before fetching new data
