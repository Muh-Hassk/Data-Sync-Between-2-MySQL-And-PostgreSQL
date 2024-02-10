import time
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

def mirror_data():
    try:
        # Retrieve data from MySQL
        mysql_cursor = mysql_conn.cursor()
        mysql_query = "SELECT id, name, city, revenue FROM myapp_shaguftask"
        mysql_cursor.execute(mysql_query)
        data_to_mirror = mysql_cursor.fetchall()

        # Insert or update data into PostgreSQL
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

        # Commit changes
        pg_conn.commit()
        print("Data mirroring completed successfully.")
    except Exception as e:
        print("An error occurred:", str(e))

# Continuously run the mirroring script
while True:
    mirror_data()
    # Sleep for a specified interval before fetching new data again
    time.sleep(10)  # Sleep for 1 minute before fetching new data
