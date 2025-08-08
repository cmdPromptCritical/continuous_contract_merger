# check if timescaledb is installed
import os
import sys
def check_timescaledb_installed():
    try:
        import timescaledb
        return True
    except ImportError:
        return False
if not check_timescaledb_installed():
    print("TimescaleDB is not installed. Please install it using 'pip install timescaledb'.")
    sys.exit(1)
import psycopg2
from psycopg2 import sql
def upload_to_postgres(data, table_name, db_config):
    """
    Upload data to a PostgreSQL database.

    :param data: List of dictionaries containing the data to upload.
    :param table_name: Name of the table to upload the data to.
    :param db_config: Dictionary containing database connection parameters.
    """
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Create table if it does not exist
        columns = data[0].keys()
        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(sql.Identifier(col) for col in columns)
        )
        cursor.execute(create_table_query)

        # Insert data into the table
        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(sql.Identifier(col) for col in columns),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        for row in data:
            cursor.execute(insert_query, tuple(row.values()))

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()