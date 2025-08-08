"""
1_upload_to_postgres.py

This script is the second step in the data ingestion pipeline. It is designed
to efficiently upload processed CSV trading data into a PostgreSQL/TimescaleDB
database.

It works in conjunction with '0_prepare_for_timescaledb_upload.py', which
generates the standardized CSV files that this script consumes. After data
is uploaded, '2_deduplicate.py' can be used to clean and deduplicate the
uploaded trade data.

The script provides two methods for bulk upload:
1. COPY method (recommended for performance)
2. Batch insert method (for compatibility)

Usage:
    python3 1_upload_to_postgres.py

The script is currently configured with hardcoded paths and database credentials
for demonstration purposes. For production use, it is recommended to:
- Prompt the user for the CSV file path.
- Use environment variables or a configuration file for database credentials.

Example:
    python3 1_upload_to_postgres.py
    (The script will attempt to upload the hardcoded CSV file to the specified DB table.)
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import io
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def bulk_upload_to_postgres(csv_file_path, table_name, connection_params, create_table=True, batch_size=10000):
    """
    Bulk upload CSV data to PostgreSQL table using efficient COPY method.
    
    Args:
        csv_file_path (str): Path to the processed CSV file
        table_name (str): Name of the PostgreSQL table to insert into
        connection_params (dict): Database connection parameters
            {
                'host': 'localhost',
                'database': 'your_db',
                'user': 'your_user',
                'password': 'your_password',
                'port': '5432'
            }
        create_table (bool): Whether to create the table if it doesn't exist
        batch_size (int): Number of rows to process at once
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    try:
        # Read the CSV file
        print(f"Reading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        # Convert datetime columns properly
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
        
        print(f"Loaded {len(df)} rows from CSV")
        
        # Connect to PostgreSQL
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Create table if requested
        # if create_table:
        #     create_table_sql = f"""
        #     CREATE TABLE IF NOT EXISTS {table_name} (
        #         trade_date VARCHAR(20),
        #         trade_time TIME,
        #         time TIMESTAMP,
        #         symbol VARCHAR(10),
        #         symbol_period VARCHAR(10),
        #         open DECIMAL(15,2),
        #         high DECIMAL(15,2),
        #         low DECIMAL(15,2),
        #         close DECIMAL(15,2),
        #         volume INTEGER,
        #         bid_volume INTEGER,
        #         ask_volume INTEGER
        #     );
        #     """
        #     cursor.execute(create_table_sql)
        #     conn.commit()
        #     print(f"Table '{table_name}' created/verified")
        
        # Use COPY for efficient bulk insert
        print("Starting bulk upload using COPY...")
        
        # Create a StringIO buffer
        buffer = io.StringIO()
        
        # Write DataFrame to buffer as CSV
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        # Use COPY to insert data
        cursor.copy_from(
            buffer,
            table_name,
            columns=df.columns.tolist(),
            sep='\t',
            null='\\N'
        )
        
        conn.commit()
        print(f"Successfully uploaded {len(df)} rows to table '{table_name}'")
        
        # Verify the upload
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Total rows in table: {count}")
        
        # Show sample data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        sample_data = cursor.fetchall()
        print("\nSample data from table:")
        for row in sample_data:
            print(row)
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error during bulk upload: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

def bulk_upload_batch_method(csv_file_path, table_name, connection_params, create_table=True, batch_size=10000):
    """
    Alternative bulk upload method using execute_batch for better control.
    Use this if the COPY method has issues with your specific data.
    
    Args:
        csv_file_path (str): Path to the processed CSV file
        table_name (str): Name of the PostgreSQL table to insert into
        connection_params (dict): Database connection parameters
        create_table (bool): Whether to create the table if it doesn't exist
        batch_size (int): Number of rows to process at once
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    try:
        # Read the CSV file
        print(f"Reading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        # Convert datetime columns properly
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
        
        print(f"Loaded {len(df)} rows from CSV")
        
        # Connect to PostgreSQL
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Create table if requested
        if create_table:
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                trade_date VARCHAR(20),
                trade_time TIME,
                time TIMESTAMP,
                symbol VARCHAR(10),
                symbol_period VARCHAR(10),
                open DECIMAL(15,2),
                high DECIMAL(15,2),
                low DECIMAL(15,2),
                close DECIMAL(15,2),
                volume INTEGER,
                bid_volume INTEGER,
                ask_volume INTEGER
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print(f"Table '{table_name}' created/verified")
        
        # Prepare insert statement
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.values]
        
        # Upload in batches
        total_rows = len(data_tuples)
        uploaded_rows = 0
        
        for i in range(0, total_rows, batch_size):
            batch = data_tuples[i:i + batch_size]
            execute_batch(cursor, insert_sql, batch, page_size=batch_size)
            uploaded_rows += len(batch)
            
            # Show progress
            progress = (uploaded_rows / total_rows) * 100
            print(f"Progress: {uploaded_rows}/{total_rows} rows ({progress:.1f}%)")
        
        conn.commit()
        print(f"Successfully uploaded {total_rows} rows to table '{table_name}'")
        
        # Verify the upload
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Total rows in table: {count}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error during bulk upload: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

def main():
    """Example usage of the bulk upload functions"""
    
    # Database connection parameters from .env
    connection_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_DATABASE', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
        'port': os.getenv('DB_PORT', '5432')
    }
    
    # Get file path and table name from user
    csv_file = r"C:\auxDrive\SierraChart2\Data\MESH5.CME_ticks_processed.csv" # input("Enter path to your processed CSV file: ").strip()
    table_name = "market_data" #input("Enter PostgreSQL table name: ").strip()
    
    # Remove quotes if present
    if csv_file.startswith('"') and csv_file.endswith('"'):
        csv_file = csv_file[1:-1]
    elif csv_file.startswith("'") and csv_file.endswith("'"):
        csv_file = csv_file[1:-1]
    
    # Ask for connection details (commented out as per original script's intent to use hardcoded/env vars)
    # print("\nEnter your PostgreSQL connection details:")
    # connection_params['host'] = input("Host (default: localhost): ").strip() or 'localhost'
    # connection_params['database'] = input("Database name: ").strip()
    # connection_params['user'] = input("Username: ").strip()
    # connection_params['password'] = input("Password: ").strip()
    # connection_params['port'] = input("Port (default: 5432): ").strip() or '5432'
    
    # Ask which method to use
    print("\nChoose upload method:")
    print("1. COPY method (faster, recommended)")
    print("2. Batch insert method (more compatible)")
    method = '1' # input("Enter choice (1 or 2): ").strip()
    
    # Perform upload
    if method == '2':
        success = bulk_upload_batch_method(csv_file, table_name, connection_params)
    else:
        success = bulk_upload_to_postgres(csv_file, table_name, connection_params)
    
    if success:
        print("\n✅ Bulk upload completed successfully!")
    else:
        print("\n❌ Bulk upload failed!")

if __name__ == "__main__":
    main()
