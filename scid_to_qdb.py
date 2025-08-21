## Current state: This script reads data from a SCID file, processes it, and loads it into a QuestDB database using parallel batch processing. It handles checkpoints to ensure data continuity and allows for periodic updates.
## It assumes the following QuestDB table schema:
## CREATE TABLE trades (
##     time TIMESTAMP,                -- Designated timestamp for time-series queries
##     open DOUBLE,                   -- Use DOUBLE for performance (see note above)
##     high DOUBLE,
##     low DOUBLE,
##     close DOUBLE,
##     volume INT,
##     number_of_trades INT,
##     bid_volume INT,
##     ask_volume INT,
##     symbol SYMBOL CAPACITY 256,
##     symbol_period SYMBOL CAPACITY 256
##     front_contract BOOLEAN, -- to mark the front contract. set to False for all historical data and another process will set it to True for the current front contract
## ) TIMESTAMP(time)
## PARTITION BY DAY WAL
## DEDUP UPSERT KEYS(time, symbol, symbol_period);

import asyncio
import polars as pl
import numpy as np
import sys
from pathlib import Path
import time
import os
import json
from dotenv import load_dotenv
import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from questdb.ingress import Sender, IngressError, TimestampNanos
import psycopg2

class WorkerFailureException(Exception):
    """Exception raised when one or more workers fail during batch processing"""
    pass

# Load environment variables from .env file
load_dotenv('qdb.env')

def create_table_if_not_exists(table_name, questdb_host, questdb_pg_port, user, password):
    """Create a table in QuestDB if it does not already exist."""
    conn_str = f"host='{questdb_host}' port='{questdb_pg_port}' dbname='qdb' user='{user}' password='{password}'"
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    time TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume INT,
                    number_of_trades INT,
                    bid_volume INT,
                    ask_volume INT,
                    symbol SYMBOL CAPACITY 256,
                    symbol_period SYMBOL CAPACITY 256,
                    front_contract BOOLEAN
                ) TIMESTAMP(time)
                PARTITION BY DAY WAL
                DEDUP UPSERT KEYS(time, symbol, symbol_period);
                """
                cur.execute(create_table_query)
                print(f"Table '{table_name}' created or already exists.")
    except psycopg2.Error as e:
        print(f"Error connecting to QuestDB or creating table: {e}")
        sys.exit(1)

def get_scid_np(scidFile, offset=0):
    f = Path(scidFile)
    assert f.exists(), "SCID file not found"
    with open(scidFile, 'rb') as file:
        file.seek(0, os.SEEK_END)
        file_size = file.tell()  # Total size of the file
        sciddtype = np.dtype([
            ("scdatetime", "<u8"),
            ("open", "<f4"),
            ("high", "<f4"),
            ("low", "<f4"),
            ("close", "<f4"),
            ("numtrades", "<u4"),
            ("totalvolume", "<u4"),
            ("bidvolume", "<u4"),
            ("askvolume", "<u4"),
        ])
        record_size = sciddtype.itemsize

        # Adjust the offset if not within the file size
        if offset >= file_size:
            offset = file_size - (file_size % record_size)
        elif offset < 56:
            offset = 56  # Skip header assumed to be 56 bytes

        file.seek(offset)
        scid_as_np_array = np.fromfile(file, dtype=sciddtype)
        new_position = file.tell()  # Update the position after reading

    return scid_as_np_array, new_position

def send_batch(conf_str, table_name, batches, timestamp_name, df_pandas):
    """Process batches of data and send to QuestDB"""
    try:
        with Sender.from_conf(conf_str, auto_flush=False, init_buf_size=100_000_000) as qdb_sender:
            batch_count = 0
            failed = False
            while True:
                try:
                    start_idx, end_idx = batches.pop()
                    batch_count += 1
                    print(f"Processing batch {batch_count} with {end_idx - start_idx} rows")

                    # Only create the batch DataFrame when needed
                    batch_df = df_pandas.iloc[start_idx:end_idx].copy()

                    # Send the batch to QuestDB
                    qdb_sender.dataframe(
                        batch_df,
                        table_name=table_name,
                        symbols=['symbol', 'symbol_period'],  # Mark these columns as SYMBOL types
                        at=timestamp_name
                    )
                    qdb_sender.flush()
                    print(f"Successfully sent batch {batch_count}")

                    # Explicitly delete the batch to free memory
                    del batch_df

                except IndexError:
                    # No more batches to process
                    break
                except Exception as e:
                    print(f"Error processing batch {batch_count}: {e}")
                    failed = True
                    # Re-add the batch to the queue for retry (optional)
                    # batches.append((start_idx, end_idx))
                    break

            print(f"Thread completed. Processed {batch_count} batches.")
            if failed:
                raise Exception("One or more batches failed to process")

    except IngressError as e:
        print(f"QuestDB ingestion error: {e}")
        raise  # Re-raise to propagate the error
    except Exception as e:
        print(f"Unexpected error in send_batch: {e}")
        raise  # Re-raise to propagate the error

def load_data_to_questdb(df, table_name, symbol, symbol_period, questdb_host='localhost', questdb_port=9009):
    """Load data into QuestDB using parallel batch processing"""
    
    # SCDateTime epoch is December 30, 1899
    epoch = pl.datetime(1899, 12, 30, 0, 0, 0, 0, time_unit="us")

    # Process the dataframe to match QuestDB schema
    df_processed = df.with_columns([
        (epoch + pl.duration(microseconds=pl.col('scdatetime'))).alias('time'),
        pl.col('open').cast(pl.Float64), 
        pl.col('high').cast(pl.Float64), 
        pl.col('low').cast(pl.Float64), 
        pl.col('close').cast(pl.Float64), 
        pl.col('totalvolume').alias('volume').cast(pl.Int32),
        pl.col('numtrades').alias('number_of_trades').cast(pl.Int32),
        pl.col('bidvolume').alias('bid_volume').cast(pl.Int32),
        pl.col('askvolume').alias('ask_volume').cast(pl.Int32),
        pl.lit(symbol).alias('symbol'),
        pl.lit(symbol_period).alias('symbol_period'),
        pl.lit(False).alias('front_contract')  # Add front_contract column, all False
    ]).select([
        'time', 'open', 'high', 'low', 'close',
        'volume', 'number_of_trades', 'bid_volume', 'ask_volume', 'symbol', 'symbol_period', 'front_contract'
    ])

    # convert time column to int64 for QuestDB
    df_processed = df_processed.with_columns(
        pl.col('time').cast(pl.Int64).alias('time')
    )

    # Convert to Pandas for QuestDB ingestion
    df_pandas = df_processed.to_pandas()

    # Ensure the timestamp column is properly formatted for QuestDB
    df_pandas['time'] = pd.to_datetime(df_pandas['time'], utc=True, unit='us')
    print(df_pandas.head())
    #time.sleep(100000)  # Allow time for the print to flush before proceeding
    
    print(f"Preparing to load {len(df_pandas)} records to QuestDB")

    # Create batches using indices instead of copying data
    batches = deque()
    batch_size = int(os.getenv("BATCH_SIZE", "200000"))  # Default 100k records per batch. Set to 1M for high performance
    parallel_workers = int(os.getenv("PARALLEL_WORKERS", "8"))  # Default 8 parallel connections

    # Calculate batch indices instead of splitting the DataFrame
    total_rows = len(df_pandas)
    total_batches = total_rows // batch_size + (1 if total_rows % batch_size > 0 else 0)
    print(f"Splitting data into {total_batches} batches of up to {batch_size} records each")

    for i in range(total_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_rows)
        if start_idx < end_idx:  # Only add non-empty batches
            batches.append((start_idx, end_idx))
            print(f"Created batch {i+1} with {end_idx - start_idx} records")

    # QuestDB connection configuration
    conf_str = f'http::addr={questdb_host}:{questdb_port};'
    timestamp_name = 'time'

    print(f"Starting parallel ingestion with {parallel_workers} workers")
    start_time = time.time()

    # Use ThreadPoolExecutor for parallel batch processing
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = []
        for i in range(parallel_workers):
            future = executor.submit(send_batch, conf_str, table_name, batches, timestamp_name, df_pandas)
            futures.append(future)
            print(f"Started worker {i+1}")
        
        # Wait for all workers to complete
        worker_failed = False
        for i, future in enumerate(futures):
            try:
                future.result()
                print(f"Worker {i+1} completed successfully")
            except Exception as e:
                print(f"Worker {i+1} failed with error: {e}")
                worker_failed = True

        # If any worker failed, raise an exception to stop the process
        if worker_failed:
            raise WorkerFailureException("One or more workers failed during batch processing")

    end_time = time.time()
    print(f"Batch processing completed in {end_time - start_time:.2f} seconds")
    
    if not batches:
        print("All batches processed successfully")
    else:
        print(f"Warning: {len(batches)} batches remaining unprocessed")

def main(table_name, scid_file):
    """Main processing function"""
    start_time = time.time()
    
    # Get QuestDB connection details from environment variables for table creation
    questdb_host = os.getenv("DB_HOST", "localhost")
    questdb_pg_port = int(os.getenv("QUESTDB_PG_PORT", "8812"))  # Standard PG port for QuestDB, needed for create if not exist function
    questdb_user = os.getenv("DB_USER", "admin")
    questdb_password = os.getenv("DB_PASSWORD", "quest")

    # Create table if it doesn't exist
    create_table_if_not_exists(table_name, questdb_host, questdb_pg_port, questdb_user, questdb_password)

    # Extract symbol and symbol_period from the file name
    file_name = Path(scid_file).stem  # Get file name without extension

    # Use regex to match the symbol and period. Might need adjustment based on your SCID file naming conventions.
    # Example: ESU5.CME.scid -> symbol: ESU5, symbol_period: CME
    pattern = r'^([A-Z]{2,3})([A-Z]\d)\.([A-Z]+)$'
    match = re.match(pattern, file_name)
    # pattern will return groups ('ES', 'U5', 'CME') for ESU5.CME.scid
    if match:
        symbol = match.group(1)  # Combine symbol parts (ES + U5)
        symbol_period = match.group(2)
    else:
        # Fallback to splitting by '.' if regex doesn't match   
        print(f"Warning: Unable to parse symbol and period from file name '{file_name}'. Using fallback method.")    
        parts = file_name.split('.')
        symbol = parts[0] if len(parts) > 0 else ""
        symbol_period = parts[1] if len(parts) > 1 else ""

    checkpoint_file = Path(f"checkpoint_qdb.json")

    # Check if the initial load is done, otherwise set last_position to 0 and initial_load_done to False
    last_position = 0
    initial_load_done = False
    checkpoint_data = {}

    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, "r") as f:
                checkpoint_data = json.load(f)
                # Ensure the correct table_name is used for checkpoint data retrieval
                table_data = checkpoint_data.get(f'{symbol}{symbol_period}', {})
                last_position = table_data.get("last_position", 0)
                initial_load_done = table_data.get("initial_load_done", False)
                print(f"Last position for {symbol}{symbol_period}: {last_position}, Initial load done: {initial_load_done}")
        except json.JSONDecodeError:
            checkpoint_data = {}
            print("Checkpoint file is corrupted or empty. Starting fresh.")

    print(f"Processing SCID file: {scid_file}, Symbol: {symbol}, Period: {symbol_period}")

    intermediate_np_array, new_position = get_scid_np(scid_file, offset=last_position)

    if new_position > last_position:  # Only update if there's new data
        print(f"Found {len(intermediate_np_array)} new records")
        df_raw = pl.DataFrame(intermediate_np_array)
        
        # Get QuestDB connection details from environment variables
        questdb_host = os.getenv("DB_HOST", "localhost")
        questdb_port = int(os.getenv("DB_PORT", "9000"))
        
        load_data_to_questdb(df_raw, table_name, symbol, symbol_period, questdb_host, questdb_port)
        
        last_position = new_position  # Updates the last position

        # Update the checkpoint file with the new position and initial load status
        checkpoint_data[f'{symbol}{symbol_period}'] = {
            "last_position": last_position, 
            "initial_load_done": True
        }
        
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint_data, f, indent=4)
            
        print(f"Checkpoint updated: position {last_position}")
    else:
        print(f"No new data to process for {table_name} at position {last_position}. Skipping update.")

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    # Import pandas here since it's needed for QuestDB ingestion
    import pandas as pd
    
    table_name = "trades"  # QuestDB table name
    scid_file = r"C:\auxDrive\SierraChart2\Data\ESM5.CME.scid"  # Set the file path to your SCID file.

    # Continuously update data from SCID file every 'x' seconds
    while True:
        try:
            main(table_name, scid_file)
            sleep_duration = int(os.getenv("SLEEP_DURATION", "1000"))  # Default 1000 seconds
            print(f"Sleeping for {sleep_duration} seconds...")
            time.sleep(sleep_duration)
        except KeyboardInterrupt:
            print("Process interrupted by user")
            break
        except WorkerFailureException as e:
            print(f"Worker failure detected: {e}")
            print("Exiting due to worker failure...")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            print("Retrying in 60 seconds...")
            time.sleep(60)