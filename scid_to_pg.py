## Current state: This script reads data from a SCID file, processes it, and loads it into a PostgreSQL database using async operations. It handles checkpoints to ensure data continuity and allows for periodic updates.
## It assumes the folowing database schema:
##                              Table "public.market_data"
##       Column      |              Type              | Collation | Nullable | Default
## ------------------+--------------------------------+-----------+----------+---------
##  time             | timestamp(3) without time zone |           | not null |
##  trade_date       | date                           |           |          |
##  trade_time       | time(3) without time zone      |           |          |
##  open             | numeric(44,2)                  |           |          |
##  high             | numeric(15,2)                  |           |          |
##  low              | numeric(15,2)                  |           |          |
##  close            | numeric(15,2)                  |           |          |
##  volume           | integer                        |           |          |
##  number_of_trades | integer                        |           |          |
##  bid_volume       | integer                        |           |          |
##  ask_volume       | integer                        |           |          |
##  symbol           | text                           |           |          |
##  symbol_period    | text                           |           |          |
## if you need to change table columns, you can disable timescaledb.compress, and then re-enable it:
# ALTER TABLE market_data SET (timescaledb.compress,
#    timescaledb.compress_orderby = 'time ASC',
#    timescaledb.compress_segmentby = 'symbol',
#    timescaledb.compress_chunk_time_interval='7 days'
# );
# # Also, a materialized view is created to summarize the data in daily time buckets:
# CREATE MATERIALIZED VIEW one_day_candle
# WITH (timescaledb.continuous) AS
#     SELECT
#         time_bucket('1 day', time) AS time_bucket,
#         symbol,
# 		symbol_period,
#         FIRST(close, time) AS "open",
#         MAX(close) AS high,
#         MIN(close) AS low,
#         LAST(close, time) AS "close",
#         SUM(volume) AS day_volume
#     FROM market_data
#     GROUP BY time_bucket, symbol, symbol_period;

import asyncio
import asyncpg
import polars as pl
import numpy as np
import sys
from pathlib import Path
import time
import os
import json
from dotenv import load_dotenv
import re

# Load environment variables from .env file
load_dotenv()

# Establishes a connection to the PostgreSQL database using provided credentials.
async def db_connect():
    return await asyncpg.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE"),
        port=os.getenv("DB_PORT")
    )

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

# Inserts data into the specified table in the PostgreSQL database.
async def load_data_to_db(conn, df, table_name, symbol, symbol_period):
    # SCDateTime epoch is December 30, 1899
    epoch = pl.datetime(1899, 12, 30, 0, 0, 0, 0, time_unit="us")

    df = df.with_columns([
        (epoch + pl.duration(microseconds=pl.col('scdatetime'))).alias('time'),
        (epoch + pl.duration(microseconds=pl.col('scdatetime'))).cast(pl.Date).alias('trade_date'),
        (epoch + pl.duration(microseconds=pl.col('scdatetime'))).cast(pl.Time).alias('trade_time'),
        pl.col('open'), 
        pl.col('high'), 
        pl.col('low'), 
        pl.col('close'), 
        pl.col('totalvolume').alias('volume'),
        pl.col('numtrades').alias('number_of_trades'),
        pl.col('bidvolume').alias('bid_volume'),
        pl.col('askvolume').alias('ask_volume'),
        pl.lit(symbol).alias('symbol'),
        pl.lit(symbol_period).alias('symbol_period')
    ]).select([
        'time', 'trade_date', 'trade_time', 'open', 'high', 'low', 'close',
        'volume', 'number_of_trades', 'bid_volume', 'ask_volume', 'symbol', 'symbol_period'
    ])


    # Filter out rows with erroneous values (anything approaching 1e13)
    # Using a more conservative threshold like 1e10 to be safe
    # df_filtered = df.filter(
    #     (pl.col('open').abs() > 1e10) |
    #     (pl.col('high').abs() > 1e10) |
    #     (pl.col('low').abs() > 1e10) |
    #     (pl.col('close').abs() > 1e10) |
    #     (pl.col('volume').abs() > 1e10) |
    #     (pl.col('bid_volume').abs() > 1e10) |
    #     (pl.col('ask_volume').abs() > 1e10)
    # )

    # # save filtered data to csv file for debugging
    # debug_csv_path = Path(f"debug_{table_name}.csv")
    # df_filtered.write_csv(debug_csv_path)
    # print('saved filtered data to', debug_csv_path)
    # gets the minimum value for each column
    min_values = df.select([
        pl.col('open').min().alias('min_open'),
        pl.col('high').min().alias('min_high'),
        pl.col('low').min().alias('min_low'),
        pl.col('close').min().alias('min_close'),
        pl.col('volume').min().alias('min_volume'),
        pl.col('number_of_trades').min().alias('min_number_of_trades'),
        pl.col('bid_volume').min().alias('min_bid_volume'),
        pl.col('ask_volume').min().alias('min_ask_volume')
    ]).to_dict(as_series=False)
    print(f"Minimum values for {table_name}: {min_values}")
    time.sleep(300)

    # Use copy_records_to_table for efficient bulk loading
    columns = [
        'time', 'trade_date', 'trade_time', 'open', 'high', 'low', 'close',
        'volume', 'number_of_trades', 'bid_volume', 'ask_volume', 'symbol', 'symbol_period'
    ]

    await conn.copy_records_to_table(
        table_name,
        records=df.iter_rows(),
        columns=columns
    )

    # for reference, this is the executemany method that can be used instead of copy_records_to_table
    # depreciated as it used a lot of memory
    # records = [tuple(row) for row in df.iter_rows()]
    # await conn.executemany(f"""
    #     INSERT INTO {table_name} (
    #         time, trade_date, trade_time, open, high, low, close,
    #         volume, number_of_trades, bid_volume, ask_volume, symbol, symbol_period
    #     )
    #     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    # """, records)

# Coordinates the data processing workflow: connects to the database, reads data from the SCID file, and loads it into the database. Manages checkpoints to handle data continuity.
async def main(table_name, scid_file):
    start_time = time.time()
    conn = await db_connect()

    # Extract symbol and symbol_period from the file name
    file_name = Path(scid_file).stem  # Get file name without extension

    # use regex to match the symbol and period
    # Example: ESU5.CME.scid -> symbol: ESU5, symbol_period: CME
    pattern = r'^([A-Z]{2,3})([A-Z]\d)\.([A-Z]+)$'
    match = re.match(pattern, file_name)
    if match:
        symbol = match.group(1)
        symbol_period = match.group(2)
    else:
        # Fallback to splitting by '.' if regex doesn't match   
        print(f"Warning: Unable to parse symbol and period from file name '{file_name}'. Using fallback method.")    
        parts = file_name.split('.')
        symbol = parts[0] if len(parts) > 0 else ""
        symbol_period = parts[1] if len(parts) > 1 else ""

    checkpoint_file = Path(f"checkpoint.json")

    # Check if the initial load is done, otherwise set last_position to 0 and initial_load_done to False
    last_position = 0
    initial_load_done = False

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
            pass

    
    print(f"Processing SCID file: {scid_file}, Symbol: {symbol}, Period: {symbol_period}")

    intermediate_np_array, new_position = get_scid_np(scid_file, offset=last_position)

    if new_position > last_position:  # Only update if there's new data
        df_raw = pl.DataFrame(intermediate_np_array)
        await load_data_to_db(conn, df_raw, table_name, symbol, symbol_period)
        last_position = new_position  # Updates the last position

        # update the checkpoint file with the new position and initial load status
        with open(checkpoint_file, "w") as f:
            checkpoint_data[f'{symbol}{symbol_period}'] = {
                "last_position": last_position, 
                "initial_load_done": True
                }
            json.dump(checkpoint_data, f, indent=4)
    else:
        print(f"No new data to process for {table_name} at position {last_position}. Skipping update.")

    await conn.close()

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

table_name = "market_data"  # Specify the unique table name for your data.
scid_file = r"C:\auxDrive\SierraChart2\Data\ESZ4.CME.scid"  # Set the file path to your SCID file.

# Continuously update data from SCID file every 'x' seconds. Here, "1" means pause the execution for 1 second between updates.
while True:
    asyncio.run(main(table_name, scid_file))
    time.sleep(1000)  # Pause for 1 second before the next update. Adjust as needed.
