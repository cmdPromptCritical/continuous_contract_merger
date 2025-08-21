# Continuous Futures Contract Data Management

This repository contains Python scripts for managing and processing futures contract data, specifically for creating continuous contract data from individual contract files.

## Scripts

### `scid_to_qdb.py`

This script reads futures data from Sierra Chart `.scid` files and inserts it into a QuestDB database. 

**Features:**

*   **Data Ingestion:** Reads `.scid` files, which contain tick data for futures contracts.
*   **Database Integration:** Inserts the data into a QuestDB time-series database.
*   **Checkpointing:** Uses a `checkpoint.json` file to keep track of the last processed data, ensuring that the script can be run repeatedly without duplicating data.
*   **Continuous Updates:** The script can be run in a loop to continuously monitor for new data in the `.scid` file and update the database accordingly.
*   **Table Creation:** Automatically creates the necessary table in QuestDB if it doesn't already exist.

### `scid_to_pg.py`

This script is similar to `scid_to_qdb.py` but is designed to work with a PostgreSQL database instead of QuestDB. It reads data from `.scid` files and loads it into a PostgreSQL table.

### `lib/compute_front_contract_questdb.py`

This script identifies the front contract for each day and updates the `front_contract` flag in the `trades` table.

### `lib/create_materialized_views_qdb.py`

This script creates a materialized view in QuestDB to aggregate the trade data into OHLC (Open, High, Low, Close) format for a specified time interval (e.g., 15 seconds).

## Workflow

The process of creating a continuous contract from individual `.scid` files involves the following steps:

### 1. Uploading Data to QuestDB

The first step is to upload the raw futures data from the `.scid` files into a QuestDB database. This is done using the `scid_to_qdb.py` script.

1.  **Prerequisites:**
    *   Ensure you have a running instance of QuestDB.
    *   Create a `qdb.env` file with the necessary database connection details (see `.env.example`).

2.  **Table Schema:**
    The script assumes the following table schema in QuestDB. If the table does not exist, it will be created automatically.

    ```sql
    CREATE TABLE trades (
        time TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume INT,
        number_of_trades INT,
        bid_volume INT,
        ask_volume INT,
        symbol SYMBOL,
        symbol_period SYMBOL,
        front_contract BOOLEAN
    ) TIMESTAMP(time)
    PARTITION BY DAY WAL
    DEDUP UPSERT KEYS(time, symbol, symbol_period);
    ```

3.  **Execution:**
    Run the `scid_to_qdb.py` script, specifying the path to the `.scid` file. The script will process the file and upload the data to the `trades` table in QuestDB.

    ```bash
    python scid_to_qdb.py
    ```

    The script will continuously monitor the `.scid` file for new data and update the database accordingly.

### 2. Identifying the Front Contract

Once the data is in QuestDB, the next step is to identify the front contract for each day. The front contract is typically the contract with the highest trading volume for a given day.

1.  **Execution:**
    Run the `compute_front_contract_questdb.py` script.

    ```bash
    python lib/compute_front_contract_questdb.py
    ```

    This script will iterate through the data, and for each day, it will identify the `symbol_period` with the highest volume and set the `front_contract` flag to `TRUE` for that contract and `FALSE` for all others.

### 3. Creating Materialized Views

To facilitate analysis, you can create materialized views that aggregate the raw trade data into OHLC format.

1.  **Execution:**
    The `lib/create_materialized_views_qdb.py` script contains the SQL query to create a materialized view. You can execute this script to create a view named `trades_ohlc_15s` that aggregates the data into 15-second bars.

    ```sql
    CREATE MATERIALIZED VIEW IF NOT EXISTS trades_ohlc_15s
    WITH BASE 'trades' REFRESH IMMEDIATE AS (
        SELECT
            time,
            symbol,
            symbol_period,
            first(open) AS open,
            max(high) AS high,
            min(low) AS low,
            last(close) AS close,
            sum(volume) AS volume,
            sum(number_of_trades) AS number_of_trades,
            sum(bid_volume) AS bid_volume,
            sum(ask_volume) AS ask_volume,
            front_contract
        FROM trades
        SAMPLE BY 15s
    ) PARTITION BY DAY;
    ```

    You can run the script using:
    ```bash
    python lib/qdb_create_materialized_views.py
    ```

    This will create the materialized view in QuestDB, which can then be queried for aggregated OHLC data.