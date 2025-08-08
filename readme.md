# Continuous Futures Contract Data Management

This repository contains Python scripts for managing and processing futures contract data, specifically for creating continuous contract data from individual contract files.

## Purpose

The main goal of this project is to take separate tick data files from a futures contract series and merge them into one continuous contract. This is essential for historical analysis and backtesting trading strategies.

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

### `dedupcontract.py`

This script is intended to handle the deduplication of contract data, which is a crucial step in creating a clean, continuous contract series. (Note: The implementation details of this script are not fully elaborated in the provided file.)

## Workflow

1.  **Data Extraction:** Use `scid_to_qdb.py` or `scid_to_pg.py` to extract data from individual `.scid` contract files.
2.  **Database Storage:** The extracted data is stored in either a QuestDB or PostgreSQL database.
3.  **Deduplication:** The `dedupcontract.py` script can be used to process the data in the database to remove duplicate entries, which can occur around contract rollover periods.

## TODO

-   Flesh out the implementation of `dedupcontract.py` to perform the following:
    -   Drop days where the trading volume is significantly lower than the recent median volume (e.g., 15% of the 100-day median). This helps to filter out periods of low liquidity, such as the beginning of a new contract or Sundays.
