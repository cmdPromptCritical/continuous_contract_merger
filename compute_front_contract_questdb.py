"""
QuestDB Front Contract Computation Script

This script determines the front contract of futures contract series in QuestDB by
identifying and marking the primary contract for each symbol on each trading day.
For each symbol and date, it sets the 'front_contract' flag to True for the
symbol_period with the highest total volume (typically the front contract), and
False for all others in the series.

Usage:
  python compute_front_contract_questdb.py [--date YYYY-MM-DD] [--symbol SYMBOL]

Arguments:
  --date    Start date in YYYY-MM-DD format. If not provided, will prompt for input
            or start from the earliest date in the database.
  --symbol  Symbol to start processing from. If not provided, will prompt for input
            or process all symbols. Will process this symbol and all subsequent symbols.

Examples:
  python compute_front_contract_questdb.py
  python compute_front_contract_questdb.py --date 2024-01-15
  python compute_front_contract_questdb.py --symbol ESM4
  python compute_front_contract_questdb.py --date 2024-01-15 --symbol ESM4

Requirements:
  - QuestDB running and accessible
  - Environment variables set in .env file (see .env.example)
  - 'trades' table with columns: symbol, time, volume, symbol_period
"""

import os
import sys
import argparse
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    """Establishes a connection to the QuestDB database."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("QUESTDB_HOST", "localhost"),
            port=os.getenv("QUESTDB_PG_PORT", "8812"),
            user=os.getenv("QUESTDB_USER", "admin"),
            password=os.getenv("QUESTDB_PASSWORD", "quest"),
            dbname="qdb"
        )
        print("Successfully connected to QuestDB.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to QuestDB: {e}", file=sys.stderr)
        sys.exit(1)

def get_symbols(cursor):
    """Fetches all distinct symbols from the trades table."""
    try:
        cursor.execute("SELECT DISTINCT symbol FROM trades;")
        symbols = [row[0] for row in cursor.fetchall()]
        print(f"Found symbols: {symbols}")
        return symbols
    except psycopg2.Error as e:
        print(f"Error fetching symbols: {e}", file=sys.stderr)
        return []

def get_start_date(cursor):
    """Fetches the earliest date from the trades table."""
    try:
        cursor.execute("SELECT min(time) FROM trades;")
        result = cursor.fetchone()
        start_date = result[0] if result and result[0] else datetime.now()
        print(f"Earliest record in database is from: {start_date.strftime('%Y-%m-%d')}")
        return start_date
    except psycopg2.Error as e:
        print(f"Error fetching start date: {e}", file=sys.stderr)
        return datetime.now()

def ensure_front_contract_column_exists(cursor):
    """Ensures the 'front_contract' column exists in the 'trades' table."""
    try:
        # Check if the column exists
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'trades' AND column_name = 'front_contract';
        """)
        result = cursor.fetchone()

        if not result:
            # Column doesn't exist, add it
            cursor.execute("""
                ALTER TABLE trades ADD COLUMN front_contract BOOLEAN DEFAULT FALSE;
            """)
            print("Added 'front_contract' column to 'trades' table.")
        else:
            print("'front_contract' column already exists in 'trades' table.")
    except psycopg2.Error as e:
        print(f"Error ensuring 'front_contract' column exists: {e}", file=sys.stderr)
        cursor.connection.rollback()

def deduplicate_data(cursor, symbol, date_str):
    """
    For a given symbol and day, sets front_contract to True for the symbol_period with the highest volume
    and False for the others.
    """
    try:
        # Get all symbol_periods for the given symbol and day, ordered by total volume
        cursor.execute("""
            SELECT symbol_period, sum(volume) as total_volume
            FROM trades
            WHERE symbol = %s AND to_str(time, 'yyyy-MM-dd') = %s
            GROUP BY symbol_period
            ORDER BY total_volume DESC;
        """, (symbol, date_str))

        results = cursor.fetchall()

        if len(results) <= 1:
            print(f"  - No overlapping data for symbol '{symbol}' on {date_str}. Skipping.")
            return

        # The first result is the one to set as front contract
        symbol_period_to_keep = results[0][0]
        print(f"  - Setting front_contract=True for symbol_period '{symbol_period_to_keep}' with volume {results[0][1]}.")

        # First, set all front_contract to False for this symbol and day
        cursor.execute("""
            UPDATE trades
            SET front_contract = FALSE
            WHERE symbol = %s AND to_str(time, 'yyyy-MM-dd') = %s;
        """, (symbol, date_str))

        # Then, set front_contract to True for the highest volume symbol_period
        cursor.execute("""
            UPDATE trades
            SET front_contract = TRUE
            WHERE symbol = %s AND to_str(time, 'yyyy-MM-dd') = %s AND symbol_period = %s;
        """, (symbol, date_str, symbol_period_to_keep))

        # Get all other symbol_periods to set as not front contract
        symbol_periods_to_set_false = [row[0] for row in results[1:]]

        for sp_to_set_false in symbol_periods_to_set_false:
            print(f"  - Setting front_contract=False for symbol_period '{sp_to_set_false}'.")

    except psycopg2.Error as e:
        print(f"  - Error processing symbol '{symbol}' on {date_str}: {e}", file=sys.stderr)
        cursor.connection.rollback()


def main():
    """Main function to run the deduplication process."""
    parser = argparse.ArgumentParser(
        description="Deduplicate trading data in QuestDB by marking front contracts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --date 2024-01-15
  %(prog)s --symbol AAPL
  %(prog)s --date 2024-01-15 --symbol AAPL
        """
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Start date in YYYY-MM-DD format. If not provided, will prompt for input or start from the earliest date in the database."
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Symbol to start processing from. If not provided, will prompt for input or process all symbols."
    )

    args = parser.parse_args()

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Ensure the front_contract column exists
            ensure_front_contract_column_exists(cursor)
            conn.commit()

            # Get start date from command line argument or prompt
            start_date = None
            if args.date:
                try:
                    start_date = datetime.strptime(args.date, "%Y-%m-%d")
                except ValueError:
                    print(f"Invalid date format: {args.date}. Please use YYYY-MM-DD.", file=sys.stderr)
                    return
            else:
                user_start_date_str = input(f"Enter the start date (YYYY-MM-DD), or press Enter to start from the beginning: ")
                if user_start_date_str:
                    try:
                        start_date = datetime.strptime(user_start_date_str, "%Y-%m-%d")
                    except ValueError:
                        print("Invalid date format. Please use YYYY-MM-DD.", file=sys.stderr)
                        return
                else:
                    start_date = get_start_date(cursor)

            # Get start symbol from command line argument or prompt
            user_start_symbol = args.symbol.upper() if args.symbol else None
            if not user_start_symbol:
                user_start_symbol = input("Enter the symbol to start with, or press Enter for all symbols: ").upper()

            all_symbols = get_symbols(cursor)
            if not all_symbols:
                print("No symbols found in the database.")
                return

            symbols_to_process = all_symbols
            if user_start_symbol:
                if user_start_symbol in all_symbols:
                    # Process from the specified symbol onwards
                    start_index = all_symbols.index(user_start_symbol)
                    symbols_to_process = all_symbols[start_index:]
                else:
                    print(f"Symbol '{user_start_symbol}' not found. Processing all symbols.")

            end_date = datetime.now()
            current_date = start_date

            print(f"\nStarting deduplication from {current_date.strftime('%Y-%m-%d')} for symbols: {symbols_to_process}")

            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                print(f"\nProcessing date: {date_str}")

                for symbol in symbols_to_process:
                    print(f"  Processing symbol: '{symbol}'")
                    deduplicate_data(cursor, symbol, date_str)

                # Commit changes for the current day
                conn.commit()
                print(f"\nCommitted changes for {date_str}")

                current_date += timedelta(days=1)
                # After the first day, all symbols should be processed
                symbols_to_process = all_symbols


            print("\nDeduplication process completed.")

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
