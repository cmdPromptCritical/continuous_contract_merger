"""
2_deleteLowVolSourcesByDay.py

This script processes daily trade data for a specified symbol within a date range.
It sums volume per symbol_period, reports volume by source to the terminal,
and can optionally delete trades from the lowest-volume source if there are
at least two sources present for that day.

Usage:
    python3 2_deleteLowVolSourcesByDay.py --start-date YYYY-MM-DD --symbol SYMBOL [OPTIONS]

Arguments:
    --symbol        : Symbol to process (e.g., 'SPY'). (Required)
    --start-date    : (optional) First date to process (YYYY-MM-DD). Will default to the earliest date for the symbol in the database.
    --end-date      : (optional) Last date to process (YYYY-MM-DD). Defaults to yesterday.
    --delete        : (optional) Delete trades belonging to the lowest-volume source each day.
                      Deletion only occurs if there are at least two sources for the day. Defaults to False.

Example:
    python3 2_deleteLowVolSourcesByDay.py --start-date 2023-01-01 --symbol SPY --delete

"""

import os
import sys
import argparse
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

def parse_args():
    p = argparse.ArgumentParser(
        description="Track daily volumes by source and delete the lowest-volume source's trades."
    )
    p.add_argument("--start-date",
                   help="First date to process (YYYY-MM-DD). Defaults to earliest date for symbol in DB.")
    p.add_argument("--end-date",
                   help="Last date to process (YYYY-MM-DD). Defaults to yesterday.")
    p.add_argument("--symbol",
                   help="Symbol to process (e.g., 'SPY'). If not provided, all symbols in the database will be processed.")
    p.add_argument("--delete", action="store_true",
                   help="Delete trades belonging to the lowest-volume source each day.")
    p.add_argument("--dry-run", action="store_true",
                   help="Show operations without performing inserts/deletes.")
    # DB connection params
    p.add_argument("--db-name", default=os.getenv("DB_DATABASE"),
                   help="Database name (env: DB_DATABASE)")
    p.add_argument("--db-user", default=os.getenv("DB_USER"),
                   help="Database user (env: DB_USER)")
    p.add_argument("--db-password", default=os.getenv("DB_PASSWORD"),
                   help="Database password (env: DB_PASSWORD)")
    p.add_argument("--db-host", default=os.getenv("DB_HOST"),
                   help="Database host (env: DB_HOST).")
    p.add_argument("--db-port", default=os.getenv("DB_PORT"),
                   help="Database port (env: DB_PORT).")
    return p.parse_args()


def connect_db(args):
    conn = psycopg2.connect(
        # get parameters from either args or environment variables
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        host=args.db_host,
        port=args.db_port
    )
    conn.autocommit = False
    return conn


def ensure_index_and_table(conn):
    # disabled.
    return
    # with conn.cursor() as cur:
    #     cur.execute("""
    #         CREATE INDEX IF NOT EXISTS idx_trades_date_source
    #         ON market_data(date, symbol_period);
    #     """)
    #     if track:
    #         cur.execute("""
    #             CREATE TABLE IF NOT EXISTS daily_source_volume (
    #                 date        DATE        NOT NULL,
    #                 symbol_period TEXT      NOT NULL,
    #                 total_volume BIGINT     NOT NULL,
    #                 PRIMARY KEY (date, symbol_period)
    #             );
    #         """)
    # conn.commit()


def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def get_earliest_date_for_symbol(conn, symbol: str) -> date | None:
    print(f"Determining earliest date for symbol {symbol}. This might take a few minutes depending on size of dataset...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MIN(time_bucket)
            FROM one_day_candle
            WHERE symbol = %s;
        """, (symbol,))
        result = cur.fetchone()
        if result and result[0]:
            return result[0]
        return None


def get_all_symbols(conn) -> list[str]:
    print("Querying database for all available symbols...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT symbol
            FROM market_data
            ORDER BY symbol;
        """)
        return [row[0] for row in cur.fetchall()]


def process_day(conn, day: date, symbol: str, delete: bool = False, dry_run: bool = False):
    day_str = day.isoformat()
    with conn.cursor() as cur:
        # 1) Sum volumes per source
        cur.execute("""
            SELECT symbol_period, SUM(volume) as total_volume
              FROM market_data
             WHERE trade_date = %s AND symbol = %s
             GROUP BY symbol_period
             ORDER BY symbol_period;
        """, (day_str, symbol))
        rows = cur.fetchall()
        if not rows:
            print(f"[{day_str}] No trades found for symbol {symbol}.")
            return

        # Log volumes
        print(f"[{day_str}] Volume by source for {symbol}:")
        for src, vol in rows:
            print(f"    {src:10s} → {vol}")

        # 3) Find minimum-volume source(s)
        min_vol = min(vol for _, vol in rows)
        min_sources = [src for src, vol in rows if vol == min_vol]
        print(f"    Lowest volume = {min_vol} from source(s): {', '.join(min_sources)}")

        # 4) Delete trades if requested
        if delete and len(rows) >= 2:
            placeholders = ", ".join(["%s"] * len(min_sources))
            del_sql = f"""
                DELETE FROM market_data
                 WHERE trade_date = %s AND symbol = %s
                   AND symbol_period IN ({placeholders});
            """
            params = [day_str, symbol] + min_sources
            print(f"    → Deleting trades for {len(min_sources)} source(s)...")
            if not dry_run:
                cur.execute(del_sql, params)
                deleted = cur.rowcount
                conn.commit()
                print(f"    → Deleted {deleted} rows.")
            else:
                print("    → Dry run: no rows deleted.")


def main():
    args = parse_args()

    conn = connect_db(args)
    ensure_index_and_table(conn)

    symbols_to_process = []
    if args.symbol:
        symbols_to_process.append(args.symbol)
    else:
        symbols_to_process = get_all_symbols(conn)
        if not symbols_to_process:
            sys.exit("Error: No symbols found in the database to process.")

    # Determine end date once
    if args.end_date:
        try:
            end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            sys.exit("Error: --end-date must be YYYY-MM-DD")
    else:
        end = date.today() - timedelta(days=1)

    for current_symbol in symbols_to_process:
        print(f"\n--- Processing symbol: {current_symbol} ---")
        # Determine start date for the current symbol
        if args.start_date:
            try:
                start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            except ValueError:
                sys.exit("Error: --start-date must be YYYY-MM-DD")
        else:
            start = get_earliest_date_for_symbol(conn, current_symbol)
            if not start:
                print(f"Warning: No data found for symbol {current_symbol} to determine start date. Skipping.")
                continue # Skip to next symbol if no data
            print(f"Using earliest date for {current_symbol}: {start.isoformat()}")

        if start > end:
            print(f"Warning: Start date {start} is after end date {end} for symbol {current_symbol}. Skipping.")
            continue # Skip to next symbol if start date is after end date

        for day in date_range(start, end):
            try:
                process_day(conn, day, current_symbol, args.delete, args.dry_run)
            except Exception as e:
                conn.rollback()
                print(f"[{day.isoformat()}] ERROR for {current_symbol}: {e}")
    conn.close()


if __name__ == "__main__":
    main()
