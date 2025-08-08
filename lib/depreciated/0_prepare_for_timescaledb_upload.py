"""
0_prepare_for_timescaledb_upload.py

This script is the first step in the data ingestion pipeline. It takes raw CSV
trading data, processes it to a standardized format, and prepares it for
upload into a PostgreSQL/TimescaleDB database.

The processing steps include:
1. Renaming columns to a consistent schema (e.g., 'Date' to 'trade_date').
2. Standardizing time formats and combining date/time into a single 'time' column.
3. Adding 'symbol' and 'symbol_period' columns (currently hardcoded to 'MES' and 'H5').
4. Reordering columns for database compatibility.
5. Optionally saving the processed data to a new CSV file (e.g., original_file_processed.csv).

This processed CSV file is then intended to be used by '1_upload_to_postgres.py'
for efficient bulk insertion into the 'market_data' table.

Usage:
    python3 0_prepare_for_timescaledb_upload.py

The script will prompt the user to enter the path to the raw CSV file.

Example:
    python3 0_prepare_for_timescaledb_upload.py
    (Then, when prompted, enter: C:\path\to\your\raw_data.csv)
"""

import pandas as pd
from datetime import datetime
import os

def process_trading_data(csv_file_path):
    """
    Load and process trading data CSV file according to specified requirements.
    
    Args:
        csv_file_path (str): Path to the input CSV file
    """
    try:
        # Load the CSV file
        print(f"Loading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        # Display original columns
        print(f"Original columns: {list(df.columns)}")
        print(f"Original data shape: {df.shape}")

        # Rename columns
        column_mapping = {
            'Date': 'trade_date',
            ' Time': 'trade_time', 
            ' Open': 'open',
            ' High': 'high',
            ' Low': 'low',
            ' Last': 'close',
            ' Volume': 'volume',
            ' NumberOfTrades': 'number_of_trades',
            ' BidVolume': 'bid_volume',
            ' AskVolume': 'ask_volume'
        }
        
        df = df.rename(columns=column_mapping)
        print("Renamed columns according to mapping")
        
        # append .000 where the string length in ' Time' column is less than 10 characters
        if 'trade_time' in df.columns:
            df['trade_time'] = df['trade_time'].apply(lambda x: x + '.000' if len(x) < 11 else x)
            print("Appended '.000' to 'trade_time' where necessary")


        # Convert trade_date to string (it's likely already a string, but ensuring consistency)
        df['trade_date'] = df['trade_date'].astype(str)
        

        # Create datetime column by combining trade_date and trade_time
        df['time'] = pd.to_datetime(df['trade_date'] + ' ' + df['trade_time'].astype(str))
        print("Created 'time' column by combining trade_date and trade_time into datetime")
        
        # Convert trade_time to PostgreSQL-compatible time format
        # Assuming the time format is HH:MM:SS.fff
        df['trade_time'] = pd.to_datetime(df['trade_time'], format=' %H:%M:%S.%f').dt.time
        print("Converted trade_time to time format")
        
        # Add new columns
        df['symbol'] = 'MES'
        df['symbol_period'] = 'H5'
        print("Added 'symbol' and 'symbol_period' columns")
        
        # Reorder columns for better readability
        column_order = ['trade_date', 'trade_time', 'time', 'symbol', 'symbol_period', 
                       'open', 'high', 'low', 'close', 'volume', 'number_of_trades', 'bid_volume', 'ask_volume']
        
        df = df[column_order]
        
        # Display the first 10 rows
        print("\n" + "="*80)
        print("PROCESSED DATA - First 10 rows:")
        print("="*80)
        print(df.head(10).to_string(index=False))
        print("="*80)
        
        # Show data types
        print("\nData types:")
        print(df.dtypes)
        
        # Ask user if they want to save to CSV
        save_response = input("\nDo you want to save this processed data to a CSV file? (y/n): ").lower().strip()
        
        if save_response in ['y', 'yes']:
            # Generate output filename
            base_name = os.path.splitext(csv_file_path)[0]
            output_file = f"{base_name}_processed.csv"
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            print(f"Data saved to: {output_file}")
            
            # Display summary
            print(f"\nSummary:")
            print(f"- Input file: {csv_file_path}")
            print(f"- Output file: {output_file}")
            print(f"- Rows processed: {len(df)}")
            print(f"- Columns: {len(df.columns)}")
            
        else:
            print("Data not saved.")
            
        return df
        
    except FileNotFoundError:
        print(f"Error: File '{csv_file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None

def main():
    """Main function to run the script"""
    # Get CSV file path from user
    csv_file = input("Enter the path to your CSV file: ").strip()
    
    # Remove quotes if user wrapped the path in quotes
    if csv_file.startswith('"') and csv_file.endswith('"'):
        csv_file = csv_file[1:-1]
    elif csv_file.startswith("'") and csv_file.endswith("'"):
        csv_file = csv_file[1:-1]
    
    # Process the data
    processed_df = process_trading_data(csv_file)
    
    if processed_df is not None:
        print("\nProcessing completed successfully!")
    else:
        print("\nProcessing failed!")

if __name__ == "__main__":
    main()

# sql command to update number_of_trades column in all records to 1:
# UPDATE market_data SET number_of_trades = 1;
