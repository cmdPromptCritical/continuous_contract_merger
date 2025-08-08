"""
This script consolidates futures contract data from multiple daily or monthly CSV/TXT files into a single continuous contract.
It identifies the file with the highest trading volume for each day within a specified date range and combines the data chronologically.
The script optimizes performance by pre-calculating daily volumes and grouping data by date.
It generates a consolidated CSV output and a detailed log file summarizing the consolidation process, including file usage and daily breakdowns.
"""
"""
This script consolidates futures contract data from multiple daily or monthly CSV/TXT files into a single continuous contract.
It identifies the file with the highest trading volume for each day within a specified date range and combines the data chronologically.
The script optimizes performance by pre-calculating daily volumes and grouping data by date.
It generates a consolidated CSV output and a detailed log file summarizing the consolidation process, including file usage and daily breakdowns.
"""

import pandas as pd
import os
import glob
from datetime import datetime, timedelta
from collections import defaultdict
import argparse

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Consolidate futures contract data files')
    parser.add_argument('--input_folder', '-i', type=str, required=True,
                        help='Path to folder containing input text files')
    parser.add_argument('--output_folder', '-o', type=str, default='.',
                        help='Path to output folder (default: current directory)')
    return parser.parse_args()

def load_data_files(input_folder):
    """Load all CSV files from the input folder"""
    file_pattern = '*.csv'
    all_files = glob.glob(os.path.join(input_folder, file_pattern))
    
    # Also check for .txt files in case they're CSV formatted
    txt_files = glob.glob(os.path.join(input_folder, '*.txt'))
    all_files.extend(txt_files)
    
    if not all_files:
        raise ValueError(f"No CSV/TXT files found in {input_folder}")
    
    print(f"Found {len(all_files)} data files:")
    for file in all_files:
        print(f"  - {os.path.basename(file)}")
    
    return all_files

def load_and_parse_file(file_path):
    """Load and parse a single data file with optimizations"""
    try:
        print(f"Loading {os.path.basename(file_path)}...")
        
        # Read CSV with optimized parameters
        df = pd.read_csv(
            file_path,
            sep=',',
            dtype={
                'Open': 'float32',
                'High': 'float32', 
                'Low': 'float32',
                'Last': 'float32',
                'Volume': 'int32',
                'NumberOfTrades': 'int32',
                'BidVolume': 'int32',
                'AskVolume': 'int32'
            },
            parse_dates=['Date']
        )
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        return df
    
    except Exception as e:
        print(f"Error loading {file_path}: {str(e)}")
        return None

def calculate_daily_volumes(loaded_files):
    """Pre-calculate daily volume summaries for all files"""
    print("\nCalculating daily volume summaries...")
    
    daily_volumes = {}  # {file_path: {date: total_volume}}
    
    for file_path, df in loaded_files.items():
        print(f"Processing daily volumes for {os.path.basename(file_path)}...")
        
        # Group by date and sum volume - much faster than filtering each day
        daily_vol = df.groupby(df['Date'].dt.date)['Volume'].sum()
        daily_volumes[file_path] = daily_vol.to_dict()
    
    return daily_volumes

def get_date_range_from_daily_volumes(daily_volumes):
    """Get overall date range from daily volume summaries"""
    all_dates = set()
    
    for file_volumes in daily_volumes.values():
        all_dates.update(file_volumes.keys())
    
    if not all_dates:
        raise ValueError("No dates found in any files")
    
    return min(all_dates), max(all_dates)

def consolidate_data(input_folder, output_folder):
    """Main function to consolidate the data"""
    
    # Load all data files
    data_files = load_data_files(input_folder)
    
    # Load all files into memory once
    print(f"\nLoading all data files into memory...")
    loaded_files = {}
    for file_path in data_files:
        df = load_and_parse_file(file_path)
        if df is not None:
            loaded_files[file_path] = df
        else:
            print(f"Failed to load {file_path}")
    
    print(f"Successfully loaded {len(loaded_files)} files")
    
    # Pre-calculate daily volume summaries
    daily_volumes = calculate_daily_volumes(loaded_files)
    
    # Get date range from daily summaries
    start_date, end_date = get_date_range_from_daily_volumes(daily_volumes)
    print(f"\nDate range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Pre-group data by date for faster filtering
    print("Pre-grouping data by date for faster processing...")
    grouped_data = {}  # {file_path: {date: DataFrame}}
    
    for file_path, df in loaded_files.items():
        print(f"Grouping {os.path.basename(file_path)} by date...")
        grouped = df.groupby(df['Date'].dt.date)
        grouped_data[file_path] = {date: group for date, group in grouped}
    
    # Initialize results
    consolidated_data = []
    log_entries = []
    
    # Process each day using pre-calculated summaries
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    processed_days = 0
    
    while current_date <= end_date:
        processed_days += 1
        date_str = current_date.strftime('%Y-%m-%d')
        
        if processed_days % 10 == 0:  # Progress update every 10 days
            print(f"Processing day {processed_days}/{total_days}: {date_str}")
        
        best_file = None
        best_volume = 0
        best_data = None
        
        # Use pre-calculated daily volumes to find best file
        for file_path in loaded_files.keys():
            volume = daily_volumes[file_path].get(current_date, 0)
            
            if volume > best_volume:
                best_volume = volume
                best_file = file_path
        
        # Get the actual data for the best file/date combination
        if best_file and current_date in grouped_data[best_file]:
            best_data = grouped_data[best_file][current_date].copy()
            
            consolidated_data.append(best_data)
            log_entries.append({
                'date': date_str,
                'file': os.path.basename(best_file),
                'volume': best_volume,
                'trades': len(best_data)
            })
        else:
            log_entries.append({
                'date': date_str,
                'file': 'NO_DATA',
                'volume': 0,
                'trades': 0
            })
        
        current_date += timedelta(days=1)
    
    # Combine all data
    if consolidated_data:
        print(f"\nCombining all daily data...")
        final_df = pd.concat(consolidated_data, ignore_index=True)
        
        # No sorting needed since:
        # 1. We process days sequentially (chronological order)
        # 2. Each source file is already sorted within each day
        # 3. Data is naturally in correct order after concatenation
        
        # Save consolidated data
        current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"continuous_contract_{current_datetime}.csv"
        output_path = os.path.join(output_folder, output_filename)
        
        print(f"Saving consolidated data...")
        # Select only the required columns in the specified order
        output_columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Last', 'Volume', 'NumberOfTrades']
        final_df = final_df[output_columns]
        
        final_df.to_csv(output_path, index=False)
        print(f"Consolidated data saved to: {output_path}")
        print(f"Total records: {len(final_df):,}")
        
        # Save log file
        log_filename = f"continuous_contract_log_{current_datetime}.txt"
        log_path = os.path.join(output_folder, log_filename)
        
        with open(log_path, 'w') as f:
            f.write(f"Futures Contract Data Consolidation Log\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Input folder: {input_folder}\n")
            f.write(f"Start date: {start_date.strftime('%Y-%m-%d')}\n")
            f.write(f"End date: {end_date.strftime('%Y-%m-%d')}\n")
            f.write(f"Total records: {len(final_df):,}\n")
            f.write(f"Files processed: {len(loaded_files)}\n\n")
            
            # Summary statistics
            file_usage = {}
            total_volume = 0
            total_trades = 0
            
            for entry in log_entries:
                if entry['file'] != 'NO_DATA':
                    file_usage[entry['file']] = file_usage.get(entry['file'], 0) + 1
                    total_volume += entry['volume']
                    total_trades += entry['trades']
            
            f.write("File Usage Summary:\n")
            f.write("-" * 30 + "\n")
            for file, days in sorted(file_usage.items()):
                f.write(f"{file}: {days} days\n")
            
            f.write(f"\nTotal Volume: {total_volume:,}\n")
            f.write(f"Total Trades: {total_trades:,}\n\n")
            
            f.write("Daily breakdown:\n")
            f.write("Date\t\tFile Used\t\tVolume\t\tTrades\n")
            f.write("-" * 70 + "\n")
            
            for entry in log_entries:
                f.write(f"{entry['date']}\t{entry['file']:<25}\t{entry['volume']:<12,}\t{entry['trades']:,}\n")
        
        print(f"Log file saved to: {log_path}")
        return output_path, log_path
    
    else:
        print("No data found to consolidate!")
        return None, None

def main():
    """Main entry point"""
    try:
        args = parse_arguments()
        
        if not os.path.exists(args.input_folder):
            print(f"Error: Input folder '{args.input_folder}' does not exist")
            return
        
        if not os.path.exists(args.output_folder):
            os.makedirs(args.output_folder)
        
        print(f"Consolidating futures contract data...")
        print(f"Input folder: {args.input_folder}")
        print(f"Output folder: {args.output_folder}")
        
        start_time = datetime.now()
        output_file, log_file = consolidate_data(args.input_folder, args.output_folder)
        end_time = datetime.now()
        
        if output_file:
            print(f"\nConsolidation completed successfully!")
            print(f"Total processing time: {end_time - start_time}")
            print(f"Output file: {output_file}")
            print(f"Log file: {log_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
