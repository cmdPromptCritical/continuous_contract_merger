"""
This script consolidates futures contract data from multiple input files into a single continuous contract.
It processes data files (TXT, CSV, DAT) from a specified input folder, identifies the overall date range,
and for each day, selects the data from the file with the highest trading volume.
The consolidated data is then saved to a new CSV file, and a detailed log file is generated,
documenting the daily selection process and overall consolidation summary.
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
    """Load all text files from the input folder"""
    # Look for common text file extensions
    file_patterns = ['*.txt', '*.csv', '*.dat']
    all_files = []
    
    for pattern in file_patterns:
        files = glob.glob(os.path.join(input_folder, pattern))
        all_files.extend(files)
    
    if not all_files:
        raise ValueError(f"No data files found in {input_folder}")
    
    print(f"Found {len(all_files)} data files:")
    for file in all_files:
        print(f"  - {os.path.basename(file)}")
    
    return all_files

def load_and_parse_file(file_path):
    """Load and parse a single data file"""
    try:
        df = pd.read_csv(file_path, skip_blank_lines=True)
        # Check if we have the expected columns
        #expected_cols = ['Date', 'Time', 'Open', 'High', 'Low', 'Last', 'Volume', 'NumberOfTrades', 'BidVolume', 'AskVolume']
        df.columns = df.columns.str.strip()

        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Ensure Volume is numeric
        #df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
        
        return df
    
    except Exception as e:
        print(f"Error loading {file_path}: {str(e)}")
        return None

# Removed get_date_range function as it's now integrated into consolidate_data

def consolidate_data(input_folder, output_folder):
    """Main function to consolidate the data"""
    
    # Load all data files
    data_files = load_data_files(input_folder)
    
    # Load all files into memory once
    print(f"\nLoading all data files into memory...")
    loaded_files = {}
    for file_path in data_files:
        print(f"Loading {os.path.basename(file_path)}...")
        df = load_and_parse_file(file_path)
        if df is not None:
            loaded_files[file_path] = df
        else:
            print(f"Failed to load {file_path}")
    
    print(f"Successfully loaded {len(loaded_files)} files")
    
    # Get overall date range from loaded files
    start_date = None
    end_date = None
    for df in loaded_files.values():
        if not df.empty:
            file_min = df['Date'].min()
            file_max = df['Date'].max()
            
            if start_date is None or file_min < start_date:
                start_date = file_min
            if end_date is None or file_max > end_date:
                end_date = file_max
    
    if start_date is not None and end_date is not None:
        print(f"\nDate range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    else:
        print("\nDate range: Not available (no valid dates found in files)")
    
    # Initialize results
    consolidated_data = []
    log_entries = []
    
    # Process each day
    current_date = start_date
    if start_date is not None and end_date is not None and current_date is not None:
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"Processing {date_str}...")
    else:
        print("No valid date range found. Skipping daily processing.")
        return None, None
        
        best_file = None
        best_volume = 0
        best_data = None
        
        # Check each loaded file for this date
        for file_path, df in loaded_files.items():
            # Filter data for current date
            day_data = df[df['Date'].dt.date == current_date.date()]
            
            if not day_data.empty:
                # Calculate total volume for this day
                total_volume = day_data['Volume'].sum()
                
                if total_volume > best_volume:
                    best_volume = total_volume
                    best_file = file_path
                    best_data = day_data.copy()
        
        # Add the best data for this day
        if best_data is not None:
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
        final_df = pd.concat(consolidated_data, ignore_index=True)
        final_df = final_df.sort_values(['Date', 'Time'])
        
        # Save consolidated data
        current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"continuous_contract_{current_datetime}.csv"
        output_path = os.path.join(output_folder, output_filename)
        
        final_df.to_csv(output_path, index=False)
        print(f"\nConsolidated data saved to: {output_path}")
        print(f"Total records: {len(final_df)}")
        
        # Save log file
        log_filename = f"continuous_contract_log_{current_datetime}.txt"
        log_path = os.path.join(output_folder, log_filename)
        
        with open(log_path, 'w') as f:
            f.write(f"Futures Contract Data Consolidation Log\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Input folder: {input_folder}\n")
            f.write(f"Start date: {start_date.strftime('%Y-%m-%d')}\n")
            f.write(f"End date: {end_date.strftime('%Y-%m-%d')}\n")
            f.write(f"Total records: {len(final_df)}\n\n")
            
            f.write("Daily breakdown:\n")
            f.write("Date\t\tFile Used\t\tVolume\t\tTrades\n")
            f.write("-" * 60 + "\n")
            
            for entry in log_entries:
                f.write(f"{entry['date']}\t{entry['file']:<20}\t{entry['volume']:<10}\t{entry['trades']}\n")
        
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
        
        output_file, log_file = consolidate_data(args.input_folder, args.output_folder)
        
        if output_file:
            print(f"\nConsolidation completed successfully!")
            print(f"Output file: {output_file}")
            print(f"Log file: {log_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()