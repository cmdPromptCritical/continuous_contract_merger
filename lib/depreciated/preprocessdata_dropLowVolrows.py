# Purpose: to drop days in the dataset where there is low volume. Low volume tends to equal days where there is a holiday OR
#          a day where the front contract is something other than the contract being actively processed by the script.
# TODO: integrate into the main script to automatically apply prior to exporting
# - test code: current script not tested.
# 
import pandas as pd

# Specify the path to your CSV file
file_path = 'continuous_contract_20250616_223400.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(file_path)

# Ensure column 'A' is numeric
df['Volume'] = pd.to_numeric(df['Open'], errors='coerce')

# Compute the average value of the last 50,000 rows of column 'A'
average_value_last_50k = df['Open'].tail(50000).mean()

# Drop the rows whose column 'A' value is less than the computed average
filtered_df = df[df['Open'] >= average_value_last_50k]

# Optionally, save the filtered DataFrame to a new CSV file
filtered_df.to_csv('filtered_file.csv', index=False)

print(f"Average value of the last 50,000 rows of column 'A': {average_value_last_50k}")
