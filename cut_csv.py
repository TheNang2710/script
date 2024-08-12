# Author    : Nang Nguyen
# Version   : 1.2
# Date      : 2024-Aug-11
# Note      : Ask to delete zip files after processing, process large CSV files in chunks, only look for CSV files in the specified directory

import os
import pandas as pd
import math
import zipfile
from tqdm import tqdm

# Ask the user for the directory containing the CSV files
data_folder = input("Enter the absolute directory containing the CSV files: ").strip()

# Check if the directory exists
if not os.path.isdir(data_folder):
    print(f"The directory '{data_folder}' does not exist.")
    exit(1)

# Check for zip files in the directory
zip_files = [f for f in os.listdir(data_folder) if f.endswith('.zip')]

if zip_files:
    unzip_choice = input(f"Found {len(zip_files)} zip file(s). Do you want to unzip them? (yes/no): ").strip().lower()
    
    if unzip_choice == 'yes':
        for item in tqdm(zip_files, desc="Unzipping files"):
            zip_path = os.path.join(data_folder, item)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(data_folder)
            print(f"Unzipped: {item}")

            # Check if the extracted files are all CSVs
            extracted_files = zip_ref.namelist()
            for file in extracted_files:
                if not file.endswith('.csv'):
                    print(f"Warning: The file '{file}' inside the zip archive '{item}' is not a CSV file and will be skipped.")
                else:
                    print(f"Found CSV file '{file}' inside the zip archive '{item}' and it will be processed.")
    else:
        print("Skipping unzipping. Proceeding with existing CSV files.")

# Create the "small" folder if it doesn't exist
small_folder = os.path.join(data_folder, 'small')
os.makedirs(small_folder, exist_ok=True)

# Ask the user for the integer input
divisor = int(input("Enter the integer to divide the rows by: "))

print(f"You entered: {divisor}")
print("="*30)

# Iterate over CSV files only in the specified directory (no subdirectories)
csv_files = [f for f in os.listdir(data_folder) if f.endswith('.csv') and f != 'page_views.csv']

# Process each CSV file with a progress bar and in chunks
for filename in tqdm(csv_files, desc="Processing CSV files"):
    file_path = os.path.join(data_folder, filename)

    chunk_size = 100000  # Number of rows per chunk
    small_num_rows = None
    chunks = []

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        if small_num_rows is None:
            num_rows = len(chunk)
            small_num_rows = math.floor(num_rows / divisor)
            print(f"\nProcessing file: {filename}")
            print(f"Total rows: {num_rows}")
            print(f"Small file rows: {small_num_rows}")

        # Append chunks until we have enough rows
        if len(chunks) * chunk_size < small_num_rows:
            chunks.append(chunk)
        else:
            # Add just enough rows to reach small_num_rows
            rows_needed = small_num_rows - (len(chunks) * chunk_size)
            chunks.append(chunk.iloc[:rows_needed])
            break

    if small_num_rows > 0 and chunks:
        # Concatenate the collected chunks and save the small file
        small_df = pd.concat(chunks)
        base_name, ext = os.path.splitext(filename)
        new_filename = f"{base_name}_{divisor}{ext}"
        small_file_path = os.path.join(small_folder, new_filename)
        small_df.to_csv(small_file_path, index=False)
        print(f"Generated small file: {small_file_path}")
    else:
        print(f"Skipped generating small file for {filename} because the number of rows would be zero.")
    
    print("="*30)

# Ask the user if they want to delete the zip files
if zip_files:
    delete_choice = input(f"Do you want to delete the original zip file(s)? (yes/no): ").strip().lower()

    if delete_choice == 'yes':
        for item in zip_files:
            zip_path = os.path.join(data_folder, item)
            os.remove(zip_path)
            print(f"Deleted: {item}")

print("Task completed.")