import os
import csv
import shutil
import glob
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_merge.log", mode='a'),
        logging.StreamHandler()
    ]
)

# Define paths
SOURCE_DIR = Path("saved")
TARGET_DIR = Path("data")
SOURCE_OHLCV_DIR = SOURCE_DIR / "ohlcv_data"
TARGET_OHLCV_DIR = TARGET_DIR / "ohlcv_data"

def ensure_directory(directory):
    """Ensure a directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")

def merge_csv_files(source_file, target_file, key_columns=None):
    """
    Merge two CSV files, removing duplicates based on key columns.
    
    Args:
        source_file: Path to the source CSV file
        target_file: Path to the target CSV file
        key_columns: List of column names to use as keys for identifying duplicates
    
    Returns:
        None
    """
    if not os.path.exists(source_file):
        logging.warning(f"Source file does not exist: {source_file}")
        return False

    # If target doesn't exist, just copy the source file
    if not os.path.exists(target_file):
        ensure_directory(os.path.dirname(target_file))
        shutil.copy2(source_file, target_file)
        logging.info(f"Copied {source_file} to {target_file} (target did not exist)")
        return True
    
    try:
        # Read both files
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)
        
        initial_source_rows = len(source_df)
        initial_target_rows = len(target_df)
        
        logging.info(f"Source file {source_file} has {initial_source_rows} rows")
        logging.info(f"Target file {target_file} has {initial_target_rows} rows")
        
        # Combine the dataframes
        combined_df = pd.concat([target_df, source_df])
        
        # Remove duplicates if key columns are provided
        if key_columns:
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=key_columns, keep='first')
            after_dedup = len(combined_df)
            logging.info(f"Removed {before_dedup - after_dedup} duplicate rows based on columns: {key_columns}")
        else:
            before_dedup = len(combined_df)
            combined_df = combined_df.drop_duplicates(keep='first')
            after_dedup = len(combined_df)
            logging.info(f"Removed {before_dedup - after_dedup} duplicate rows")
        
        # Save the combined dataframe
        combined_df.to_csv(target_file, index=False)
        
        logging.info(f"Merged {source_file} into {target_file}, resulting in {len(combined_df)} rows")
        return True
    
    except Exception as e:
        logging.error(f"Error merging {source_file} into {target_file}: {str(e)}")
        return False

def merge_ohlcv_data():
    """
    Merge OHLCV data files (CSV only) from source to target directory.
    If a file already exists in the target, it won't be overwritten.
    """
    # Ensure target directory exists
    ensure_directory(TARGET_OHLCV_DIR)
    
    # Get list of all CSV files in source directory
    source_csv_files = glob.glob(str(SOURCE_OHLCV_DIR / "*.csv"))
    
    if not source_csv_files:
        logging.warning(f"No CSV files found in {SOURCE_OHLCV_DIR}")
        return
    
    copied_count = 0
    skipped_count = 0
    
    # Process CSV files
    for source_file in source_csv_files:
        file_name = os.path.basename(source_file)
        target_file = TARGET_OHLCV_DIR / file_name
        
        # If target file doesn't exist, copy it
        if not os.path.exists(target_file):
            shutil.copy2(source_file, target_file)
            copied_count += 1
            logging.info(f"Copied {source_file} to {target_file}")
        else:
            skipped_count += 1
    
    logging.info(f"OHLCV data merge: Copied {copied_count} new files, skipped {skipped_count} existing files")

def main():
    """Main function to merge all data."""
    try:
        # Ensure log directory exists
        ensure_directory("logs")
        
        # Ensure target directories exist
        ensure_directory(TARGET_DIR)
        ensure_directory(TARGET_OHLCV_DIR)
        
        logging.info("Starting data merge process")
        
        # First, merge OHLCV data
        if os.path.exists(SOURCE_OHLCV_DIR):
            merge_ohlcv_data()
        else:
            logging.warning(f"OHLCV data source directory not found: {SOURCE_OHLCV_DIR}")
        
        # Merge transaction_log.csv
        transaction_log_source = SOURCE_DIR / "transaction_log.csv"
        transaction_log_target = TARGET_DIR / "transaction_log.csv"
        
        if os.path.exists(transaction_log_source):
            # For transaction log, we'll deduplicate based on transaction ID or similar unique field
            merge_csv_files(
                transaction_log_source,
                transaction_log_target,
                key_columns=None  # Use all columns to identify duplicates
            )
        else:
            logging.warning(f"Transaction log source file not found: {transaction_log_source}")
        
        # Merge token_summaries.csv if it exists in source
        token_summaries_source = SOURCE_DIR / "token_summaries.csv"
        token_summaries_target = TARGET_DIR / "token_summaries.csv"
        
        if os.path.exists(token_summaries_source):
            # For token summaries, deduplicate based on mint address
            merge_csv_files(
                token_summaries_source,
                token_summaries_target,
                key_columns=["mint_address"]
            )
        else:
            logging.info(f"No token_summaries.csv found in {SOURCE_DIR}, skipping merge for this file")
        
        logging.info("Data merge process completed successfully")
        
    except Exception as e:
        logging.error(f"Error during data merge process: {str(e)}")

if __name__ == "__main__":
    main() 