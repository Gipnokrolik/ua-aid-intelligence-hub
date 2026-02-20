import sqlite3
import pandas as pd
import os
import glob
import logging

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Path configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw')
MASTER_DB_PATH = os.path.join(BASE_DIR, 'data', 'master', 'master.db')


def merge_specific_foundation(folder_name):
    """
    Merges databases from a specific subdirectory in data/raw/ into master.db
    """
    target_folder = os.path.join(RAW_DIR, folder_name)

    if not os.path.exists(target_folder):
        logging.error(f"Directory not found: {target_folder}")
        return

    logging.info(f"--- STARTING MERGE FOR FOUNDATION: {folder_name} ---")

    # Ensure master directory exists
    os.makedirs(os.path.dirname(MASTER_DB_PATH), exist_ok=True)

    # Connect to master database
    conn_master = sqlite3.connect(MASTER_DB_PATH)
    total_rows = 0

    # Find all .db files in the target folder
    db_files = glob.glob(os.path.join(target_folder, "*.db"))

    if not db_files:
        logging.warning(f"No .db files found in {folder_name}. Skipping.")
        conn_master.close()
        return

    for db_file in db_files:
        filename = os.path.basename(db_file)
        logging.info(f"Processing file: {filename}")

        try:
            conn_temp = sqlite3.connect(db_file)
            # Read source table
            df = pd.read_sql_query("SELECT * FROM donations", conn_temp)
            conn_temp.close()

            if df.empty:
                logging.info(f"File {filename} is empty. Skipping.")
                continue

            # Tag data with foundation name
            df['foundation_name'] = folder_name

            # Append to master table
            df.to_sql('donations', conn_master, if_exists='append', index=False)

            rows_in_file = len(df)
            total_rows += rows_in_file
            logging.info(f"Successfully added {rows_in_file} rows.")

        except Exception as e:
            logging.error(f"Error reading {filename}: {e}")

    # Performance optimization: creating indexes
    logging.info("Optimizing master database indexes...")
    try:
        conn_master.execute("CREATE INDEX IF NOT EXISTS idx_date ON donations (date)")
        conn_master.execute("CREATE INDEX IF NOT EXISTS idx_foundation_name ON donations (foundation_name)")
        # Critical index for Superset VIEW performance
        conn_master.execute("CREATE INDEX IF NOT EXISTS idx_date_short ON donations (SUBSTR(date, 1, 10))")
        logging.info("Indexes updated successfully.")
    except Exception as e:
        logging.error(f"Failed to create indexes: {e}")

    conn_master.close()
    logging.info(f"--- FINISHED: {folder_name} ---")
    logging.info(f"Total new records added: {total_rows}")


if __name__ == "__main__":
    # Target folder name in data/raw/
    TARGET = 'come_back_alive'
    merge_specific_foundation(TARGET)