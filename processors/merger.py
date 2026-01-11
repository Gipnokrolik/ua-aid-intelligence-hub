import sqlite3
import pandas as pd
import os
import glob

# Path settings
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Now RAW_DIR points to the root of raw data, not a specific folder
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw')
MASTER_DB_PATH = os.path.join(BASE_DIR, 'data', 'master', 'master.db')


def merge_databases():
    print("--- UNIVERSAL MERGE PROCESS STARTED ---")
    print(f"Scanning raw directory: {RAW_DIR}")

    # 1. Create master directory if not exists
    os.makedirs(os.path.dirname(MASTER_DB_PATH), exist_ok=True)

    # 2. Connect to master DB
    conn_master = sqlite3.connect(MASTER_DB_PATH)
    total_rows = 0

    # 3. Iterate through all subdirectories in data/raw
    # Each folder name (e.g., 'come_back_alive') becomes the 'source'
    if not os.path.exists(RAW_DIR):
        print(f"ERROR: Raw directory not found: {RAW_DIR}")
        return

    subfolders = [f.path for f in os.scandir(RAW_DIR) if f.is_dir()]

    if not subfolders:
        print(f"WARNING: No source folders found in {RAW_DIR}")

    for folder_path in subfolders:
        source_name = os.path.basename(folder_path)
        print(f"\nProcessing Source: [{source_name}]")

        # Find all .db files in this source folder
        db_files = glob.glob(os.path.join(folder_path, "*.db"))

        if not db_files:
            print(f"  -> No .db files found in {source_name}, skipping.")
            continue

        for db_file in db_files:
            filename = os.path.basename(db_file)
            # print(f"  -> Merging: {filename}...")

            try:
                # Read from monthly file
                conn_temp = sqlite3.connect(db_file)
                df = pd.read_sql_query("SELECT * FROM donations", conn_temp)
                conn_temp.close()

                if df.empty:
                    print(f"  -> File {filename} is empty, skipping.")
                    continue

                # --- CRITICAL UPDATE: Add Source Column ---
                df['source'] = source_name
                # ------------------------------------------

                # Write to master
                df.to_sql('donations', conn_master, if_exists='append', index=False)

                rows_in_file = len(df)
                total_rows += rows_in_file
                # Optional: print less noise if too many files
                # print(f"     + Added {rows_in_file} rows.")

            except Exception as e:
                print(f"  -> ERROR reading {filename}: {e}")

        print(f"  -> Finished source [{source_name}].")

    # Create indexes for performance (including source now)
    print("\nCreating indexes...")
    conn_master.execute("CREATE INDEX IF NOT EXISTS idx_date ON donations (date)")
    conn_master.execute("CREATE INDEX IF NOT EXISTS idx_source ON donations (source)")

    conn_master.close()
    print("--- FINISHED ---")
    print(f"Master DB created at: {MASTER_DB_PATH}")
    print(f"TOTAL RECORDS IN HUB: {total_rows}")


if __name__ == "__main__":
    merge_databases()