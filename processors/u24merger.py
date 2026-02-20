import sqlite3
import pandas as pd
import os
import logging

# Logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
MASTER_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'master', 'master.db')
U24_CSV_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'united24', 'u24_master_dataset.csv')


def get_table_columns(conn, table_name):
    """Retrieves column names from the specified SQLite table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [info[1] for info in cursor.fetchall()]


def migrate_and_upload():
    if not os.path.exists(U24_CSV_PATH):
        logging.error(f"Source CSV not found: {U24_CSV_PATH}")
        return

    conn = sqlite3.connect(MASTER_DB_PATH)

    try:
        # 1. Inspect existing schema
        existing_columns = get_table_columns(conn, 'donations')
        logging.info(f"Existing columns in 'donations': {existing_columns}")

        # 2. Schema Migration: Add 'category' if missing
        if 'category' not in existing_columns:
            logging.info("Adding 'category' column to master database.")
            conn.execute("ALTER TABLE donations ADD COLUMN category TEXT")
            conn.commit()
            existing_columns.append('category')

        # 3. Load and Prepare Data
        df = pd.DataFrame()
        df_raw = pd.read_csv(U24_CSV_PATH)

        # MAP COLUMNS: We must match your master.db naming convention
        # Identify the 'amount' column name in your DB (it might be 'amount', 'sum', etc.)
        target_amount_col = 'amount_uah'  # Default expectation
        if 'amount_uah' not in existing_columns:
            # Heuristic: find a column that sounds like 'amount'
            potential_cols = [c for c in existing_columns if 'amount' in c or 'sum' in c]
            if potential_cols:
                target_amount_col = potential_cols[0]
                logging.warning(f"Target DB uses '{target_amount_col}' instead of 'amount_uah'. Mapping...")
            else:
                # If no amount column found, we assume the table is new or we force 'amount_uah'
                logging.info("No amount column detected. Initializing with 'amount_uah'.")

        # Prepare final DataFrame for SQL insertion
        df['date'] = df_raw['date']
        df[target_amount_col] = df_raw['amount_uah']
        df['foundation_name'] = 'united24'
        df['category'] = df_raw['category']

        # 4. Final Data Upload
        logging.info(f"Uploading {len(df)} records to 'donations' table...")
        df.to_sql('donations', conn, if_exists='append', index=False)

        # 5. Build/Update Indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cat ON donations (category)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_found ON donations (foundation_name)")

        conn.commit()
        logging.info("Integration completed successfully.")

    except Exception as e:
        logging.error(f"Critical failure during DB operation: {e}")
        # Doubts: If the error persists, the table 'donations' might not exist at all
        # or it was created with a very different structure.
    finally:
        conn.close()


if __name__ == "__main__":
    migrate_and_upload()