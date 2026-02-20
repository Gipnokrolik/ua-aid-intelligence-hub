import sqlite3
import os
import logging

# Logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Relative path management
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
MASTER_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'master', 'master.db')


def normalize_cba_categories():
    """
    Updates legacy records for Come Back Alive foundation.
    Replaces NULL categories with 'general' to ensure data consistency.
    """
    if not os.path.exists(MASTER_DB_PATH):
        logging.error(f"Database not found: {MASTER_DB_PATH}")
        return

    conn = sqlite3.connect(MASTER_DB_PATH)
    cursor = conn.cursor()

    try:
        # SQL update to fill missing categories for CBA
        # We use 'general' as the standard label for non-categorized funds
        query = """
            UPDATE donations 
            SET category = 'general' 
            WHERE foundation_name = 'come_back_alive' 
            AND category IS NULL;
        """

        cursor.execute(query)
        rows_affected = cursor.rowcount
        conn.commit()

        logging.info(f"Cleanup complete. Updated {rows_affected} records for 'come_back_alive'.")

        # Verify if any NULLs remain in the database
        cursor.execute("SELECT COUNT(*) FROM donations WHERE category IS NULL")
        remaining_nulls = cursor.fetchone()[0]

        if remaining_nulls > 0:
            logging.warning(f"Validation: {remaining_nulls} records still have NULL categories.")
        else:
            logging.info("Validation successful: No NULL categories remaining.")

    except Exception as e:
        logging.error(f"Failed to normalize categories: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    normalize_cba_categories()