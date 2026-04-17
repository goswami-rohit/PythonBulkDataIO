import pandas as pd
import psycopg2
from psycopg2 import extras
import os
from dotenv import load_dotenv
import re

# ==============================================================================
# 1. Configuration
# ==============================================================================

# Adjust this to the actual path of your file
FILE_PATH = "/Users/rohitgoswami/Downloads/destination_master.xlsx"
SHEET_NAME = "destination_master"
START_ROW_TO_SKIP = 0

TARGET_TABLE = "bestcement.destination_master"

# OMITTING 'id' because it is a SERIAL column and DB will auto-generate it
TARGET_COLUMNS = [
    "institution",
    "zone",
    "district",
    "destination"
]

EXPECTED_COLUMNS = len(TARGET_COLUMNS)

# ==============================================================================
# 2. Helpers
# ==============================================================================

def clean_value(value):
    """Clean empty/NaN values and strip trailing whitespaces."""
    if pd.isna(value):
        return None

    value_str = str(value).strip()

    if value_str.lower() == "nan" or value_str == "":
        return None

    return value_str


def insert_data_to_db(data_to_insert, db_url):
    conn = None

    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        cursor = conn.cursor()

        column_list = ", ".join([f'"{col}"' for col in TARGET_COLUMNS])
        placeholders = ", ".join(["%s"] * EXPECTED_COLUMNS)

        insert_query = f"""
        INSERT INTO {TARGET_TABLE} ({column_list})
        VALUES ({placeholders})
        """

        print(f"Inserting {len(data_to_insert)} rows into '{TARGET_TABLE}'...")
        extras.execute_batch(cursor, insert_query, data_to_insert)

        conn.commit()
        print("SUCCESS: Insert complete.")

    except psycopg2.Error as e:
        print("DATABASE ERROR:", e)
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()
            print("Connection closed.")


# ==============================================================================
# 3. Main
# ==============================================================================

if __name__ == "__main__":

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        print("DATABASE_URL missing. Please set it in your .env file.")
        exit()

    print(f"Reading file from {FILE_PATH}...")

    try:
        df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME)

        # Clean string columns
        for col in TARGET_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(clean_value)
            else:
                print(f"CRITICAL ERROR: Expected column '{col}' not found in the file.")
                exit()

        # Build records
        records_to_insert = []

        for index, row in df.iterrows():
            # Create a tuple of exactly the 4 fields we are inserting
            data_tuple = (
                row["institution"],
                row["zone"],
                row["district"],
                row["destination"]
            )

            # Basic validation
            if len(data_tuple) == EXPECTED_COLUMNS:
                # To prevent inserting completely blank rows
                if any(val is not None for val in data_tuple):
                    records_to_insert.append(data_tuple)
            else:
                print(f"Skipping row {index}: Column mismatch.")

        if records_to_insert:
            insert_data_to_db(records_to_insert, db_url)
        else:
            print("No valid rows found to insert.")

    except FileNotFoundError:
        print(f"ERROR: File not found at the specified path: {FILE_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")