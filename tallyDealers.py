import pandas as pd
import psycopg2
from psycopg2 import extras
import os
from dotenv import load_dotenv
import re

# ==============================================================================
# 1. CONFIG
# ==============================================================================

FILE_PATH = "/Users/rohitgoswami/Downloads/tallyDealersToNeon.xlsx"
SHEET_NAME = "Sheet1"
START_ROW_TO_SKIP = 0

TARGET_TABLE = "tally_dealers"

# --- EXACT Neon Columns (NO id, NO timestamps) ---
TARGET_COLUMNS = [
    "institution",
    "name",
    "alias",

    "address1",
    "address2",
    "address3",
    "address4",
    "address5",

    "bill_wise_details",
    "phone",
    "mobile",
    "email",

    "group1",
    "group2",
    "group3",
    "group4",

    "pan",
    "tin",
    "cst",
    "cr_limit",

    "contact_person",
    "state",
    "pincode",

    "gst_reg_type",
    "gst_no",

    "list_of_ledger",
    "sd_ledger",

    "salesman_name",
    "sales_promoter",
    "security_blank_check_no",

    "destination",
    "district",
    "zone",
]

EXPECTED_COLUMNS = len(TARGET_COLUMNS)

# ==============================================================================
# 2. HELPERS
# ==============================================================================

def clean_value(value):
    if pd.isna(value):
        return None

    val = str(value).strip()

    if val.lower() == "nan" or val == "":
        return None

    # remove float junk
    val = re.sub(r'\.0$', '', val)

    return val


def clean_numeric(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except:
        return None


# ==============================================================================
# 3. INSERT
# ==============================================================================

def insert_data_to_neon(data_to_insert, db_url):

    conn = None

    try:
        print("Connecting to Neon...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        cursor = conn.cursor()

        column_list = ", ".join([f'"{col}"' for col in TARGET_COLUMNS])
        placeholders = ", ".join(["%s"] * EXPECTED_COLUMNS)

        insert_query = f"""
        INSERT INTO {TARGET_TABLE} ({column_list})
        VALUES ({placeholders})
        """

        print(f"Inserting {len(data_to_insert)} rows...")

        extras.execute_batch(
            cursor,
            insert_query,
            data_to_insert,
            page_size=500
        )

        conn.commit()
        print("SUCCESS: Bulk insert completed.")

    except psycopg2.Error as e:
        print("DATABASE ERROR:", e)
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()
            print("Connection closed.")


# ==============================================================================
# 4. MAIN
# ==============================================================================

if __name__ == "__main__":

    load_dotenv()
    db_url = os.getenv("TALLY_DATABASE_URL")

    if not db_url:
        print("TALLY_DATABASE_URL missing.")
        exit()

    print("Reading Excel...")

    df = pd.read_excel(
        FILE_PATH,
        sheet_name=SHEET_NAME,
        skiprows=START_ROW_TO_SKIP,
        engine="openpyxl"
    )

    # --------------------------------------------------------------------------
    # Rename Excel columns → DB columns (camelCase → snake_case)
    # --------------------------------------------------------------------------

    df = df.rename(columns={
        "billWiseDetails": "bill_wise_details",
        "crLimit": "cr_limit",
        "contactPerson": "contact_person",
        "gstRegType": "gst_reg_type",
        "gstNo": "gst_no",
        "listOfLedger": "list_of_ledger",
        "sdLedger": "sd_ledger",
        "salesmanName": "salesman_name",
        "salesPromoter": "sales_promoter",
        "securityBlankCheckNo": "security_blank_check_no",
    })

    # --------------------------------------------------------------------------
    # CLEANING
    # --------------------------------------------------------------------------

    string_cols = [c for c in TARGET_COLUMNS if c != "cr_limit"]

    for col in string_cols:
        df[col] = df[col].apply(clean_value)

    df["cr_limit"] = df["cr_limit"].apply(clean_numeric)

    # --------------------------------------------------------------------------
    # BUILD RECORDS
    # --------------------------------------------------------------------------

    records_to_insert = []

    for _, row in df.iterrows():

        data_tuple = tuple(row.get(col) for col in TARGET_COLUMNS)

        if len(data_tuple) == EXPECTED_COLUMNS:
            records_to_insert.append(data_tuple)

    # --------------------------------------------------------------------------
    # EXECUTE
    # --------------------------------------------------------------------------

    if records_to_insert:
        insert_data_to_neon(records_to_insert, db_url)
    else:
        print("No valid rows found.")