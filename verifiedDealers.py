import pandas as pd
import psycopg2
from psycopg2 import extras
import os
from dotenv import load_dotenv
import re

# ==============================================================================
# 1. Configuration
# ==============================================================================

FILE_PATH = "/Users/rohitgoswami/Downloads/verfiedDealers-neon.xlsx"
SHEET_NAME = "Sheet1"
START_ROW_TO_SKIP = 0

TARGET_TABLE = "verified_dealers"

# --- Columns EXACTLY as in Neon schema (except serial id) ---
TARGET_COLUMNS = [
    "dealer_code",
    "dealer_category",
    "is_subdealer",
    "dealer_party_name",
    "zone",
    "area",
    "contact_no1",
    "contact_no2",
    "email",
    "address",
    "pin_code",
    "related_sp_name",
    "owner_proprietor_name",
    "nature_of_firm",
    "gst_no",
    "pan_no"
]

EXPECTED_COLUMNS = len(TARGET_COLUMNS)

# ==============================================================================
# 2. Helpers
# ==============================================================================

def clean_value(value):
    """Clean Excel junk values"""
    if pd.isna(value):
        return None

    value_str = str(value).strip()

    if value_str.lower() == "nan" or value_str == "":
        return None

    # remove .0 from numbers stored as float
    value_str = re.sub(r'\.0$', '', value_str)

    return value_str


def clean_boolean(value):
    """Convert excel boolean-like values"""
    if pd.isna(value):
        return None

    val = str(value).strip().lower()

    if val in ["true", "yes", "1"]:
        return True
    if val in ["false", "no", "0"]:
        return False

    return None


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
        print("DATABASE_URL missing.")
        exit()

    print("Reading Excel...")

    df = pd.read_excel(
        FILE_PATH,
        sheet_name=SHEET_NAME,
        skiprows=START_ROW_TO_SKIP,
        engine="openpyxl"
    )

    # --- Rename columns from Excel → DB format ---
    df = df.rename(columns={
        "dealerCode": "dealer_code",
        "dealerCategory": "dealer_category",
        "isSubdealer": "is_subdealer",
        "dealerPartyName": "dealer_party_name",
        "contactNo1": "contact_no1",
        "contactNo2": "contact_no2",
        "pinCode": "pin_code",
        "relatedSpName": "related_sp_name",
        "ownerProprietorName": "owner_proprietor_name",
        "natureOfFirm": "nature_of_firm",
        "gstNo": "gst_no",
        "panNo": "pan_no"
    })

    # --- Clean columns ---
    string_cols = [
        "dealer_code","dealer_category","dealer_party_name",
        "zone","area","contact_no1","contact_no2",
        "email","address","pin_code","related_sp_name",
        "owner_proprietor_name","nature_of_firm",
        "gst_no","pan_no"
    ]

    for col in string_cols:
        df[col] = df[col].apply(clean_value)

    df["is_subdealer"] = df["is_subdealer"].apply(clean_boolean)

    # --- Build records ---
    records_to_insert = []

    for _, row in df.iterrows():

        data_tuple = (
            row["dealer_code"],
            row["dealer_category"],
            row["is_subdealer"],
            row["dealer_party_name"],
            row["zone"],
            row["area"],
            row["contact_no1"],
            row["contact_no2"],
            row["email"],
            row["address"],
            row["pin_code"],
            row["related_sp_name"],
            row["owner_proprietor_name"],
            row["nature_of_firm"],
            row["gst_no"],
            row["pan_no"],
        )

        if len(data_tuple) == EXPECTED_COLUMNS:
            records_to_insert.append(data_tuple)

    if records_to_insert:
        insert_data_to_neon(records_to_insert, db_url)
    else:
        print("No valid rows found.")
