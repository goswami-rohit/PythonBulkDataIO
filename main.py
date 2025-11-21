import pandas as pd
import psycopg2
from psycopg2 import extras
from pathlib import Path
import os
from dotenv import load_dotenv
import uuid 
import re # For cleaning strings
# Import the extensions module for AsIs
from psycopg2 import extensions 
from psycopg2.extensions import AsIs

# ==============================================================================
# 1. Configuration (File & Table)
# ==============================================================================

# File and Table Configuration
FILE_PATH = "/Users/rohitgoswami/Downloads/neon_ready_subdealers2.xlsx"
SHEET_NAME = "neon_ready_subdealers2"
START_ROW_TO_SKIP = 1

# Target table and columns in your Neon database
TARGET_TABLE = "dealers"

# This 24-column list is correct
TARGET_COLUMNS = [
    "id", "user_id", "type", "parent_dealer_id", "name", "region", "area", "phone_no", 
    "address", "total_potential", "best_potential", "brand_selling", "feedbacks", 
    "remarks", "pinCode", "dateOfBirth", "anniversaryDate", "latitude", "longitude",
    "verification_status", "business_type", "nameOfFirm", "underSalesPromoterName", 
    "gstin_no", "pan_no"
]
EXPECTED_COLUMNS = len(TARGET_COLUMNS)


# ==============================================================================
# 2. Helper Functions
# ==============================================================================

# --- 'adapt_array_literal' function removed ---
# We will pass the Python list or None directly.

def clean_value(value):
    """
    Cleans up the value and handles NaN (empty) inputs from Excel.
    """
    # 1. Check for pandas NaN or NaT
    if pd.isna(value):
        return None
        
    # 2. Convert to string, strip whitespace
    value_str = str(value).strip()
    
    # 3. Check if the string is empty or 'nan'
    if not value_str or value_str.lower() == 'nan':
        return None
    
    # 4. Clean .0 from the end (for pin codes, etc.)
    value_str = re.sub(r'\.0$', '', value_str)
    
    return value_str

def insert_data_to_neon(data_to_insert, db_url):
    """
    Connects to Neon using the full DATABASE_URL string and executes the batch insert.
    """
    conn = None
    try:
        print("Connecting to the Neon database...")
        
        conn = psycopg2.connect(db_url)
        conn.autocommit = False 
        
        cursor = conn.cursor()

        # The INSERT query template
        column_list = ", ".join([f'"{col}"' for col in TARGET_COLUMNS])
        placeholders = ', '.join(['%s'] * EXPECTED_COLUMNS)
        insert_query = f"INSERT INTO {TARGET_TABLE} ({column_list}) VALUES ({placeholders})"
        
        print(f"\n--- DEBUG: Sample of data prepared for insertion (First 3 records) ---")
        for i, record in enumerate(data_to_insert[:3]):
            print(f"Record {i+1} (Length {len(record)}/{EXPECTED_COLUMNS}): {record}")
        print("------------------------------------------------------------------\n")
        
        print(f"Executing batch insert of {len(data_to_insert)} rows...")
        
        extras.execute_batch(cursor, insert_query, data_to_insert)
        
        conn.commit()
        print(f"SUCCESS: Successfully inserted {len(data_to_insert)} records into '{TARGET_TABLE}'.")

    except psycopg2.Error as e:
        print(f"DATABASE INSERTION FAILED:")
        print(f"PostgreSQL Error Message: {e}")
        print("\n--- ERROR DEBUG: Sample of data that failed insertion (First 5 records) ---")
        for i, record in enumerate(data_to_insert[:5]):
            print(f"Record {i+1} (Length {len(record)}): {record}")
        print("------------------------------------------------------------------\n")
        
        if conn:
            conn.rollback() 
    except Exception as e:
        print(f"An unexpected Python execution error occurred: {e}")
        print(f"\n--- ACTION REQUIRED: Check all {EXPECTED_COLUMNS} elements of the data tuples below ---")
        for i, record in enumerate(data_to_insert[:5]):
            if len(record) != EXPECTED_COLUMNS:
                print(f"CRITICAL LENGTH ERROR AT RECORD {i+1}. Expected {EXPECTED_COLUMNS}, got {len(record)}.")
            print(f"Record {i+1} Types: {tuple(type(item).__name__ for item in record)}")
            print(f"Record {i+1} Data: {record}")
        print("------------------------------------------------------------------\n")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


# ==============================================================================
# 3. Main Execution Logic
# ==============================================================================

if __name__ == "__main__":
    
    # 3a. Load environment and get connection string
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        print("ERROR: DATABASE_URL not found. Please ensure you have a .env file with DATABASE_URL set.")
        exit()
        
    # 3b. Read and process Excel data
    try:
        print(f"Reading data from: {FILE_PATH} (Sheet: {SHEET_NAME})...")
        
        df = pd.read_excel(
            FILE_PATH, 
            sheet_name=SHEET_NAME, 
            skiprows=START_ROW_TO_SKIP, 
            header=None, 
            engine='openpyxl'
        )

        df = df.iloc[:, :24]
        
        df.columns = [
            "user_id", "type", "parent_dealer_id", "name", "region", "area", "phone_no", 
            "address", "total_potential", "best_potential", "brand_selling", "feedbacks",
            "remarks", "pinCode", "dateOfBirth", "anniversaryDate", "latitude", "longitude",
            "verificationStatus", "business_type", "nameOfFirm", "underSalesPromoterName", 
            "gstin_no", "pan_no"
        ]
        
        # === Explicit Type Casting to ensure data integrity for PostgreSQL ===
        
        numeric_cols = ['user_id', 'total_potential', 'best_potential']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        numeric_float_cols = ['latitude', 'longitude']
        for col in numeric_float_cols:
             df[col] = pd.to_numeric(df[col], errors='coerce').apply(lambda x: None if pd.isna(x) else x)

        # Ensure phone_no is treated as a string
        df['phone_no'] = df['phone_no'].astype(str).str.replace(r'\.0$', '', regex=True).fillna('')
        
        # !!! FIX: Create a separate list for date columns
        date_cols = ['dateOfBirth', 'anniversaryDate']
        for col in date_cols:
            # Convert to datetime, letting errors become NaT (Not a Time)
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Convert any valid dates to Python date objects, and NaT to None
            df[col] = df[col].apply(lambda x: x.date() if pd.notnull(x) else None)

        # !!! FIX: Remove date_cols from the string_cols list
        string_cols = [
            'type', 'parent_dealer_id', 'name', 'region', 'area', 'address', 'feedbacks', 'remarks', 
            'pinCode', 'verificationStatus', 'business_type', 'nameOfFirm', 
            'underSalesPromoterName', 'gstin_no', 'pan_no'
        ]
        for col in string_cols:
            # Apply the improved clean_value function
            df[col] = df[col].apply(clean_value)
        
        # Process the DataFrame into a list of tuples suitable for batch insertion
        records_to_insert = []
        for index, row in df.iterrows():
            
            # --- Brand Selling Array Formatting ---
            brand_selling_value = row['brand_selling']
            
            if pd.isna(brand_selling_value) or brand_selling_value is None or str(brand_selling_value).strip().lower() == 'nan':
                brand_list_or_none = None
            else:
                brand_list_or_none = [str(brand_selling_value).strip()]

            data_tuple = (
                str(uuid.uuid4()),  # 1. id
                row['user_id'],     # 2. user_id
                row['type'],        # 3. type
                row['parent_dealer_id'], # 4. parent_dealer_id
                row['name'],        # 5. name
                row['region'],      # 6. region
                row['area'],        # 7. area
                row['phone_no'],    # 8. phone_no
                row['address'],     # 9. address
                row['total_potential'], # 10. total_potential
                row['best_potential'],  # 11. best_potential
                brand_list_or_none,     # 12. brand_selling
                row['feedbacks'],   # 13. feedbacks
                row['remarks'],     # 14. remarks
                row['pinCode'],     # 15. pinCode
                row['dateOfBirth'], # 16. dateOfBirth (will be None or a date object)
                row['anniversaryDate'], # 17. anniversaryDate (will be None or a date object)
                row['latitude'],    # 18. latitude
                row['longitude'],   # 19. longitude
                row['verificationStatus'], # 20. verification_status
                row['business_type'], # 21. business_type
                row['nameOfFirm'],  # 22. nameOfFirm
                row['underSalesPromoterName'], # 23. underSalesPromoterName
                row['gstin_no'],    # 24. gstin_no
                row['pan_no']       # 25. pan_no
            )
            
            if len(data_tuple) != EXPECTED_COLUMNS:
                print(f"SKIPPING ROW {index+START_ROW_TO_SKIP+1}: Incorrect tuple length.")
                continue
                
            records_to_insert.append(data_tuple)
        
        # 3c. Insert data
        if records_to_insert:
            insert_data_to_neon(records_to_insert, db_url)
        else:
            print("INFO: No valid records found to insert after skipping rows.")

    except FileNotFoundError:
        print(f"ERROR: File not found at the specified path: {FILE_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred during file processing or database operation: {e}")