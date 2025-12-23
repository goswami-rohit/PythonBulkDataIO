import pandas as pd
import psycopg2
from psycopg2 import extras
from pathlib import Path
import os
from dotenv import load_dotenv
import uuid 
import re 
import json # Required for Radar Metadata
import requests # Required for Radar API calls
from psycopg2 import extensions 
from psycopg2.extensions import AsIs

# ==============================================================================
# 1. Configuration (File & Table)
# ==============================================================================

FILE_PATH = "/Users/rohitgoswami/Downloads/neon_ready_dealers2.xlsx"
SHEET_NAME = "Sheet1"
START_ROW_TO_SKIP = 1

TARGET_TABLE = "dealers"

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

def clean_value(value):
    if pd.isna(value): return None
    value_str = str(value).strip()
    if not value_str or value_str.lower() == 'nan': return None
    value_str = re.sub(r'\.0$', '', value_str)
    return value_str

# --- NEW: RADAR GEOFENCE FUNCTION ---
def upsert_radar_geofence(dealer_data, secret_key):
    """
    Creates/Updates a geofence in Radar.io matching the TypeScript logic.
    dealer_data is the tuple created in the main loop.
    """
    # Extract data using the tuple indices based on TARGET_COLUMNS order
    dealer_id = dealer_data[0]
    user_id = dealer_data[1]
    name = dealer_data[4]
    region = dealer_data[5]
    area = dealer_data[6]
    phone_no = dealer_data[7]
    latitude = dealer_data[17]
    longitude = dealer_data[18]
    verification_status = dealer_data[19]

    # Skip if no coordinates (Radar requires them)
    if latitude is None or longitude is None:
        print(f"Skipping Radar for Dealer ID {dealer_id}: Missing Coordinates")
        return False

    # 1. API Configuration
    url = f"https://api.radar.io/v1/geofences/dealer/dealer:{dealer_id}"
    headers = {
        "Authorization": secret_key,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # 2. Prepare Metadata (Matches TS: dealerId, userId, region, area, phoneNo, verificationStatus)
    metadata = {
        "dealerId": str(dealer_id),
        "userId": str(user_id) if user_id else None,
        "region": region,
        "area": area,
        "phoneNo": phone_no,
        "verificationStatus": verification_status
    }
    # Remove None keys from metadata
    metadata = {k: v for k, v in metadata.items() if v is not None}

    # 3. Prepare Payload
    payload = {
    "description": str(name)[:120],               # Name as description
    "type": "circle",
    "coordinates": json.dumps([float(longitude), float(latitude)]),  # [lng, lat]
    "radius": "50",                               # meters
    "tag": "dealer",                              # <- tag = dealer
    "externalId": f"dealer:{dealer_id}",          # <- externalId = dealer:<neon_uuid>
    "metadata": json.dumps(metadata)
 }

    try:
        response = requests.put(url, headers=headers, data=payload)
        if response.status_code in [200, 201]:
            # print(f"Radar Success: {name}") 
            return True
        else:
            print(f"Radar Failed [{response.status_code}]: {response.text}")
            return False
    except Exception as e:
        print(f"Radar Error: {e}")
        return False

def insert_data_to_neon(data_to_insert, db_url, radar_key):
    """
    Connects to Neon, inserts DB records, AND THEN processes Radar geofences.
    """
    conn = None
    try:
        print("Connecting to the Neon database...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False 
        cursor = conn.cursor()

        # --- STEP 1: DATABASE INSERT ---
        column_list = ", ".join([f'"{col}"' for col in TARGET_COLUMNS])
        placeholders = ', '.join(['%s'] * EXPECTED_COLUMNS)
        insert_query = f"INSERT INTO {TARGET_TABLE} ({column_list}) VALUES ({placeholders})"
        
        print(f"Executing batch insert of {len(data_to_insert)} rows into PostgreSQL...")
        extras.execute_batch(cursor, insert_query, data_to_insert)
        conn.commit()
        print(f"✅ DB SUCCESS: Inserted {len(data_to_insert)} records into '{TARGET_TABLE}'.")

        # --- STEP 2: RADAR IMPORT (Only if DB succeeds) ---
        if radar_key:
            print("\n--- Starting Radar.io Geofence Import ---")
            success_count = 0
            for i, record in enumerate(data_to_insert):
                # record is the tuple
                if upsert_radar_geofence(record, radar_key):
                    success_count += 1
                
                # Optional: Progress indicator every 50 records
                if (i + 1) % 50 == 0:
                    print(f"Processed {i + 1}/{len(data_to_insert)} Radar records...")

            print(f"✅ RADAR SUCCESS: Created {success_count} geofences out of {len(data_to_insert)} records.")
        else:
            print("⚠️ SKIPPING RADAR: No RADAR_SECRET_KEY found in .env")

    except psycopg2.Error as e:
        print(f"DATABASE INSERTION FAILED:")
        print(f"PostgreSQL Error Message: {e}")
        if conn: conn.rollback() 
    except Exception as e:
        print(f"An unexpected Python execution error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

# ==============================================================================
# 3. Main Execution Logic
# ==============================================================================

if __name__ == "__main__":
    
    # 3a. Load environment
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    radar_key = os.getenv("RADAR_SECRET_KEY") # Load Radar Key

    if not db_url:
        print("ERROR: DATABASE_URL not found.")
        exit()
    
    if not radar_key:
        print("WARNING: RADAR_SECRET_KEY not found. Geofences will NOT be created.")
        
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
        
        # === Explicit Type Casting ===
        numeric_cols = ['user_id', 'total_potential', 'best_potential']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        numeric_float_cols = ['latitude', 'longitude']
        for col in numeric_float_cols:
             df[col] = pd.to_numeric(df[col], errors='coerce').apply(lambda x: None if pd.isna(x) else x)

        df['phone_no'] = df['phone_no'].apply(clean_value)
        df['phone_no'] = df['phone_no'].fillna('')

        
        date_cols = ['dateOfBirth', 'anniversaryDate']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: x.date() if pd.notnull(x) else None)

        string_cols = [
            'type', 'parent_dealer_id', 'name', 'region', 'area', 'address', 'feedbacks', 'remarks', 
            'pinCode', 'verificationStatus', 'business_type', 'nameOfFirm', 
            'underSalesPromoterName', 'gstin_no', 'pan_no'
        ]
        for col in string_cols:
            df[col] = df[col].apply(clean_value)
        
        records_to_insert = []
        for index, row in df.iterrows():
            
            brand_selling_value = row['brand_selling']
            if pd.isna(brand_selling_value) or brand_selling_value is None or str(brand_selling_value).strip().lower() == 'nan':
                brand_list_or_none = None
            else:
                brand_list_or_none = [str(brand_selling_value).strip()]

            # GENERATING UUID HERE
            new_dealer_uuid = str(uuid.uuid4())

            data_tuple = (
                new_dealer_uuid,    # 1. id
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
                row['dateOfBirth'], # 16. dateOfBirth
                row['anniversaryDate'], # 17. anniversaryDate
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
                print(f"SKIPPING ROW: Incorrect tuple length.")
                continue
                
            records_to_insert.append(data_tuple)
        
        # 3c. Insert data (SQL + Radar)
        if records_to_insert:
            insert_data_to_neon(records_to_insert, db_url, radar_key)
        else:
            print("INFO: No valid records found to insert.")

    except FileNotFoundError:
        print(f"ERROR: File not found: {FILE_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")