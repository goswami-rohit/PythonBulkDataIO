import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import re

# ==============================================================================
# CONFIG
# ==============================================================================

# Your original Excel file path
FILE_PATH = "/Users/rohitgoswami/Downloads/KamdhenuDistributors.xlsx"
SHEET_NAME = 0

# ==============================================================================
# HELPERS
# ==============================================================================

def clean_value(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if value.lower() in ["nan", "none", "null", ""]:
        return None
    value = re.sub(r"\.0$", "", value)
    return value

# ==============================================================================
# MAIN
# ==============================================================================

def process_distributors(df, db_url):
    conn = None

    try:
        print("Connecting to DB...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        cursor = conn.cursor()

        # ---------------------------------------------------------
        # 1. PRE-LOAD USERS FOR IN-MEMORY MAPPING
        # ---------------------------------------------------------
        print("Fetching users to map Employee names to User IDs...")
        
        # Fetching ONLY id and username
        cursor.execute("SELECT id, username FROM kamdhenu.users")
        all_users = cursor.fetchall()
        
        # Create a lookup dictionary by username 
        # Using lowercase and strip to ensure "Vinay Raj Choudhary" matches perfectly
        user_map_by_name = {str(u[1]).lower().strip(): u[0] for u in all_users if u[1]}

        # ---------------------------------------------------------
        # 2. ITERATE AND INSERT
        # ---------------------------------------------------------
        for index, row in df.iterrows():
            
            # Extract distributor details from Excel
            distributor_name = clean_value(row.get("Name"))
            employee_name = clean_value(row.get("Employee")) # <-- THIS IS THE KEY FIX
            contact_person = clean_value(row.get("Concerned Person"))
            contact_phone = clean_value(row.get("Phone"))
            address = clean_value(row.get("Address"))
            gst = clean_value(row.get("GST number"))

            # Since the original excel doesn't have split area/zone/city/state 
            # in distinct columns, we leave them NULL for now based on your schema.
            area = None
            zone = None
            state = None
            district = None
            city = None
            pin_code = None

            if not distributor_name or not contact_person or not contact_phone or not address:
                print(f"Skipping row {index + 2}: Missing mandatory fields.")
                continue

            print(f"Processing Distributor: {distributor_name}")

            # --- MAP THE FOREIGN KEY (userId) ---
            mapped_user_id = None
            
            if employee_name:
                identifier_lower = employee_name.lower().strip()
                mapped_user_id = user_map_by_name.get(identifier_lower)

            if not mapped_user_id:
                print(f"  -> Warning: Could not find DB user matching '{employee_name}'. Inserting with NULL userId.")
            else:
                print(f"  -> Linked '{employee_name}' to user ID: {mapped_user_id}")

            # ==========================================================
            # CHECK EXISTING 
            # ==========================================================
            cursor.execute("""
                SELECT id
                FROM kamdhenu.distributors
                WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s)) 
                  AND concerned_person_phone_num = %s
                LIMIT 1
            """, (distributor_name, contact_phone))

            if cursor.fetchone():
                print("  -> Already exists, skipping")
                continue

            # ==========================================================
            # INSERT INTO DISTRIBUTORS
            # ==========================================================
            cursor.execute("""
                INSERT INTO kamdhenu.distributors (
                    user_id,
                    name, 
                    concerned_person_name, 
                    concerned_person_phone_num, 
                    area, 
                    zone,
                    state,
                    district,
                    city,
                    address,
                    pin_code,
                    gst_number
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                mapped_user_id,
                distributor_name,
                contact_person,
                contact_phone,
                area,
                zone,
                state,
                district,
                city,
                address,
                pin_code,
                gst
            ))

        conn.commit()
        print("\n✅ Distributor import completed successfully")

    except Exception as e:
        print("\n❌ ERROR:", e)
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()

# ==============================================================================
# RUN
# ==============================================================================
if __name__ == "__main__":
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        print("DATABASE_URL missing in environment variables.")
        exit()

    print("Reading Excel...")
    df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME, engine="openpyxl")
    df = df.dropna(how='all')
    
    process_distributors(df, db_url)