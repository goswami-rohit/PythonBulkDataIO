import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import re

# ==============================================================================
# CONFIG
# ==============================================================================

# Using the original file because it contains the Employee and Distributor name strings
FILE_PATH = "/Users/rohitgoswami/Downloads/KamdhenuOutlets.xlsx"
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

def process_outlets(df, db_url):
    conn = None

    try:
        print("Connecting to DB...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        cursor = conn.cursor()

        # ---------------------------------------------------------
        # 1. PRE-LOAD USERS & DISTRIBUTORS FOR IN-MEMORY MAPPING
        # ---------------------------------------------------------
        print("Fetching users and distributors for ID mapping...")
        
        # 1A. Map Users
        cursor.execute("SELECT id, username FROM kamdhenu.users")
        all_users = cursor.fetchall()
        user_map_by_name = {str(u[1]).lower().strip(): u[0] for u in all_users if u[1]}

        # 1B. Map Distributors
        cursor.execute("SELECT id, name FROM kamdhenu.distributors")
        all_distributors = cursor.fetchall()
        dist_map_by_name = {str(d[1]).lower().strip(): d[0] for d in all_distributors if d[1]}

        # ---------------------------------------------------------
        # 2. ITERATE AND INSERT
        # ---------------------------------------------------------
        for index, row in df.iterrows():
            
            # Extract outlet details from the ORIGINAL Excel file
            outlet_name = clean_value(row.get("Name"))
            employee_name = clean_value(row.get("Employee")) 
            distributor_name = clean_value(row.get("Distributor Name"))
            contact_person = clean_value(row.get("Concerned Person"))
            contact_phone = clean_value(row.get("Phone"))
            address = clean_value(row.get("Address"))
            area = clean_value(row.get("Dealer Area"))

            # Fields not present in the original Excel file but exist in schema
            zone = None
            state = None
            district = None
            city = None
            pin_code = None
            gst = None

            if not outlet_name or not contact_person or not contact_phone or not address:
                print(f"Skipping row {index + 2}: Missing mandatory fields (Name, Person, Phone, Address).")
                continue

            print(f"\nProcessing Outlet: {outlet_name}")

            # --- MAP FOREIGN KEY 1: userId ---
            mapped_user_id = None
            if employee_name:
                mapped_user_id = user_map_by_name.get(employee_name.lower().strip())
                if mapped_user_id:
                    print(f"  -> Linked '{employee_name}' to user ID: {mapped_user_id}")
                else:
                    print(f"  -> Warning: Could not find user '{employee_name}'.")

            # --- MAP FOREIGN KEY 2: distributorId ---
            mapped_distributor_id = None
            if distributor_name:
                mapped_distributor_id = dist_map_by_name.get(distributor_name.lower().strip())
                if mapped_distributor_id:
                    print(f"  -> Linked '{distributor_name}' to distributor ID: {mapped_distributor_id}")
                else:
                    print(f"  -> Warning: Could not find distributor '{distributor_name}'.")

            # ==========================================================
            # CHECK EXISTING (To prevent duplicates)
            # ==========================================================
            cursor.execute("""
                SELECT id
                FROM kamdhenu.outlets
                WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s)) 
                  AND concerned_person_phone_num = %s
                LIMIT 1
            """, (outlet_name, contact_phone))

            if cursor.fetchone():
                print("  -> Already exists, skipping")
                continue

            # ==========================================================
            # INSERT INTO OUTLETS
            # ==========================================================
            cursor.execute("""
                INSERT INTO kamdhenu.outlets (
                    distributor_id,
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                mapped_distributor_id,
                mapped_user_id,
                outlet_name,
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
        print("\n✅ Outlet import completed successfully")

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
    
    process_outlets(df, db_url)