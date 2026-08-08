import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import re

# ==============================================================================
# CONFIG
# ==============================================================================

FILE_PATH = "/Users/rohitgoswami/Downloads/kamdhenuEmpl.xlsx"
SHEET_NAME = 0 # Reads the first sheet by default

# ==============================================================================
# HELPERS
# ==============================================================================

def clean_value(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value.lower() in ["nan", "none", "null", ""]:
        return None

    # Strip trailing .0 from phone numbers parsed as floats
    value = re.sub(r"\.0$", "", value)

    return value

def generate_first_name_password(full_name):
    if not full_name:
        return "user@123"
    
    # Extract the first word (first name) and convert to lowercase
    first_name = full_name.split()[0].lower()
    # Remove any non-alphabet characters just in case
    first_name = re.sub(r'[^a-z]', '', first_name)
    
    return f"{first_name}@123"

# ==============================================================================
# MAIN
# ==============================================================================

def process_users(df, db_url):
    conn = None

    try:
        print("Connecting to DB...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        cursor = conn.cursor()

        for index, row in df.iterrows():
            
            # --- Extract & Clean Base Values ---
            raw_email = clean_value(row.get("email"))
            raw_username = clean_value(row.get("username"))
            phone_num = clean_value(row.get("phoneNumber"))
            role = clean_value(row.get("role"))
            area = clean_value(row.get("area"))
            zone = clean_value(row.get("zone"))
            
            # Require at least phone number and name to create a valid user
            if not phone_num or not raw_username:
                print(f"Skipping row {index + 2}: Missing Name or Phone Number")
                continue

            print(f"Processing: {raw_username} ({phone_num})")

            # --- Apply Normalization Rules ---
            # 1. Normalize name to Title Case (e.g., "Piku Goswami")
            username_title_cased = raw_username.title()
            
            # 2. Email format (lowercase)
            email = raw_email.lower() if raw_email else f"{phone_num}@placeholder.com"
            
            # 3. Dynamic App Password (e.g., "piku@123")
            app_password = generate_first_name_password(raw_username)

            # ==========================================================
            # CHECK EXISTING BY PHONE NUMBER OR EMAIL
            # ==========================================================

            cursor.execute("""
                SELECT id
                FROM kamdhenu.users
                WHERE salesman_login_id = %s OR email = %s
                LIMIT 1
            """, (phone_num, email))

            existing = cursor.fetchone()

            if existing:
                print("→ Already exists, skipping")
                continue

            # ==========================================================
            # INSERT INTO NEW SCHEMA
            # ==========================================================

            cursor.execute("""
                INSERT INTO kamdhenu.users (
                    email, 
                    username, 
                    phone_number, 
                    role, 
                    status, 
                    area, 
                    zone,
                    is_dashboard_user, 
                    is_sales_app_user, 
                    salesman_login_id, 
                    sales_app_password
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                email,
                username_title_cased,
                phone_num,
                role or 'Salesman', # Fallback role if empty
                'active',
                area,
                zone,
                False,        # is_dashboard_user
                True,         # is_sales_app_user
                phone_num,    # salesman_login_id
                app_password  # sales_app_password
            ))

            print(f"→ Inserted {username_title_cased} with pass {app_password}")

        conn.commit()
        print("\n✅ User import completed successfully")

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
    
    # Make sure to install openpyxl if you haven't: pip install openpyxl
    df = pd.read_excel(
        FILE_PATH,
        sheet_name=SHEET_NAME,
        engine="openpyxl"
    )

    process_users(df, db_url)