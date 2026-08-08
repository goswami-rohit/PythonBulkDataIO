import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import re

# ==============================================================================
# CONFIG
# ==============================================================================

FILE_PATH = "/Users/rohitgoswami/Downloads/euroDealersList2.xlsx"
SHEET_NAME = "Sheet1"

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

def process_dealers(df, db_url):

    conn = None

    try:

        print("Connecting to DB...")

        conn = psycopg2.connect(db_url)
        conn.autocommit = False

        cursor = conn.cursor()

        for _, row in df.iterrows():

            dealer_name = clean_value(row.get("dealer_party_name"))
            zone = clean_value(row.get("zone"))

            if not dealer_name:
                print("Skipping empty dealer")
                continue

            print(f"Processing: {dealer_name}")

            # ==========================================================
            # CHECK EXISTING
            # ==========================================================

            cursor.execute("""
                SELECT id
                FROM eurofoam.dealers
                WHERE LOWER(TRIM(dealer_party_name)) =
                      LOWER(TRIM(%s))
                LIMIT 1
            """, (dealer_name,))

            existing = cursor.fetchone()

            # ==========================================================
            # SKIP IF EXISTS
            # ==========================================================

            if existing:
                print("→ Already exists, skipping")
                continue

            # ==========================================================
            # INSERT
            # ==========================================================

            cursor.execute("""
                INSERT INTO eurofoam.dealers (
                    dealer_party_name,
                    zone,
                    is_verified
                )
                VALUES (%s, %s, TRUE)
            """, (
                dealer_name,
                zone
            ))

            print("→ Inserted")

        conn.commit()

        print("\n✅ Dealer import completed")

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
        print("DATABASE_URL missing")
        exit()

    print("Reading Excel...")

    df = pd.read_excel(
        FILE_PATH,
        sheet_name=SHEET_NAME,
        engine="openpyxl"
    )

    # Rename columns from Excel
    df = df.rename(columns={
        "dealerPartyName": "dealer_party_name",
        "Zone": "zone"
    })

    process_dealers(df, db_url)