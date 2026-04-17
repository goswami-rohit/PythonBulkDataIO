import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import re
import uuid

# ==============================================================================
# CONFIG
# ==============================================================================

FILE_PATH = "/Users/rohitgoswami/Downloads/verifiedDealersWDealerUUID.xlsx"
SHEET_NAME = "Sheet1"

DEFAULT_USER_ID = 77  # Change if needed

# ==============================================================================
# HELPERS
# ==============================================================================

def clean_value(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if value.lower() == "nan" or value == "":
        return None
    value = re.sub(r'\.0$', '', value)
    return value


def clean_boolean(value):
    if pd.isna(value):
        return None
    val = str(value).strip().lower()
    if val in ["true", "yes", "1"]:
        return True
    if val in ["false", "no", "0"]:
        return False
    return None


# ==============================================================================
# MAIN PROCESSOR
# ==============================================================================

def process_verified_dealers(df, db_url):
    conn = None

    try:
        print("Connecting to Neon...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        cursor = conn.cursor()

        for _, row in df.iterrows():

            dealer_name = clean_value(row.get("dealer_party_name"))
            if not dealer_name:
                print("Skipping row without dealer name")
                continue

            print(f"\nProcessing: {dealer_name}")

            dealer_uuid = clean_value(row.get("dealer_uuid"))
            gst_no = clean_value(row.get("gst_no"))

            # ==========================================================
            # SAFE DEFAULTS FOR DEALERS TABLE
            # ==========================================================

            region = clean_value(row.get("zone")) or "zone"
            area = clean_value(row.get("area")) or "area"
            phone = clean_value(row.get("contact_no1")) or "0000000000"
            address = clean_value(row.get("address")) or "address"

            # ==========================================================
            # CASE 1: dealer_uuid already exists
            # ==========================================================
            if dealer_uuid:
                print("→ Using existing dealer_uuid from Excel")

            # ==========================================================
            # CASE 2: Create or reuse dealer
            # ==========================================================
            else:
                print("→ Checking GST in dealers table")

                existing = None

                if gst_no:
                    cursor.execute(
                        """
                        SELECT id
                        FROM dealers
                        WHERE UPPER(TRIM(gstin_no)) = UPPER(TRIM(%s))
                        """,
                        (gst_no,)
                    )
                    existing = cursor.fetchone()

                if existing:
                    dealer_uuid = existing[0]
                    print("→ Found existing dealer by GST")
                else:
                    print("→ Creating new dealer")

                    new_id = str(uuid.uuid4())

                    insert_dealer_query = """
                    INSERT INTO dealers (
                        id,
                        user_id,
                        type,
                        name,
                        region,
                        area,
                        phone_no,
                        address,
                        total_potential,
                        best_potential,
                        brand_selling,
                        feedbacks,
                        verification_status,
                        gstin_no,
                        pan_no
                    )
                    VALUES (
                        %s,
                        %s,
                        'Dealer-Best',
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        0,
                        0,
                        ARRAY[]::text[],
                        '',
                        'VERIFIED',
                        %s,
                        %s
                    )
                    RETURNING id;
                    """

                    cursor.execute(insert_dealer_query, (
                        new_id,
                        DEFAULT_USER_ID,
                        dealer_name,
                        region,
                        area,
                        phone,
                        address,
                        gst_no,
                        clean_value(row.get("pan_no"))
                    ))

                    dealer_uuid = new_id
                    print("→ New dealer created")

            # ==========================================================
            # Prevent duplicate verified dealers (by GST)
            # ==========================================================

            if gst_no:
                cursor.execute(
                    """
                    SELECT id FROM verified_dealers
                    WHERE UPPER(TRIM(gst_no)) = UPPER(TRIM(%s))
                    """,
                    (gst_no,)
                )
                if cursor.fetchone():
                    print("→ Verified dealer already exists. Skipping insert.")
                    continue

            # ==========================================================
            # Insert into verified_dealers
            # ==========================================================

            insert_verified_query = """
            INSERT INTO verified_dealers (
                dealer_party_name,
                contact_no1,
                contact_no2,
                email,
                pin_code,
                gst_no,
                pan_no,
                credit_limit,
                credit_days_allowed,
                zone,
                area,
                sales_man_name_raw,
                alias,
                district,
                state,
                dealer_segment,
                contact_person,
                security_blank_cheque_no,
                sales_promoter_id,
                dealer_uuid
            )
            VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            );
            """

            cursor.execute(insert_verified_query, (
                dealer_name,
                clean_value(row.get("contact_no1")),
                clean_value(row.get("contact_no2")),
                clean_value(row.get("email")),
                clean_value(row.get("pin_code")),
                gst_no,
                clean_value(row.get("pan_no")),
                0,  # credit_limit default
                0,  # credit_days_allowed default
                region,
                area,
                clean_value(row.get("sales_man_name_raw")),
                clean_value(row.get("alias")),
                clean_value(row.get("district")),
                clean_value(row.get("state")),
                clean_value(row.get("dealer_segment")),
                clean_value(row.get("contact_person")),
                clean_value(row.get("security_blank_cheque")),
                clean_value(row.get("sales_promoter_id")),
                dealer_uuid
            ))

        conn.commit()
        print("\nSUCCESS: All dealers processed.")

    except Exception as e:
        print("\nERROR:", e)
        if conn:
            conn.rollback()
            print("Transaction rolled back.")

    finally:
        if conn:
            conn.close()
            print("Connection closed.")


# ==============================================================================
# RUN
# ==============================================================================

if __name__ == "__main__":

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        print("DATABASE_URL missing.")
        exit()

    print("Reading Excel...")

    df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME, engine="openpyxl")

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

    process_verified_dealers(df, db_url)