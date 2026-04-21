import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import re
import uuid

# ==============================================================================
# CONFIG
# ==============================================================================

FILE_PATH = "/Users/rohitgoswami/Downloads/verifiedDealers-db-upsert.xlsx"
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
        print("Connecting to DB...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        cursor = conn.cursor()

        # ==========================================================
        # STEP 0: Helper for valid SP
        # ==========================================================
        def is_valid_sp(name):
            if not name:
                return False
            name_clean = name.strip().lower()
            return name_clean not in [
                "no sales promoter",
                "(no sales promoter)",
                "no sp",
                "n/a"
            ]

        # ==========================================================
        # STEP 1: PRELOAD SALES PROMOTERS
        # ==========================================================
        print("Loading existing sales promoters...")

        cursor.execute("SELECT id, name FROM bestcement.sales_promoters")
        sp_map = {
            row[1].strip().lower(): row[0]
            for row in cursor.fetchall()
            if row[1]
        }

        # ==========================================================
        # STEP 2: INSERT UNIQUE NEW SALES PROMOTERS
        # ==========================================================
        print("Processing unique sales promoters...")

        unique_sps = set()

        for _, row in df.iterrows():
            sp = clean_value(row.get("related_sp_name"))
            if sp and is_valid_sp(sp):
                unique_sps.add(sp.strip())

        for sp in unique_sps:
            key = sp.lower()

            if key not in sp_map:
                cursor.execute("""
                    INSERT INTO bestcement.sales_promoters (name)
                    VALUES (%s)
                    RETURNING id
                """, (sp,))
                sp_map[key] = cursor.fetchone()[0]
                print(f"→ Added SP: {sp}")

        # ==========================================================
        # STEP 3: MAIN LOOP
        # ==========================================================
        for _, row in df.iterrows():

            dealer_name = clean_value(row.get("dealer_party_name"))
            if not dealer_name:
                print("Skipping row without dealer name")
                continue

            gst_no = clean_value(row.get("gst_no"))
            dealer_uuid = clean_value(row.get("dealer_uuid"))

            # ---------------------------
            # SALES PROMOTER ID
            # ---------------------------
            sp_name = clean_value(row.get("related_sp_name"))
            sales_promoter_id = None

            if sp_name and is_valid_sp(sp_name):
                sales_promoter_id = sp_map.get(sp_name.lower())

            print(f"\nProcessing: {dealer_name} | GST: {gst_no} | SP_ID: {sales_promoter_id}")

            # ==========================================================
            # STEP 3A: ENSURE DEALER UUID
            # ==========================================================
            if not dealer_uuid:
                existing = None

                if gst_no:
                    cursor.execute("""
                        SELECT id FROM bestcement.dealers
                        WHERE UPPER(TRIM(gstin_no)) = UPPER(TRIM(%s))
                    """, (gst_no,))
                    existing = cursor.fetchone()

                if existing:
                    dealer_uuid = existing[0]
                    print("→ Reused dealer_uuid from GST")
                else:
                    dealer_uuid = str(uuid.uuid4())

                    cursor.execute("""
                        INSERT INTO bestcement.dealers (
                            id, user_id, type, name, region, area,
                            phone_no, address, total_potential,
                            best_potential, brand_selling, feedbacks,
                            verification_status, gstin_no, pan_no
                        )
                        VALUES (%s, %s, 'Dealer-Best', %s, %s, %s,
                                %s, %s, 0, 0, ARRAY[]::text[], '',
                                'VERIFIED', %s, %s)
                    """, (
                        dealer_uuid,
                        DEFAULT_USER_ID,
                        dealer_name,
                        clean_value(row.get("zone")) or "zone",
                        clean_value(row.get("area")) or "area",
                        clean_value(row.get("contact_no1")) or "0000000000",
                        clean_value(row.get("address")) or "address",
                        gst_no,
                        clean_value(row.get("pan_no"))
                    ))

                    print("→ New dealer created")

            # ==========================================================
            # STEP 3B: FIND EXISTING VERIFIED DEALER
            # ==========================================================
            existing = None

            if gst_no:
                cursor.execute("""
                    SELECT id FROM bestcement.verified_dealers
                    WHERE UPPER(TRIM(gst_no)) = UPPER(TRIM(%s))
                    LIMIT 1
                """, (gst_no,))
                existing = cursor.fetchone()

            if not existing:
                cursor.execute("""
                    SELECT id FROM bestcement.verified_dealers
                    WHERE LOWER(TRIM(dealer_party_name)) = LOWER(TRIM(%s))
                    LIMIT 1
                """, (dealer_name,))
                existing = cursor.fetchone()

            # ==========================================================
            # STEP 3C: UPDATE
            # ==========================================================
            if existing:
                print("→ Updating existing verified dealer")

                cursor.execute("""
                    UPDATE bestcement.verified_dealers SET
                        contact_no1 = COALESCE(%s, contact_no1),
                        contact_no2 = COALESCE(%s, contact_no2),
                        email = COALESCE(%s, email),
                        pin_code = COALESCE(%s, pin_code),
                        gst_no = COALESCE(%s, gst_no),
                        pan_no = COALESCE(%s, pan_no),
                        zone = COALESCE(%s, zone),
                        area = COALESCE(%s, area),
                        district = COALESCE(%s, district),
                        state = COALESCE(%s, state),
                        dealer_segment = COALESCE(%s, dealer_segment),
                        contact_person = COALESCE(%s, contact_person),
                        security_blank_cheque_no = COALESCE(%s, security_blank_cheque_no),
                        sales_promoter_id = COALESCE(%s, sales_promoter_id),
                        dealer_uuid = COALESCE(%s, dealer_uuid),
                        updated_at = NOW()
                    WHERE id = %s
                """, (
                    clean_value(row.get("contact_no1")),
                    clean_value(row.get("contact_no2")),
                    clean_value(row.get("email")),
                    clean_value(row.get("pin_code")),
                    gst_no,
                    clean_value(row.get("pan_no")),
                    clean_value(row.get("zone")),
                    clean_value(row.get("area")),
                    clean_value(row.get("district")),
                    clean_value(row.get("state")),
                    clean_value(row.get("dealer_segment")),
                    clean_value(row.get("contact_person")),
                    clean_value(row.get("security_blank_cheque")),
                    sales_promoter_id,
                    dealer_uuid,
                    existing[0]
                ))

            # ==========================================================
            # STEP 3D: INSERT
            # ==========================================================
            else:
                print("→ Inserting new verified dealer")

                cursor.execute("""
                    INSERT INTO bestcement.verified_dealers (
                        dealer_party_name,
                        contact_no1,
                        contact_no2,
                        email,
                        pin_code,
                        gst_no,
                        pan_no,
                        zone,
                        area,
                        district,
                        state,
                        dealer_segment,
                        contact_person,
                        security_blank_cheque_no,
                        sales_promoter_id,
                        dealer_uuid
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    dealer_name,
                    clean_value(row.get("contact_no1")),
                    clean_value(row.get("contact_no2")),
                    clean_value(row.get("email")),
                    clean_value(row.get("pin_code")),
                    gst_no,
                    clean_value(row.get("pan_no")),
                    clean_value(row.get("zone")),
                    clean_value(row.get("area")),
                    clean_value(row.get("district")),
                    clean_value(row.get("state")),
                    clean_value(row.get("dealer_segment")),
                    clean_value(row.get("contact_person")),
                    clean_value(row.get("security_blank_cheque")),
                    sales_promoter_id,
                    dealer_uuid
                ))

        conn.commit()
        print("\n✅ SUCCESS: Clean upsert completed")

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