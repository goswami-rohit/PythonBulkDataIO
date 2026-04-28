import pandas as pd
import json
from rapidfuzz import process, fuzz
import re

# ==============================================================================
# 1. CONFIG
# ==============================================================================

EXCEL_PATH = "/Users/rohitgoswami/Downloads/destination_master.xlsx"
SHEET_NAME = "destination_master"
PINCODE_JSON = "NEpincode.json"

OUTPUT_FILE = "destination_with_pincode.csv"

# ==============================================================================
# 2. HELPERS
# ==============================================================================

def clean_value(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value.lower() in ["nan", ""]:
        return None

    return value


def normalize_text(text):
    if not text:
        return ""

    text = text.lower()

    # remove bracket content
    text = re.sub(r"\(.*?\)", "", text)

    # remove extra stuff
    text = re.sub(r"[^a-z0-9\s]", "", text)

    return text.strip()


# ==============================================================================
# 3. LOAD DATA
# ==============================================================================

print("Loading Excel...")
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

print("Loading Pincode JSON...")
with open(PINCODE_JSON, "r") as f:
    pin_data = json.load(f)

pin_df = pd.DataFrame(pin_data)

# normalize pincode dataset
pin_df["officename_clean"] = pin_df["officename"].apply(normalize_text)
pin_df["district_clean"] = pin_df["district"].str.lower()

# ==============================================================================
# 4. MATCH FUNCTION
# ==============================================================================

def find_pincode(destination, district):
    if not destination:
        return None, None, 0

    dest_clean = normalize_text(destination)
    district_clean = (district or "").lower()

    # filter by district first (VERY IMPORTANT)
    subset = pin_df[pin_df["district_clean"] == district_clean]

    if subset.empty:
        subset = pin_df  # fallback if district mismatch

    choices = subset["officename_clean"].tolist()

    if not choices:
        return None, None, 0

    match, score, idx = process.extractOne(
        dest_clean,
        choices,
        scorer=fuzz.token_sort_ratio
    )

    if score >= 80:
        matched_row = subset.iloc[idx]
        return matched_row["pincode"], matched_row["officename"], score

    # fallback: district HQ (best effort)
    fallback = subset.iloc[0]
    return fallback["pincode"], fallback["officename"], score


# ==============================================================================
# 5. PROCESS
# ==============================================================================

print("Processing matches...")

results = []

for i, row in df.iterrows():
    destination = clean_value(row["destination"])
    district = clean_value(row["district"])

    pincode, matched_office, score = find_pincode(destination, district)

    results.append({
        "institution": row["institution"],
        "zone": row["zone"],
        "district": district,
        "destination": destination,
        "matched_office": matched_office,
        "pincode": pincode,
        "confidence": score
    })

    if i % 50 == 0:
        print(f"Processed {i} rows...")

# ==============================================================================
# 6. SAVE
# ==============================================================================

output_df = pd.DataFrame(results)
output_df.to_csv(OUTPUT_FILE, index=False)

print(f"\n✅ Done! Saved to {OUTPUT_FILE}")