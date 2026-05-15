import pandas as pd
import re
from rapidfuzz import process, fuzz

# ==============================================================================
# CONFIG
# ==============================================================================

DEST_FILE = "/Users/rohitgoswami/Downloads/destination_master.xlsx"
DEST_SHEET = "destination_master"

PINCODE_FILE = "NE_pincodes_scraped2.csv"

OUTPUT_FILE = "destination_pincode_final.csv"

FUZZY_THRESHOLD = 90

# ==============================================================================
# HELPERS
# ==============================================================================

def clean_text(value):
    """
    Normalize destination names:
    - lowercase
    - remove brackets and content
    - remove extra spaces
    - remove punctuation
    """

    if pd.isna(value):
        return None

    text = str(value).lower().strip()

    # remove brackets + contents
    text = re.sub(r"\(.*?\)", "", text)

    # remove punctuation
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    # collapse spaces
    text = re.sub(r"\s+", " ", text).strip()

    return text


# ==============================================================================
# LOAD FILES
# ==============================================================================

print("Loading destination master...")
dest_df = pd.read_excel(
    DEST_FILE,
    sheet_name=DEST_SHEET
)

print("Loading pincode dataset...")
pin_df = pd.read_csv(PINCODE_FILE)

# ==============================================================================
# CLEAN DESTINATION MASTER
# ==============================================================================

dest_df["destination_clean"] = dest_df["destination"].apply(clean_text)

# remove empty
dest_df = dest_df[
    dest_df["destination_clean"].notna()
]

# remove duplicates AFTER cleaning
dest_df = dest_df.drop_duplicates(
    subset=["destination_clean"]
)

print(f"Unique cleaned destinations: {len(dest_df)}")

# ==============================================================================
# CLEAN PINCODE DATASET
# ==============================================================================

pin_df["area_clean"] = pin_df["area"].apply(clean_text)

pin_df = pin_df[
    pin_df["area_clean"].notna()
]

# remove duplicates
pin_df = pin_df.drop_duplicates(
    subset=["area_clean"]
)

print(f"Unique cleaned pincode areas: {len(pin_df)}")

# ==============================================================================
# BUILD LOOKUP
# ==============================================================================

exact_lookup = {}

for _, row in pin_df.iterrows():
    exact_lookup[row["area_clean"]] = row["pincode"]

all_areas = pin_df["area_clean"].tolist()

# ==============================================================================
# MATCHING
# ==============================================================================

results = []

for _, row in dest_df.iterrows():

    original_destination = row["destination"]
    cleaned_destination = row["destination_clean"]

    matched_area = None
    matched_pincode = None
    match_type = None
    score = None

    # --------------------------------------------------------------------------
    # EXACT MATCH
    # --------------------------------------------------------------------------

    if cleaned_destination in exact_lookup:

        matched_area = cleaned_destination
        matched_pincode = exact_lookup[cleaned_destination]
        match_type = "exact"
        score = 100

    # --------------------------------------------------------------------------
    # FUZZY MATCH
    # --------------------------------------------------------------------------

    else:

        best_match = process.extractOne(
            cleaned_destination,
            all_areas,
            scorer=fuzz.token_sort_ratio
        )

        if best_match:

            area_name, similarity, _ = best_match

            if similarity >= FUZZY_THRESHOLD:

                matched_area = area_name
                matched_pincode = exact_lookup[area_name]
                match_type = "fuzzy"
                score = similarity

    # --------------------------------------------------------------------------
    # SAVE
    # --------------------------------------------------------------------------

    results.append({
        "destination": original_destination,
        "destination_clean": cleaned_destination,
        "matched_area": matched_area,
        "pincode": matched_pincode,
        "match_type": match_type,
        "score": score
    })

# ==============================================================================
# SAVE
# ==============================================================================

final_df = pd.DataFrame(results)

final_df.to_csv(OUTPUT_FILE, index=False)

print(f"\n✅ Done. Output saved to: {OUTPUT_FILE}")

# stats
matched_count = final_df["pincode"].notna().sum()

print(f"Matched: {matched_count}/{len(final_df)}")

unmatched_df = final_df[
    final_df["pincode"].isna()
]

unmatched_df.to_csv(
    "unmatched_destinations.csv",
    index=False
)

print(f"Unmatched exported: {len(unmatched_df)}")