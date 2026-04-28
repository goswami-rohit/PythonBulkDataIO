from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import re

STATE_URLS = [
    "https://pincodearea.in/Assam-Pincode",
    "https://pincodearea.in/Tripura-Pincode",
    "https://pincodearea.in/Nagaland-Pincode",
    "https://pincodearea.in/Manipur-Pincode",
    "https://pincodearea.in/Mizoram-Pincode",
    "https://pincodearea.in/Meghalaya-Pincode",
    "https://pincodearea.in/Arunachal-Pradesh-Pincode"
]

options = Options()

# 👇 IMPORTANT: point to Brave
options.binary_location = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"

options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)

results = []

def scrape_state(url):
    print(f"\nScraping: {url}")
    
    state = url.split("/")[-1].replace("-Pincode", "")

    driver.get(url)
    time.sleep(2)

    # 🔥 CLICK "SHOW MORE" UNTIL GONE
    previous_count = 0

    while True:
        rows = driver.find_elements(By.XPATH, "//table//tr")

        current_count = len(rows)

        if current_count == previous_count:
            break

        previous_count = current_count

        try:
            button = driver.find_element(
                By.XPATH,
                "//*[contains(text(),'Show more')]"
            )

            driver.execute_script("arguments[0].click();", button)

            time.sleep(1.5)

        except Exception:
            break

    print("  → Fully expanded")

    # get ALL rows from page AFTER expansion
    all_rows = driver.find_elements(By.XPATH, "//tr")

    valid_rows = []

    for row in all_rows:
        try:
            cols = row.find_elements(By.TAG_NAME, "td")

            if len(cols) < 2:
                continue

            area = cols[0].text.strip()
            raw_pin = cols[1].text.strip()

            match = re.search(r"\b\d{6}\b", raw_pin)

            if not match:
                continue

            pincode = match.group(0)

            # NE filtering
            if not (
                pincode.startswith("78")
                or pincode.startswith("79")
            ):
                continue

            valid_rows.append((state, area, pincode))

        except Exception as e:
            print(e)
            continue

    print(f"  → Valid rows found: {len(valid_rows)}")

    for state, area, pincode in valid_rows:
        results.append({
            "state": state,
            "area": area,
            "pincode": pincode
        })

# RUN
for url in STATE_URLS:
    scrape_state(url)

driver.quit()

df = pd.DataFrame(results)
df.drop_duplicates(inplace=True)
df.to_csv("NE_pincodes_scraped2.csv", index=False)

print(f"\n✅ Done. Total rows: {len(df)}")