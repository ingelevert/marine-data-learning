import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from scripts.gfw_api_v3 import get_headers

# Define constants
BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
HEADERS = get_headers()
DATASET = "public-global-vessel-identity:latest"

# Load your IMO → vessel ID data
df = pd.read_csv("/Users/levilina/Documents/Coding/marine-data-learning/data/raw/checkcheck.csv")
vessel_ids = [vid for vid in df["vessel_id"].dropna().unique().tolist() if isinstance(vid, str) and len(vid) > 0]
if not vessel_ids:
    print("❌ No valid vessel IDs found in the input file.")
    exit()

def fetch_metadata(vessel_id, retries=3):
    url = f"{BASE_URL}/vessels/{vessel_id}"
    params = {"dataset": DATASET}  # ✅ Correct param (not datasets[0])
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            print(f"❌ Error fetching metadata for vessel ID {vessel_id}: {response.status_code} - {response.text}")
        except requests.exceptions.Timeout:
            print(f"❌ Timeout fetching metadata for vessel ID {vessel_id} (attempt {attempt + 1}/{retries})")
    return None

def is_supertrawler(metadata):
    score = 0
    red_flags = []

    # Settings
    gear_flagged = {"TRAWLERS", "PURSE SEINES", "SEINERS"}
    power_threshold = 3000  # kW
    tonnage_threshold = 500  # gross tonnage
    length_threshold = 50  # meters

    # Found data
    found_gears = set()
    power_values = []
    tonnage_values = []
    length_values = []

    for source in metadata.get("combinedSourcesInfo", []):
        for gear in source.get("geartypes", []):
            gear_name = gear.get("name", "").upper()
            if gear_name in gear_flagged:
                found_gears.add(gear_name)

        if "enginePowerKw" in source:
            power_values.append(source["enginePowerKw"])
        if "grossTonnage" in source:
            tonnage_values.append(source["grossTonnage"])
        if "lengthMeters" in source:
            length_values.append(source["lengthMeters"])

    # Apply logic
    if found_gears:
        red_flags.append(f"Industrial gear: {', '.join(found_gears)}")
        score += 1
    if any(p > power_threshold for p in power_values):
        red_flags.append("High engine power (>3000 kW)")
        score += 1
    if any(t > tonnage_threshold for t in tonnage_values):
        red_flags.append("High gross tonnage (>500 GT)")
        score += 1
    if any(l > length_threshold for l in length_values):
        red_flags.append("Large vessel (>50m)")
        score += 1

    return {
        "is_supertrawler": score >= 2,
        "supertrawler_score": score,
        "gear_types": ", ".join(found_gears) if found_gears else None,
        "engine_power_kw": max(power_values) if power_values else None,
        "gross_tonnage": max(tonnage_values) if tonnage_values else None,
        "length_meters": max(length_values) if length_values else None,
        "reasons": "; ".join(red_flags)
    }

def process_vessel(vessel_id):
    metadata = fetch_metadata(vessel_id)
    if not metadata:
        return {
            "vessel_id": vessel_id,
            "is_supertrawler": False,
            "supertrawler_score": 0,
            "gear_types": None,
            "engine_power_kw": None,
            "gross_tonnage": None,
            "length_meters": None,
            "reasons": "No metadata"
        }
    return {
        "vessel_id": vessel_id,
        **is_supertrawler(metadata)
    }

def main():
    print(f"🔍 Processing {len(vessel_ids)} vessel IDs...")
    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_vessel, vid): vid for vid in vessel_ids}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    pd.DataFrame(results).to_csv("supertrawler_flags.csv", index=False)
    print("✅ Results saved to supertrawler_flags.csv")

if __name__ == "__main__":
    main()
