import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from scripts.gfw_api_v3 import get_headers
from datetime import datetime

# === CONFIGURATION ===
BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
HEADERS = get_headers()
DATASET = "public-global-vessel-identity:latest"
INPUT_PATH = "/Users/levilina/Documents/Coding/marine-data-learning/data/raw/checkcheck.csv"
OUTPUT_PATH = "supertrawler_flags.csv"
MAX_WORKERS = 10

# === LOAD IMO NUMBERS ===
df = pd.read_csv(INPUT_PATH)
df["imo"] = df["imo"].astype("Int64")  # handle float-like IMOs with missing values
df = df.dropna(subset=["imo"])
imos = df["imo"].astype(str).unique().tolist()
if not imos:
    print("❌ No valid IMO numbers found in the input file.")
    exit()

def search_vessel_id_by_imo(imo):
    url = f"{BASE_URL}/vessels/search"
    params = {
        "query": str(imo),
        "datasets[0]": DATASET
    }
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            entries = data.get("entries", [])
            if entries and "selfReportedInfo" in entries[0]:
                return entries[0]["selfReportedInfo"][0].get("id")
    except:
        pass
    return None

def fetch_metadata(vessel_id):
    url = f"{BASE_URL}/vessels/{vessel_id}"
    params = {"dataset": DATASET}
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def fetch_flag_history(vessel_id):
    url = f"{BASE_URL}/vessels/{vessel_id}/flag-history"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return response.json().get("flagHistory", [])
    except:
        pass
    return []

def fetch_sar_detections(vessel_id):
    url = f"{BASE_URL}/sar/detections"
    params = {"vesselId": vessel_id, "matched": "true"}
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if response.status_code == 200:
            return response.json().get("detections", [])
    except:
        pass
    return []

def calculate_years_active(date_from_str, date_to_str):
    try:
        start = datetime.fromisoformat(date_from_str.replace("Z", ""))
        end = datetime.fromisoformat(date_to_str.replace("Z", ""))
        return (end - start).days / 365.25
    except:
        return 0

def is_supertrawler(metadata, flag_history, sar_detections):
    score = 0
    red_flags = []

    gear_flagged = {"TRAWLERS", "PURSE SEINES", "SEINERS"}
    power_threshold = 3000
    tonnage_threshold = 500
    length_threshold = 50

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

    for sr in metadata.get("selfReportedInfo", []):
        if sr.get("messagesCounter", 0) > 500000:
            red_flags.append("High AIS message volume (>500k)")
            score += 1
        if sr.get("transmissionDateFrom") and sr.get("transmissionDateTo"):
            years_active = calculate_years_active(sr["transmissionDateFrom"], sr["transmissionDateTo"])
            if years_active > 3:
                red_flags.append("Long operational period (>3 yrs)")
                score += 1

    if sar_detections:
        red_flags.append(f"Matched SAR detections: {len(sar_detections)} event(s)")
        score += 1

    if len(flag_history) > 1:
        red_flags.append(f"Flag changes detected ({len(flag_history)} entries)")
        score += 1

    return {
        "is_supertrawler": score >= 2,
        "supertrawler_score": score,
        "gear_types": ", ".join(found_gears) if found_gears else None,
        "engine_power_kw": max(power_values) if power_values else None,
        "gross_tonnage": max(tonnage_values) if tonnage_values else None,
        "length_meters": max(length_values) if length_values else None,
        "reasons": "; ".join(red_flags) if red_flags else ""
    }

def process_vessel(imo):
    vessel_id = search_vessel_id_by_imo(imo)
    if not vessel_id:
        return {
            "imo": imo,
            "vessel_id": None,
            "is_supertrawler": False,
            "supertrawler_score": 0,
            "gear_types": None,
            "engine_power_kw": None,
            "gross_tonnage": None,
            "length_meters": None,
            "reasons": "No vessel ID found"
        }
    metadata = fetch_metadata(vessel_id)
    if not metadata:
        return {
            "imo": imo,
            "vessel_id": vessel_id,
            "is_supertrawler": False,
            "supertrawler_score": 0,
            "gear_types": None,
            "engine_power_kw": None,
            "gross_tonnage": None,
            "length_meters": None,
            "reasons": "No metadata"
        }
    flag_history = fetch_flag_history(vessel_id)
    sar_detections = fetch_sar_detections(vessel_id)
    return {
        "imo": imo,
        "vessel_id": vessel_id,
        **is_supertrawler(metadata, flag_history, sar_detections)
    }

def main():
    print(f"🔍 Processing {len(imos)} IMO numbers...")
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_vessel, imo): imo for imo in imos}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    pd.DataFrame(results).to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Results saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
