import requests
import time
import csv
import pandas as pd
try:
    from gfw_utils import get_headers, get_base_url
except ImportError:
    raise ImportError("❌ 'gfw_utils' module is missing. Ensure it is installed and accessible.")

BASE_URL = get_base_url()
HEADERS = get_headers()
INPUT_FILE = "/Users/levilina/Documents/Coding/marine-data-learning/data/raw/Cleaned_Senegal_Fleet.csv"  # You must provide this CSV with a 'Name' column
OUTPUT_FILE = "data/enriched_senegal_fleet.csv"

# Simple pattern-based tagging of ownership
OWNER_TAGS = {
    "pereira": "🇪🇸 Spanish",
    "soperka": "🇪🇸 Spanish",
    "zhejiang": "🇨🇳 Chinese",
    "dalian": "🇨🇳 Chinese",
    "france": "🇫🇷 French",
    "sen": "🇸🇳 Senegalese",
    "société": "🇸🇳 Senegalese",
    "sn": "🇸🇳 Senegalese"
}

def tag_owner_nationality(owner_string):
    if not owner_string:
        return "❓ Unknown"
    lowered = owner_string.lower()
    for keyword, tag in OWNER_TAGS.items():
        if keyword in lowered:
            return tag
    return "❓ Unknown"

def query_vessel_by_name(name):
    url = f"{BASE_URL}/vessels/search"
    params = {
        "query": name,
        "datasets[0]": "public-global-vessel-identity:latest"
    }
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"❌ Failed to fetch vessel '{name}': {response.status_code}")
        return None

    entries = response.json().get("entries", [])
    for entry in entries:
        flag = entry.get("selfReportedInfo", [{}])[0].get("flag")
        geartypes = entry.get("combinedSourcesInfo", [{}])[0].get("geartypes", [])
        geartype_names = [g["name"] for g in geartypes]

        if flag == "SEN" and any("FISHING" in g.upper() for g in geartype_names):
            owners_raw = "; ".join(
                [owner.get("name", "") for owner in entry.get("registryOwners", [])]
            )
            return {
                "shipname": entry.get("selfReportedInfo", [{}])[0].get("shipname", ""),
                "imo": entry.get("selfReportedInfo", [{}])[0].get("imo", ""),
                "callsign": entry.get("selfReportedInfo", [{}])[0].get("callsign", ""),
                "flag": flag,
                "geartypes": ", ".join(geartype_names),
                "vesselId": entry.get("selfReportedInfo", [{}])[0].get("id", ""),
                "owners": owners_raw,
                "flagged_owner": tag_owner_nationality(owners_raw)
            }
    return None

def main():
    df = pd.read_csv(INPUT_FILE)
    enriched_data = []

    for name in df["Name"].dropna().unique():
        print(f"🔍 Looking up vessel: {name}")
        vessel = query_vessel_by_name(name)
        if vessel:
            enriched_data.append(vessel)
        time.sleep(0.1)  # Rate limit friendly

    # Save results
    if enriched_data:
        keys = ["shipname", "imo", "callsign", "flag", "geartypes", "vesselId", "owners", "flagged_owner"]
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(enriched_data)
        print(f"\n📄 Done! {len(enriched_data)} vessels saved to {OUTPUT_FILE}")
    else:
        print("⚠️ No matching vessels found.")

if __name__ == "__main__":
    main()
