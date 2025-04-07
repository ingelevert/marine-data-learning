#senegal fishing events pulled from gfw api

import requests
import time
import csv
from datetime import datetime
from gfw_utils import get_headers, get_base_url

BASE_URL = get_base_url()
HEADERS = get_headers()
# Example Senegalese vessel IDs (replace with actual IDs or read from a file)
SENEGALESE_VESSEL_IDS = [
    # "9b3e9019d-d67f-005a-9593-b66b997559e5",  # CLAUDINA example
]

START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
OUTPUT_FILE = "data/fishing_events_senegal.csv"

def fetch_fishing_events(vessel_id):
    url = f"{BASE_URL}/events"
    params = {
        "vessels[0]": vessel_id,
        "datasets[0]": "public-global-fishing-events:latest",
        "start-date": START_DATE,
        "end-date": END_DATE,
        "limit": 100,
        "offset": 0
    }

    all_events = []

    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"❌ Failed to fetch events for vessel {vessel_id}: {response.status_code}")
            break

        data = response.json()
        events = data.get("entries", [])
        if not events:
            break

        all_events.extend(events)
        params["offset"] += len(events)

        if "nextOffset" not in data:
            break

        time.sleep(0.3)

    return all_events

def main():
    with open(OUTPUT_FILE, "w", newline="") as f:
        fieldnames = ["vessel_id", "start", "end", "lat", "lon", "distance_from_shore_km"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for vessel_id in SENEGALESE_VESSEL_IDS:
            print(f"🔄 Fetching events for vessel: {vessel_id}")
            events = fetch_fishing_events(vessel_id)

            for event in events:
                writer.writerow({
                    "vessel_id": vessel_id,
                    "start": event.get("start"),
                    "end": event.get("end"),
                    "lat": event.get("position", {}).get("lat"),
                    "lon": event.get("position", {}).get("lon"),
                    "distance_from_shore_km": event.get("distances", {}).get("startDistanceFromShoreKm")
                })

    print(f"✅ Done. Saved fishing events to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
