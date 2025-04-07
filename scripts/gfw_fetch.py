# scripts/gfw_fetch.py

import requests
from gfw_utils import get_base_url, get_headers

def search_vessel_by_imo(imo_number):
    """Search for a vessel by IMO number and return its vesselId."""
    print(f"🔍 Searching for vessel with IMO: {imo_number}")
    url = f"{get_base_url()}/vessels/search"
    params = {
        "query": imo_number,
        "datasets[0]": "public-global-vessel-identity:latest"
    }

    response = requests.get(url, headers=get_headers(), params=params)
    if response.status_code != 200:
        print("❌ Vessel search failed.")
        print(response.status_code, response.text)
        return None

    data = response.json()
    entries = data.get("entries", [])
    if not entries:
        print("⚠️ No vessels found.")
        return None

    vessel_info = entries[0]
    self_reported_info = vessel_info.get("selfReportedInfo", [])
    if not self_reported_info:
        print("⚠️ No selfReportedInfo available.")
        return None

    # Use name from selfReportedInfo since registryInfo is empty
    name = self_reported_info[0].get("shipname", "Unknown")
    vessel_id = self_reported_info[0].get("id")

    print(f"✅ Found vessel: {name}")
    print(f"🆔 Vessel ID: {vessel_id}")
    return vessel_id

def fetch_fishing_event(vessel_id, start_date, end_date):
    """Fetch one fishing event for a given vessel."""
    print(f"\n🎣 Fetching fishing events from {start_date} to {end_date}...")
    url = f"{get_base_url()}/events"
    params = {
        "vessels[0]": vessel_id,
        "datasets[0]": "public-global-fishing-events:latest",
        "start-date": start_date,
        "end-date": end_date,
        "limit": 1,
        "offset": 0
    }

    response = requests.get(url, headers=get_headers(), params=params)
    if response.status_code != 200:
        print("❌ Failed to fetch events.")
        print(response.status_code, response.text)
        return

    data = response.json()
    total = data.get("total", 0)
    print(f"🔢 Events found: {total}")
    if total == 0:
        print("🟡 No events in this range.")
        return

    event = data["entries"][0]
    print(f"📍 Location: lat {event['position']['lat']}, lon {event['position']['lon']}")
    print(f"🕒 From: {event['start']} to {event['end']}")
    print(f"⚓ Distance from shore: {event['distances']['startDistanceFromShoreKm']} km")

if __name__ == "__main__":
    imo = "7831410"
    start_date = "2017-03-01"
    end_date = "2017-03-31"

    vessel_id = search_vessel_by_imo(imo)
    if vessel_id:
        fetch_fishing_event(vessel_id, start_date, end_date)
