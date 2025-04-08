import pandas as pd
import requests
from datetime import datetime
import os
import concurrent.futures
import time

# Import utility functions from your project files.
from gfw_utils import get_base_url, get_headers
from gfw_fetch import fetch_gfw_data  # Function to fetch vessel metadata

# Global constants
BASE_URL = get_base_url()
HEADERS = get_headers()

# Create a global requests.Session to reuse connections.
session = requests.Session()
session.headers.update(HEADERS)

def parse_timestamp(ts):
    """
    Attempt to parse a timestamp first with milliseconds and if that fails, without.
    """
    try:
        # Try parsing with milliseconds (e.g., "2023-12-18T04:07:13.000Z")
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            # Fallback: without milliseconds (e.g., "2023-12-18T04:07:13Z")
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            print(f"Failed to parse timestamp: {ts}")
            return None

def merge_intervals(intervals):
    """
    Merge a list of intervals represented as tuples (start, end).
    Overlapping or contiguous intervals are merged.
    """
    if not intervals:
        return []
    intervals.sort(key=lambda x: x[0])
    merged = [intervals[0]]
    for current in intervals[1:]:
        last = merged[-1]
        if current[0] <= last[1]:
            merged[-1] = (last[0], max(last[1], current[1]))
        else:
            merged.append(current)
    return merged

def calculate_total_hours(events):
    """
    Convert events to (start, end) intervals, merge overlapping intervals,
    then return the sum of durations (in hours) of the merged intervals.
    """
    intervals = []
    for event in events:
        start_str = event.get("start")
        end_str = event.get("end")
        if start_str and end_str:
            start = parse_timestamp(start_str)
            end = parse_timestamp(end_str)
            if start and end:
                intervals.append((start, end))
    merged_intervals = merge_intervals(intervals)
    total_seconds = sum((end - start).total_seconds() for start, end in merged_intervals)
    return total_seconds / 3600.0

def get_vessel_details(imo):
    """
    Look up vessel details by IMO number.
    Returns a dict with vessel_id, name, and flag.
    """
    data = fetch_gfw_data(imo)
    if data and data.get("entries"):
        vessel = data["entries"][0]
        flag = None
        if vessel.get("registryInfo"):
            reg_info = vessel["registryInfo"]
            if isinstance(reg_info, list) and reg_info:
                flag = reg_info[0].get("flag")
            elif isinstance(reg_info, dict):
                flag = reg_info.get("flag")
        if not flag and vessel.get("selfReportedInfo"):
            srep = vessel["selfReportedInfo"]
            if isinstance(srep, list) and srep:
                flag = srep[0].get("flag")
            elif isinstance(srep, dict):
                flag = srep.get("flag")
        vessel_id = None
        name = "Unknown"
        if vessel.get("selfReportedInfo"):
            srep = vessel["selfReportedInfo"]
            if isinstance(srep, list) and srep:
                vessel_id = srep[0].get("id")
                name = srep[0].get("shipname", "Unknown")
            elif isinstance(srep, dict):
                vessel_id = srep.get("id")
                name = srep.get("shipname", "Unknown")
        return {"imo": imo, "vessel_id": vessel_id, "name": name, "flag": flag}
    return None

def fetch_fishing_events(vessel_id, start_date, end_date):
    """
    Fetch all fishing events for a vessel between start_date and end_date.
    Uses the Global Fishing Watch events endpoint.
    """
    url = f"{BASE_URL}/events"
    params = {
        "vessels[0]": vessel_id,
        "datasets[0]": "public-global-fishing-events:latest",
        "start-date": start_date,
        "end-date": end_date,
        "limit": 100,
        "offset": 0
    }
    all_events = []
    while True:
        response = session.get(url, params=params)
        if response.status_code != 200:
            print(f"Error fetching events for vessel {vessel_id}: HTTP {response.status_code}")
            break
        data = response.json()
        events = data.get("entries", [])
        if not events:
            break
        all_events.extend(events)
        params["offset"] += len(events)
        if "nextOffset" not in data or len(events) == 0:
            break
    return all_events

def process_vessel(imo, start_date, end_date, fishing_hours_threshold):
    print(f"Processing IMO {imo} ...")
    details = get_vessel_details(imo)
    if not details:
        print(f"Skipping IMO {imo}: No metadata found.")
        return {
            "IMO": imo,
            "Vessel Name": None,
            "Vessel ID": None,
            "Flag": None,
            "Total Fishing Hours": None,
            "Classification": "No metadata"
        }
    classification = "Genuine"
    flag = details.get("flag")
    if flag != "SEN":
        classification = "Suspect (Non-Senegalese flag)"
        total_hours = None
    else:
        vessel_id = details.get("vessel_id")
        if not vessel_id:
            print(f"Skipping IMO {imo}: No vessel_id available.")
            return {
                "IMO": imo,
                "Vessel Name": details.get("name"),
                "Vessel ID": None,
                "Flag": flag,
                "Total Fishing Hours": None,
                "Classification": "No vessel_id"
            }
        events = fetch_fishing_events(vessel_id, start_date, end_date)
        total_hours = calculate_total_hours(events)
        if total_hours < fishing_hours_threshold:
            classification = "Suspect (Low fishing effort)"
    return {
        "IMO": imo,
        "Vessel Name": details.get("name"),
        "Vessel ID": details.get("vessel_id"),
        "Flag": flag,
        "Total Fishing Hours": total_hours,
        "Classification": classification
    }

def main():
    # ===== CONFIGURATION =====
    scraped_csv_path = "/Users/levilina/Documents/Coding/marine-data-learning/data/processed/Cleaned_Merged_Vessel_List.csv"
    output_csv_path = "vessel_analysis_report.csv"
    # Increase the timeframe to cover 2015 to 2025.
    start_date = "2015-01-01"
    end_date   = "2025-12-31"
    fishing_hours_threshold = 500  # Adjust as needed.
    # ===========================
    
    try:
        df = pd.read_csv(scraped_csv_path)
    except Exception as e:
        print(f"Error reading {scraped_csv_path}: {e}")
        return

    if "IMO" not in df.columns:
        print("The CSV file must contain an 'IMO' column.")
        return

    imos = df["IMO"].tolist()
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_imo = {
            executor.submit(process_vessel, imo, start_date, end_date, fishing_hours_threshold): imo 
            for imo in imos
        }
        for future in concurrent.futures.as_completed(future_to_imo):
            result = future.result()
            results.append(result)
            # Add a short sleep if necessary to ease rate limiting.
            time.sleep(0.1)

    report_df = pd.DataFrame(results)
    report_df.to_csv(output_csv_path, index=False)
    print(f"\nAnalysis complete. Report saved to {output_csv_path}")

if __name__ == "__main__":
    main()
