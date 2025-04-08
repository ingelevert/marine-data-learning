import pandas as pd
import requests
import os
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime

# Load environment variables
load_dotenv()
API_TOKEN = os.getenv("GFW_API_TOKEN")
BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

# Configuration - adjust as needed
MAX_WORKERS = 10  # Number of concurrent threads
MAX_RETRIES = 3   # Number of API request retries
RETRY_DELAY = 2   # Seconds between retries
FISHING_HOURS_THRESHOLD = 200  # Threshold for low fishing activity in hours
ENGINE_POWER_THRESHOLD = 3000  # Threshold for industrial vessels in kW
VESSEL_LENGTH_THRESHOLD = 50   # Threshold for large vessels in meters

def fetch_with_retry(url, params=None, max_retries=MAX_RETRIES):
    """Make API request with retry logic"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=20)
            if response.status_code == 200:
                return response.json()
            
            # If we're rate limited, wait with exponential backoff
            if response.status_code == 429:
                wait_time = (2 ** attempt) * RETRY_DELAY
                print(f"⚠️ Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
                
            # For other errors, print info and retry after delay
            print(f"⚠️ API error: HTTP {response.status_code} for {url}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"⚠️ Request error: {e} for {url}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
    
    return None

def lookup_vessel(imo=None, name=None, ssid=None):
    """
    Multi-strategy vessel lookup that tries different identifiers in sequence:
    1. IMO number
    2. Vessel name
    3. SSID (vessel ID)
    """
    # Try IMO lookup first if available
    if imo:
        data = lookup_by_imo(imo)
        if data and data.get("entries") and len(data["entries"]) > 0:
            return data["entries"][0]
    
    # Try name lookup if IMO failed and name is available
    if name and name != "Unknown":
        data = lookup_by_name(name)
        if data and data.get("entries") and len(data["entries"]) > 0:
            # If we have the IMO, try to find a matching entry
            if imo:
                for entry in data["entries"]:
                    registry_info = entry.get("registryInfo", {})
                    if isinstance(registry_info, list) and registry_info:
                        for reg in registry_info:
                            if reg.get("imo") == str(imo):
                                return entry
                    elif isinstance(registry_info, dict) and registry_info.get("imo") == str(imo):
                        return entry
            # Otherwise return first match
            return data["entries"][0]
    
    # Try SSID lookup if available and previous methods failed
    if ssid:
        data = lookup_by_id(ssid)
        if data:
            return data
    
    return None

def lookup_by_imo(imo):
    """Search for vessel by IMO"""
    url = f"{BASE_URL}/vessels/search"
    params = {
        "query": str(imo),
        "datasets[0]": "public-global-vessel-identity:latest",
        "includes[0]": "OWNERSHIP",
        "includes[1]": "MATCH_CRITERIA"
    }
    return fetch_with_retry(url, params)

def lookup_by_name(name):
    """Search for vessel by name"""
    url = f"{BASE_URL}/vessels/search"
    params = {
        "query": name,
        "datasets[0]": "public-global-vessel-identity:latest",
        "includes[0]": "OWNERSHIP",
        "includes[1]": "MATCH_CRITERIA"
    }
    return fetch_with_retry(url, params)

def lookup_by_id(vessel_id):
    """Get vessel details by GFW vessel ID"""
    url = f"{BASE_URL}/vessels/{vessel_id}"
    params = {
        "dataset": "public-global-vessel-identity:latest"
    }
    return fetch_with_retry(url, params)

def get_flag_history(vessel_id):
    """Get flag history for a vessel"""
    url = f"{BASE_URL}/vessels/{vessel_id}/flag-history"
    data = fetch_with_retry(url)
    if data:
        return data.get("flagHistory", [])
    return []

def fetch_fishing_events(vessel_id, start_date, end_date):
    """Get fishing events for a vessel within a date range"""
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
        data = fetch_with_retry(url, params)
        if not data:
            break
            
        events = data.get("entries", [])
        if not events:
            break
            
        all_events.extend(events)
        
        if "nextOffset" not in data or len(events) < params["limit"]:
            break
        params["offset"] = data["nextOffset"]
    
    return all_events

def calculate_fishing_hours(events):
    """Calculate total fishing hours from a list of events"""
    total_hours = 0
    for event in events:
        if "start" in event and "end" in event:
            try:
                start_time = datetime.fromisoformat(event.get("start").replace("Z", ""))
                end_time = datetime.fromisoformat(event.get("end").replace("Z", ""))
                duration = end_time - start_time
                hours = duration.total_seconds() / 3600
                total_hours += hours
            except Exception as e:
                print(f"⚠️ Error calculating hours: {e}")
    return total_hours

def extract_vessel_details(vessel_data):
    """Extract relevant vessel details from API response"""
    if not vessel_data:
        return {
            "ssid": None,
            "name": "Unknown",
            "flag": None,
            "length": None,
            "engine_power": None,
            "tonnage": None
        }
    
    details = {
        "ssid": vessel_data.get("id"),
        "name": "Unknown",
        "flag": None,
        "length": None,
        "engine_power": None,
        "tonnage": None
    }
    
    # Extract data from self-reported info
    if "selfReportedInfo" in vessel_data:
        srep = vessel_data["selfReportedInfo"]
        if isinstance(srep, list) and srep:
            details["name"] = srep[0].get("shipname") or details["name"]
            details["flag"] = srep[0].get("flag") or details["flag"]
        elif isinstance(srep, dict):
            details["name"] = srep.get("shipname") or details["name"]
            details["flag"] = srep.get("flag") or details["flag"]
    
    # Extract data from registry info
    if "registryInfo" in vessel_data:
        reg = vessel_data["registryInfo"]
        if isinstance(reg, list) and reg:
            # Take the most recent registry entry
            for entry in reg:
                details["name"] = entry.get("vesselName") or details["name"]
                details["flag"] = entry.get("flag") or details["flag"]
                details["length"] = entry.get("lengthMeters") or details["length"]
                details["engine_power"] = entry.get("enginePowerKw") or details["engine_power"]
                details["tonnage"] = entry.get("grossTonnage") or details["tonnage"]
        elif isinstance(reg, dict):
            details["name"] = reg.get("vesselName") or details["name"]
            details["flag"] = reg.get("flag") or details["flag"]
            details["length"] = reg.get("lengthMeters") or details["length"]
            details["engine_power"] = reg.get("enginePowerKw") or details["engine_power"]
            details["tonnage"] = reg.get("grossTonnage") or details["tonnage"]
    
    return details

def analyze_vessel(row):
    """Full analysis of a vessel using IMO and name"""
    imo = row["IMO"]
    name = row["Vessel Name"]
    ssid = None  # Will be populated if found
    
    print(f"Processing IMO {imo} ({name})...")
    
    # Step 1: Find vessel in GFW database using multi-strategy lookup
    vessel_data = lookup_vessel(imo=imo, name=name, ssid=ssid)
    
    if not vessel_data:
        return {
            "IMO": imo,
            "Vessel Name": name,
            "SSID": None,
            "Flag": None,
            "Fishing Hours": None, 
            "Vessel Length (m)": None,
            "Engine Power (kW)": None,
            "Gross Tonnage (GT)": None,
            "Classification": "Unknown",
            "Reason": "No vessel data found"
        }
    
    # Step 2: Extract vessel details
    details = extract_vessel_details(vessel_data)
    ssid = details["ssid"]
    
    # Step 3: Get flag history
    flag_history = []
    if ssid:
        flag_history = get_flag_history(ssid)
    
    # Step 4: Get fishing activity data
    fishing_hours = None
    if ssid:
        start_date = "2022-01-01"
        end_date = "2022-12-31"
        events = fetch_fishing_events(ssid, start_date, end_date)
        fishing_hours = calculate_fishing_hours(events)
    
    # Step 5: Classify vessel
    classification = "Genuine Senegalese"
    reasons = []
    
    # Check flag
    if details["flag"] and details["flag"] != "SEN":
        classification = "Foreign"
        reasons.append(f"Non-Senegalese flag ({details['flag']})")
    
    # Check flag history for flag hopping
    previous_flags = []
    for entry in flag_history:
        if entry.get("flag") and entry.get("flag") != "SEN" and entry.get("flag") not in previous_flags:
            previous_flags.append(entry.get("flag"))
    
    if previous_flags:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"Previous flags: {', '.join(previous_flags)}")
    
    # Check fishing hours
    if fishing_hours is not None:
        if fishing_hours < FISHING_HOURS_THRESHOLD:
            if classification == "Genuine Senegalese":
                classification = "Suspect"
            reasons.append(f"Low fishing activity ({fishing_hours:.1f} hours)")
    
    # Check vessel specifications
    if details["engine_power"] and details["engine_power"] > ENGINE_POWER_THRESHOLD:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"High engine power ({details['engine_power']} kW)")
    
    if details["length"] and details["length"] > VESSEL_LENGTH_THRESHOLD:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"Large vessel length ({details['length']} m)")
    
    return {
        "IMO": imo,
        "Vessel Name": details["name"] if details["name"] != "Unknown" else name,
        "SSID": ssid,
        "Flag": details["flag"],
        "Fishing Hours": fishing_hours,
        "Vessel Length (m)": details["length"],
        "Engine Power (kW)": details["engine_power"],
        "Gross Tonnage (GT)": details["tonnage"],
        "Classification": classification,
        "Reason": "; ".join(reasons) if reasons else "No suspicious indicators"
    }

def main():
    # Read vessel list
    vessels_df = pd.read_csv("/Users/levilina/Documents/Coding/marine-data-learning/data/processed/Cleaned_Merged_Vessel_List.csv")
    
    # Process vessels with multi-threading
    total = len(vessels_df)
    print(f"Processing {total} vessels using {MAX_WORKERS} parallel workers...")
    
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks and store futures
        future_to_row = {executor.submit(analyze_vessel, row): i 
                        for i, row in vessels_df.iterrows()}
        
        # Process completed futures as they come in
        for i, future in enumerate(as_completed(future_to_row)):
            row_idx = future_to_row[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                imo = vessels_df.iloc[row_idx]["IMO"]
                name = vessels_df.iloc[row_idx]["Vessel Name"]
                print(f"❌ Error processing IMO {imo} ({name}): {e}")
                results.append({
                    "IMO": imo,
                    "Vessel Name": name,
                    "SSID": None,
                    "Flag": None,
                    "Fishing Hours": None,
                    "Vessel Length (m)": None,
                    "Engine Power (kW)": None,
                    "Gross Tonnage (GT)": None,
                    "Classification": "Error",
                    "Reason": f"Processing error: {str(e)}"
                })
            
            # Print progress
            if (i+1) % 5 == 0 or i+1 == total:
                print(f"Progress: {i+1}/{total} vessels processed ({(i+1)/total*100:.1f}%)")
    
    # Save results
    results_df = pd.DataFrame(results)
    output_path = "senegalese_fleet_analysis.csv"
    results_df.to_csv(output_path, index=False)
    
    # Summary stats
    foreign = len(results_df[results_df["Classification"] == "Foreign"])
    suspect = len(results_df[results_df["Classification"] == "Suspect"])
    genuine = len(results_df[results_df["Classification"] == "Genuine Senegalese"])
    unknown = len(results_df[results_df["Classification"] == "Unknown"])
    errors = len(results_df[results_df["Classification"] == "Error"])
    
    print(f"\n=== ANALYSIS COMPLETE ===")
    print(f"Total vessels analyzed: {total}")
    print(f"Foreign vessels: {foreign} ({foreign/total*100:.1f}%)")
    print(f"Suspect vessels: {suspect} ({suspect/total*100:.1f}%)")
    print(f"Genuine Senegalese: {genuine} ({genuine/total*100:.1f}%)")
    print(f"Unknown: {unknown} ({unknown/total*100:.1f}%)")
    print(f"Errors: {errors} ({errors/total*100:.1f}%)")
    print(f"Results saved to {output_path}")

if __name__ == "__main__":
    main()