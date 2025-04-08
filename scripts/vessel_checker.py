import pandas as pd
import requests
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime

# Load environment variables
load_dotenv()
API_TOKEN = os.getenv("GFW_API_TOKEN")
BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

# Load vessel IMO list from the csv file i created
vessels_df = pd.read_csv("/Users/levilina/Documents/Coding/marine-data-learning/data/processed/Cleaned_Merged_Vessel_List.csv")

def fetch_vessel_by_imo(imo):
    """Search for vessels using IMO number"""
    url = f"{BASE_URL}/vessels/search"
    params = {
        "query": str(imo),
        "datasets[0]": "public-global-vessel-identity:latest",
        "includes[0]": "OWNERSHIP",
        "includes[1]": "MATCH_CRITERIA"
    }
    
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            # Check if we have actual vessel entries
            if data.get("entries") and len(data["entries"]) > 0:
                return data
        print(f"⚠️ No vessel found for IMO {imo} (HTTP {response.status_code})")
    except Exception as e:
        print(f"❌ Error looking up IMO {imo}: {e}")
    
    return None

def get_vessel_details(imo):
    """Get vessel details from GFW API using IMO number"""
    url = f"{BASE_URL}/vessels/search"
    params = {
        "query": imo,
        "datasets[0]": "public-global-vessel-identity:latest",
        "includes[0]": "OWNERSHIP",
        "includes[1]": "MATCH_CRITERIA"
    }
    
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        data = response.json()
        if data.get("entries") and len(data["entries"]) > 0:
            return data["entries"][0]
    return None

def get_flag_history(vessel_id):
    """Get flag history for a vessel"""
    url = f"{BASE_URL}/vessels/{vessel_id}/flag-history"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json().get("flagHistory", [])
    return []

def fetch_fishing_events(vessel_id, start_date, end_date):
    """Get fishing events for a vessel within a date range"""
    url = f"{BASE_URL}/events"
    params = {
        "vessels[0]": vessel_id,
        "datasets[0]": "public-global-fishing-events:latest",
        "start-date": start_date,
        "end-date": end_date,
        "limit": 100
    }
    
    all_events = []
    next_offset = 0
    
    while True:
        params["offset"] = next_offset
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            break
            
        data = response.json()
        events = data.get("entries", [])
        if not events:
            break
            
        all_events.extend(events)
        
        if "nextOffset" not in data or len(events) < params["limit"]:
            break
        next_offset = data["nextOffset"]
    
    return all_events

def calculate_total_hours(events):
    """Calculate total fishing hours from a list of events"""
    total_hours = 0
    for event in events:
        start_time = datetime.fromisoformat(event.get("start").replace("Z", ""))
        end_time = datetime.fromisoformat(event.get("end").replace("Z", ""))
        duration = end_time - start_time
        hours = duration.total_seconds() / 3600
        total_hours += hours
    return total_hours

def analyze_vessel(imo):
    """Full analysis pipeline for a vessel by IMO"""
    print(f"Processing IMO {imo}...")
    
    # Step 1: Find vessel in GFW database
    vessel_data = fetch_vessel_by_imo(imo)
    if not vessel_data or not vessel_data.get("entries"):
        return {
            "IMO": imo,
            "Vessel Name": "Unknown",
            "Flag": "Unknown", 
            "Fishing Hours": None,
            "Vessel Length (m)": None,
            "Engine Power (kW)": None,
            "Classification": "Unknown",
            "Reason": "No vessel data found"
        }
    
    vessel = vessel_data["entries"][0]
    vessel_id = vessel.get("id")
    
    # Step 2: Extract vessel details
    name = None
    flag = None
    length = None
    power = None
    
    # Parse self-reported info
    if "selfReportedInfo" in vessel:
        srep = vessel["selfReportedInfo"]
        if isinstance(srep, list) and srep:
            name = srep[0].get("shipname")
            flag = srep[0].get("flag")
        elif isinstance(srep, dict):
            name = srep.get("shipname")
            flag = srep.get("flag")
    
    # Parse registry info for technical details
    if "registryInfo" in vessel:
        reg = vessel["registryInfo"]
        if isinstance(reg, list) and reg:
            length = reg[0].get("lengthMeters")
            power = reg[0].get("enginePowerKw")
        elif isinstance(reg, dict):
            length = reg.get("lengthMeters")
            power = reg.get("enginePowerKw")
    
    # Step 3: Get fishing activity data
    fishing_hours = None
    if vessel_id:
        events = fetch_fishing_events(vessel_id, "2022-01-01", "2022-12-31")
        fishing_hours = calculate_total_hours(events)
    
    # Step 4: Classify vessel
    classification = "Genuine Senegalese"
    reasons = []
    
    if flag and flag != "SEN":
        classification = "Foreign"
        reasons.append(f"Non-Senegalese flag ({flag})")
    
    if fishing_hours is not None:
        if fishing_hours < 200:  # Threshold for low fishing activity
            if classification == "Genuine Senegalese":
                classification = "Suspect"
            reasons.append(f"Low fishing activity ({fishing_hours:.1f} hours)")
    
    if power and power > 3000:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"High engine power ({power} kW)")
    
    if length and length > 50:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"Large vessel length ({length} m)")
    
    return {
        "IMO": imo,
        "Vessel Name": name or "Unknown",
        "Flag": flag or "Unknown",
        "Fishing Hours": fishing_hours,
        "Vessel Length (m)": length,
        "Engine Power (kW)": power,
        "Classification": classification,
        "Reason": "; ".join(reasons) if reasons else "No suspicious indicators"
    }

def main():
    # Read IMO numbers from CSV
    df = pd.read_csv("/Users/levilina/Documents/Coding/marine-data-learning/data/processed/Cleaned_Merged_Vessel_List.csv")
    
    # Process vessels with progress tracking
    results = []
    total = len(df)
    
    print(f"Processing {total} vessels...")
    
    for i, row in df.iterrows():
        imo = row["IMO"]
        print(f"[{i+1}/{total}] Analyzing IMO {imo}...")
        result = analyze_vessel(imo)
        results.append(result)
        
        # Print intermediate results
        if (i+1) % 10 == 0:
            print(f"Progress: {i+1}/{total} vessels processed")
    
    # Save results
    results_df = pd.DataFrame(results)
    output_path = "senegalese_vessel_analysis_improved.csv"
    results_df.to_csv(output_path, index=False)
    
    # Summary stats
    foreign = len(results_df[results_df["Classification"] == "Foreign"])
    suspect = len(results_df[results_df["Classification"] == "Suspect"])
    genuine = len(results_df[results_df["Classification"] == "Genuine Senegalese"])
    unknown = len(results_df[results_df["Classification"] == "Unknown"])
    
    print(f"\n=== ANALYSIS COMPLETE ===")
    print(f"Total vessels analyzed: {total}")
    print(f"Foreign vessels: {foreign} ({foreign/total*100:.1f}%)")
    print(f"Suspect vessels: {suspect} ({suspect/total*100:.1f}%)")
    print(f"Genuine Senegalese: {genuine} ({genuine/total*100:.1f}%)")
    print(f"Unknown: {unknown} ({unknown/total*100:.1f}%)")
    print(f"Results saved to {output_path}")

if __name__ == "__main__":
    main()