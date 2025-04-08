import pandas as pd
import requests
import os
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime, timedelta
from collections import Counter

# Load environment variables
load_dotenv()
API_TOKEN = os.getenv("GFW_API_TOKEN")
BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

# Configuration
MAX_WORKERS = 10  # Concurrent threads
MAX_RETRIES = 3   # API request retries
RETRY_DELAY = 2   # Seconds between retries
ANALYSIS_YEAR = "2022"  # Year for analysis
OUTPUT_FILE = "senegalese_comprehensive_analysis.csv"

# Threshold settings
FISHING_HOURS_THRESHOLD = 200  # Low fishing activity threshold
ENGINE_POWER_THRESHOLD = 3000  # Industrial vessel threshold (kW)
VESSEL_LENGTH_THRESHOLD = 50   # Large vessel threshold (m)
FOREIGN_PORT_VISIT_THRESHOLD = 0.3  # Foreign port visit percentage threshold
AIS_GAP_THRESHOLD = 48  # Suspicious AIS gaps in hours

def fetch_with_retry(url, params=None):
    """Make API request with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            
            # Handle rate limiting with exponential backoff
            if response.status_code == 429:
                wait_time = (2 ** attempt) * RETRY_DELAY
                print(f"⚠️ Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
                
            print(f"⚠️ API error: HTTP {response.status_code} for {url}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"⚠️ Request error: {e} for {url}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    return None

def lookup_vessel_by_identifiers(imo=None, name=None, ssid=None):
    """Multi-strategy vessel lookup using available identifiers"""
    vessel_data = None
    
    # Try lookup by IMO first
    if imo:
        data = lookup_by_imo(imo)
        if data and data.get("entries") and len(data["entries"]) > 0:
            vessel_data = data["entries"][0]
    
    # Try lookup by name if IMO failed
    if not vessel_data and name and name != "Unknown":
        data = lookup_by_name(name)
        if data and data.get("entries") and len(data["entries"]) > 0:
            # Try to match with IMO if available
            if imo:
                for entry in data["entries"]:
                    registry_info = entry.get("registryInfo", {})
                    if isinstance(registry_info, list) and registry_info:
                        for reg in registry_info:
                            if reg.get("imo") == str(imo):
                                vessel_data = entry
                                break
                    elif isinstance(registry_info, dict) and registry_info.get("imo") == str(imo):
                        vessel_data = entry
                        break
            # Otherwise use first match
            if not vessel_data:
                vessel_data = data["entries"][0]
    
    # Try lookup by SSID if other methods failed
    if not vessel_data and ssid:
        data = lookup_by_id(ssid)
        if data:
            vessel_data = data
    
    # Extract SSID if we found vessel data
    ssid_found = vessel_data.get("id") if vessel_data else None
    
    return vessel_data, ssid_found

def lookup_by_imo(imo):
    """Search for vessel by IMO"""
    url = f"{BASE_URL}/vessels/search"
    params = {
        "query": str(imo),
        "datasets[0]": "public-global-vessel-identity:latest",
        "includes[0]": "OWNERSHIP",
        "includes[1]": "MATCH_CRITERIA",
        "includes[2]": "AUTHORIZATIONS"
    }
    return fetch_with_retry(url, params)

def lookup_by_name(name):
    """Search for vessel by name"""
    url = f"{BASE_URL}/vessels/search"
    params = {
        "query": name,
        "datasets[0]": "public-global-vessel-identity:latest",
        "includes[0]": "OWNERSHIP",
        "includes[1]": "MATCH_CRITERIA",
        "includes[2]": "AUTHORIZATIONS"
    }
    return fetch_with_retry(url, params)

def lookup_by_id(vessel_id):
    """Get vessel details by GFW vessel ID"""
    url = f"{BASE_URL}/vessels/{vessel_id}"
    params = {
        "datasets": "public-global-vessel-identity:latest"
    }
    return fetch_with_retry(url, params)

def get_flag_history(vessel_id):
    """Get flag history for a vessel"""
    url = f"{BASE_URL}/vessels/{vessel_id}/flag-history"
    data = fetch_with_retry(url)
    if data:
        return data.get("flagHistory", [])
    return []

def extract_vessel_details(vessel_data):
    """Extract comprehensive vessel details"""
    if not vessel_data:
        return {
            "ssid": None,
            "name": "Unknown",
            "flag": None,
            "length": None,
            "engine_power": None,
            "tonnage": None,
            "gear_type": None,
            "ship_type": None,
            "ownership": None,
            "authorization_info": []
        }
    
    details = {
        "ssid": vessel_data.get("id"),
        "name": "Unknown",
        "flag": None,
        "length": None,
        "engine_power": None,
        "tonnage": None,
        "gear_type": None,
        "ship_type": None,
        "ownership": None,
        "authorization_info": []
    }
    
    # Extract from self-reported info
    if "selfReportedInfo" in vessel_data:
        srep = vessel_data["selfReportedInfo"]
        if isinstance(srep, list) and srep:
            details["name"] = srep[0].get("shipname") or details["name"]
            details["flag"] = srep[0].get("flag") or details["flag"]
            details["ship_type"] = srep[0].get("shiptype") or details["ship_type"]
        elif isinstance(srep, dict):
            details["name"] = srep.get("shipname") or details["name"]
            details["flag"] = srep.get("flag") or details["flag"]
            details["ship_type"] = srep.get("shiptype") or details["ship_type"]
    
    # Extract from registry info
    if "registryInfo" in vessel_data:
        reg = vessel_data["registryInfo"]
        if isinstance(reg, list) and reg:
            # Process all registry entries for most complete data
            for entry in reg:
                details["name"] = entry.get("vesselName") or details["name"]
                details["flag"] = entry.get("flag") or details["flag"]
                details["length"] = entry.get("lengthMeters") or details["length"]
                details["engine_power"] = entry.get("enginePowerKw") or details["engine_power"]
                details["tonnage"] = entry.get("grossTonnage") or details["tonnage"]
                details["gear_type"] = entry.get("gearType") or details["gear_type"]
        elif isinstance(reg, dict):
            details["name"] = reg.get("vesselName") or details["name"]
            details["flag"] = reg.get("flag") or details["flag"]
            details["length"] = reg.get("lengthMeters") or details["length"]
            details["engine_power"] = reg.get("enginePowerKw") or details["engine_power"]
            details["tonnage"] = reg.get("grossTonnage") or details["tonnage"]
            details["gear_type"] = reg.get("gearType") or details["gear_type"]
    
    # Extract ownership info
    if "ownerOperatorInfo" in vessel_data:
        own_info = vessel_data["ownerOperatorInfo"]
        if isinstance(own_info, list) and own_info:
            owner_data = []
            for entry in own_info:
                owner = entry.get("owner", {})
                if owner:
                    owner_name = owner.get("name")
                    owner_country = owner.get("country")
                    if owner_name and owner_country:
                        owner_data.append(f"{owner_name} ({owner_country})")
            if owner_data:
                details["ownership"] = "; ".join(owner_data)
    
    # Extract authorization info
    if "authorizationInfo" in vessel_data:
        auth_info = vessel_data["authorizationInfo"]
        if isinstance(auth_info, list):
            for entry in auth_info:
                if entry.get("authorizedFrom") and entry.get("authorizedTo"):
                    details["authorization_info"].append({
                        "country": entry.get("country"),
                        "authorized_from": entry.get("authorizedFrom"),
                        "authorized_to": entry.get("authorizedTo")
                    })
    
    return details

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

def fetch_port_visits(vessel_id, start_date, end_date):
    """Get port visits for a vessel within a date range"""
    url = f"{BASE_URL}/events"
    params = {
        "vessels[0]": vessel_id,
        "datasets[0]": "public-global-port-visits-events:latest",
        "start-date": start_date,
        "end-date": end_date,
        "limit": 100
    }
    
    all_visits = []
    offset = 0
    
    while True:
        params["offset"] = offset
        data = fetch_with_retry(url, params)
        if not data or not data.get("entries"):
            break
        
        visits = data["entries"]
        all_visits.extend(visits)
        
        if len(visits) < params["limit"] or "nextOffset" not in data:
            break
        
        offset = data["nextOffset"]
    
    return all_visits

def fetch_ais_gaps(vessel_id, start_date, end_date):
    """Get AIS gap events for a vessel within a date range"""
    url = f"{BASE_URL}/events"
    params = {
        "vessels[0]": vessel_id,
        "datasets[0]": "public-global-gaps-events:latest",
        "start-date": start_date,
        "end-date": end_date,
        "limit": 100
    }
    
    all_gaps = []
    offset = 0
    
    while True:
        params["offset"] = offset
        data = fetch_with_retry(url, params)
        if not data or not data.get("entries"):
            break
        
        gaps = data["entries"]
        all_gaps.extend(gaps)
        
        if len(gaps) < params["limit"] or "nextOffset" not in data:
            break
        
        offset = data["nextOffset"]
    
    return all_gaps

def fetch_encounters(vessel_id, start_date, end_date):
    """Get encounter events for a vessel within a date range"""
    url = f"{BASE_URL}/events"
    params = {
        "vessels[0]": vessel_id,
        "datasets[0]": "public-global-encounters-events:latest",
        "start-date": start_date,
        "end-date": end_date,
        "limit": 100
    }
    
    all_encounters = []
    offset = 0
    
    while True:
        params["offset"] = offset
        data = fetch_with_retry(url, params)
        if not data or not data.get("entries"):
            break
        
        encounters = data["entries"]
        all_encounters.extend(encounters)
        
        if len(encounters) < params["limit"] or "nextOffset" not in data:
            break
        
        offset = data["nextOffset"]
    
    return all_encounters

def analyze_port_visits(port_visits):
    """Analyze port visit patterns"""
    if not port_visits:
        return {
            "total_visits": 0,
            "foreign_visits": 0,
            "foreign_visit_pct": 0,
            "countries_visited": [],
            "most_visited_country": None
        }
    
    total_visits = len(port_visits)
    country_counts = Counter()
    
    for visit in port_visits:
        if "anchorage" in visit and "flag" in visit["anchorage"]:
            country_counts[visit["anchorage"]["flag"]] += 1
    
    senegal_visits = country_counts.get("SEN", 0)
    foreign_visits = total_visits - senegal_visits
    foreign_visit_pct = foreign_visits / total_visits if total_visits > 0 else 0
    
    most_common = country_counts.most_common(5)
    countries_visited = [f"{country}:{count}" for country, count in most_common]
    most_visited = most_common[0][0] if most_common else None
    
    return {
        "total_visits": total_visits,
        "foreign_visits": foreign_visits,
        "foreign_visit_pct": round(foreign_visit_pct, 2),
        "countries_visited": countries_visited,
        "most_visited_country": most_visited
    }

def analyze_ais_gaps(ais_gaps):
    """Analyze AIS gap patterns"""
    if not ais_gaps:
        return {
            "total_gaps": 0,
            "total_gap_hours": 0,
            "max_gap_hours": 0,
            "suspicious_gaps": 0
        }
    
    total_gaps = len(ais_gaps)
    total_gap_hours = 0
    max_gap_hours = 0
    suspicious_gaps = 0
    
    for gap in ais_gaps:
        if "start" in gap and "end" in gap:
            try:
                start = datetime.fromisoformat(gap["start"].replace("Z", ""))
                end = datetime.fromisoformat(gap["end"].replace("Z", ""))
                duration_hours = (end - start).total_seconds() / 3600
                
                total_gap_hours += duration_hours
                max_gap_hours = max(max_gap_hours, duration_hours)
                
                if duration_hours > AIS_GAP_THRESHOLD:
                    suspicious_gaps += 1
            except Exception as e:
                print(f"Error processing AIS gap: {e}")
    
    return {
        "total_gaps": total_gaps,
        "total_gap_hours": round(total_gap_hours, 1),
        "max_gap_hours": round(max_gap_hours, 1),
        "suspicious_gaps": suspicious_gaps
    }

def analyze_encounters(encounters):
    """Analyze encounter patterns"""
    if not encounters:
        return {
            "total_encounters": 0,
            "foreign_encounters": 0,
            "encounter_vessel_flags": []
        }
    
    total_encounters = len(encounters)
    foreign_encounters = 0
    encounter_flags = Counter()
    
    for encounter in encounters:
        if "vessel2" in encounter and "flag" in encounter["vessel2"]:
            flag = encounter["vessel2"]["flag"]
            if flag != "SEN":
                foreign_encounters += 1
                encounter_flags[flag] += 1
    
    encounter_vessel_flags = [f"{flag}:{count}" for flag, count in encounter_flags.most_common(5)]
    
    return {
        "total_encounters": total_encounters,
        "foreign_encounters": foreign_encounters,
        "encounter_vessel_flags": encounter_vessel_flags
    }

def analyze_fishing_activity(fishing_events):
    """Analyze fishing activity patterns"""
    if not fishing_events:
        return {
            "total_hours": 0,
            "events_count": 0,
            "avg_duration_hours": 0
        }
    
    total_hours = 0
    events_count = len(fishing_events)
    
    for event in fishing_events:
        if "start" in event and "end" in event:
            try:
                start = datetime.fromisoformat(event["start"].replace("Z", ""))
                end = datetime.fromisoformat(event["end"].replace("Z", ""))
                duration_hours = (end - start).total_seconds() / 3600
                total_hours += duration_hours
            except Exception as e:
                print(f"Error calculating fishing hours: {e}")
    
    avg_duration = total_hours / events_count if events_count > 0 else 0
    
    return {
        "total_hours": round(total_hours, 1),
        "events_count": events_count,
        "avg_duration_hours": round(avg_duration, 1)
    }

def analyze_flag_history(flag_history):
    """Analyze flag history for evidence of flag hopping"""
    if not flag_history:
        return {
            "flag_changes": 0,
            "previous_flags": [],
            "flag_change_pattern": None
        }
    
    flags = []
    for entry in flag_history:
        if "flag" in entry:
            flags.append(entry["flag"])
    
    unique_flags = set(flags)
    flag_changes = len(flags) - 1 if len(flags) > 0 else 0
    
    # Exclude current SEN flag to get previous flags
    previous_flags = [f for f in unique_flags if f != "SEN"]
    
    # Create flag change pattern
    flag_change_pattern = " → ".join(flags) if flags else None
    
    return {
        "flag_changes": flag_changes,
        "previous_flags": previous_flags,
        "flag_change_pattern": flag_change_pattern
    }

def comprehensive_vessel_analysis(row):
    """Full comprehensive analysis of a vessel"""
    imo = row["IMO"]
    name = row["Vessel Name"]
    
    print(f"Processing IMO {imo} ({name})...")
    
    # ===== Step 1: Find vessel in database =====
    vessel_data, ssid = lookup_vessel_by_identifiers(imo=imo, name=name)
    
    if not vessel_data:
        return {
            "IMO": imo,
            "Vessel Name": name,
            "SSID": None,
            "Flag": None,
            "Classification": "Unknown",
            "Reasons": "No vessel data found in GFW database",
            "Fishing Hours": None,
            "Vessel Length (m)": None,
            "Engine Power (kW)": None,
            "Gross Tonnage (GT)": None,
            "Port Visits": None,
            "Foreign Port %": None,
            "AIS Gaps": None,
            "Suspicious Gaps": None,
            "Encounters": None,
            "Flag Changes": None,
            "Previous Flags": None,
            "Owner Country": None
        }
    
    # ===== Step 2: Extract vessel details =====
    details = extract_vessel_details(vessel_data)
    
    # Set analysis timeframe
    start_date = f"{ANALYSIS_YEAR}-01-01"
    end_date = f"{ANALYSIS_YEAR}-12-31"
    
    # ===== Step 3: Collect all data if we have a vessel ID =====
    fishing_data = None
    port_data = None
    gap_data = None
    encounter_data = None
    flag_history_data = None
    
    if ssid:
        # Fetch all relevant event data
        fishing_events = fetch_fishing_events(ssid, start_date, end_date)
        fishing_data = analyze_fishing_activity(fishing_events)
        
        port_visits = fetch_port_visits(ssid, start_date, end_date)
        port_data = analyze_port_visits(port_visits)
        
        ais_gaps = fetch_ais_gaps(ssid, start_date, end_date)
        gap_data = analyze_ais_gaps(ais_gaps)
        
        encounters = fetch_encounters(ssid, start_date, end_date)
        encounter_data = analyze_encounters(encounters)
        
        flag_history = get_flag_history(ssid)
        flag_history_data = analyze_flag_history(flag_history)
    
    # ===== Step 4: Classify vessel based on all data =====
    classification = "Genuine Senegalese"
    reasons = []
    
    # Check flag
    if details["flag"] and details["flag"] != "SEN":
        classification = "Foreign"
        reasons.append(f"Non-Senegalese flag ({details['flag']})")
    
    # Check flag history
    if flag_history_data and flag_history_data["previous_flags"]:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"Previous flags: {', '.join(flag_history_data['previous_flags'])}")
    
    # Check vessel specifications (supertrawler characteristics)
    if details["engine_power"] and details["engine_power"] > ENGINE_POWER_THRESHOLD:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"High engine power ({details['engine_power']} kW)")
    
    if details["length"] and details["length"] > VESSEL_LENGTH_THRESHOLD:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"Large vessel length ({details['length']} m)")
    
    # Check fishing activity
    if fishing_data and fishing_data["total_hours"] < FISHING_HOURS_THRESHOLD:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"Low fishing activity ({fishing_data['total_hours']} hours)")
    
    # Check port visits
    if port_data and port_data["foreign_visit_pct"] > FOREIGN_PORT_VISIT_THRESHOLD:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"Predominantly visits foreign ports ({port_data['foreign_visit_pct']*100:.1f}%)")
    
    # Check AIS gaps
    if gap_data and gap_data["suspicious_gaps"] > 0:
        if classification == "Genuine Senegalese":
            classification = "Suspect"
        reasons.append(f"Has {gap_data['suspicious_gaps']} suspicious AIS gaps")
    
    # Check ownership
    owner_country = None
    if details["ownership"]:
        # Extract country from ownership string if possible
        owner_info = details["ownership"].lower()
        if "spain" in owner_info or "(esp)" in owner_info:
            owner_country = "ESP"
        elif "china" in owner_info or "(chn)" in owner_info:
            owner_country = "CHN"
        elif "france" in owner_info or "(fra)" in owner_info:
            owner_country = "FRA"
        
        if owner_country and owner_country != "SEN":
            if classification == "Genuine Senegalese":
                classification = "Suspect"
            reasons.append(f"Foreign ownership ({owner_country})")
    
    # ===== Step 5: Compile complete result =====
    result = {
        "IMO": imo,
        "Vessel Name": details["name"] if details["name"] != "Unknown" else name,
        "SSID": ssid,
        "Flag": details["flag"],
        "Classification": classification,
        "Reasons": "; ".join(reasons) if reasons else "No suspicious indicators",
        "Fishing Hours": fishing_data["total_hours"] if fishing_data else None,
        "Vessel Length (m)": details["length"],
        "Engine Power (kW)": details["engine_power"],
        "Gross Tonnage (GT)": details["tonnage"],
        "Port Visits": port_data["total_visits"] if port_data else None,
        "Foreign Port %": port_data["foreign_visit_pct"] if port_data else None,
        "AIS Gaps": gap_data["total_gaps"] if gap_data else None,
        "Suspicious Gaps": gap_data["suspicious_gaps"] if gap_data else None,
        "Encounters": encounter_data["total_encounters"] if encounter_data else None,
        "Flag Changes": flag_history_data["flag_changes"] if flag_history_data else None,
        "Previous Flags": ", ".join(flag_history_data["previous_flags"]) if flag_history_data and flag_history_data["previous_flags"] else None,
        "Owner Country": owner_country,
        "Ownership": details["ownership"],
        "Gear Type": details["gear_type"],
        "Ship Type": details["ship_type"],
        "Countries Visited": ", ".join(port_data["countries_visited"]) if port_data and port_data["countries_visited"] else None,
        "Encounter Flags": ", ".join(encounter_data["encounter_vessel_flags"]) if encounter_data and encounter_data["encounter_vessel_flags"] else None
    }
    
    return result

def main():
    """Main execution function"""
    # Load vessel list
    try:
        vessels_df = pd.read_csv("/Users/levilina/Documents/Coding/marine-data-learning/data/processed/Cleaned_Merged_Vessel_List.csv")
        print(f"✅ Loaded {len(vessels_df)} vessels from input file")
    except Exception as e:
        print(f"❌ Error loading vessel list: {e}")
        return
    
    # Process vessels with multi-threading
    total = len(vessels_df)
    print(f"⏳ Processing {total} vessels using {MAX_WORKERS} parallel workers...")
    
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks and store futures
        future_to_row = {executor.submit(comprehensive_vessel_analysis, row): i 
                        for i, row in vessels_df.iterrows()}
        
        # Process completed futures as they come in
        for i, future in enumerate(as_completed(future_to_row)):
            row_idx = future_to_row[future]
            try:
                result = future.result()
                results.append(result)
                
                if result["Classification"] != "Unknown":
                    print(f"✅ [{i+1}/{total}] IMO {result['IMO']}: {result['Classification']}")
                else:
                    print(f"⚠️ [{i+1}/{total}] IMO {result['IMO']}: No data found")
                
            except Exception as e:
                imo = vessels_df.iloc[row_idx]["IMO"]
                name = vessels_df.iloc[row_idx]["Vessel Name"]
                print(f"❌ Error processing IMO {imo} ({name}): {e}")
                results.append({
                    "IMO": imo,
                    "Vessel Name": name,
                    "SSID": None,
                    "Flag": None,
                    "Classification": "Error",
                    "Reasons": f"Processing error: {str(e)}",
                    "Fishing Hours": None,
                    "Vessel Length (m)": None,
                    "Engine Power (kW)": None,
                    "Gross Tonnage (GT)": None,
                    "Port Visits": None,
                    "Foreign Port %": None,
                    "AIS Gaps": None,
                    "Suspicious Gaps": None,
                    "Encounters": None,
                    "Flag Changes": None,
                    "Previous Flags": None,
                    "Owner Country": None
                })
            
            # Print progress
            if (i+1) % 5 == 0 or i+1 == total:
                print(f"⏳ Progress: {i+1}/{total} vessels processed ({(i+1)/total*100:.1f}%)")
    
    # Save results
    try:
        results_df = pd.DataFrame(results)
        results_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Results saved to {OUTPUT_FILE}")
    except Exception as e:
        print(f"❌ Error saving results: {e}")
    
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

if __name__ == "__main__":
    main()