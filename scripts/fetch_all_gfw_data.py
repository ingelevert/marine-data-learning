import pandas as pd
import time
import requests
from gfw_utils import get_headers, get_base_url  # Ensure these are defined in gfw_utils.py

# Load IMOs from the CSV
df = pd.read_csv("/Users/levilina/Documents/Coding/marine-data-learning/data/processed/Cleaned_Vessel_Data.csv")
imos = df['IMO'].dropna().astype(int).unique()

# Prepare output list
all_results = []

# Function to flatten the data
def flatten_data(raw_data):
    flattened = []
    try:
        # Check if raw_data is a dictionary and extract the relevant list
        if isinstance(raw_data, dict):
            # Replace 'entries' with the actual key in raw_data that contains the list of entries
            raw_data = raw_data.get('entries', [])
        
        # Ensure raw_data is now a list
        if not isinstance(raw_data, list):
            print(f"Warning: raw_data is not a list after extraction. Type: {type(raw_data)}")
            return flattened

        for entry in raw_data:
            # Ensure each entry is a dictionary
            if not isinstance(entry, dict):
                print(f"Warning: entry is not a dictionary. Type: {type(entry)}")
                continue

            # Extract relevant fields
            dataset = entry.get('dataset', None)
            shipname = entry.get('selfReportedInfo', [{}])[0].get('shipname', None)
            flag = entry.get('selfReportedInfo', [{}])[0].get('flag', None)
            imo = entry.get('selfReportedInfo', [{}])[0].get('imo', None)
            geartypes = entry.get('combinedSourcesInfo', [{}])[0].get('geartypes', [])
            geartypes = [g.get('name', None) for g in geartypes]
            flattened.append({
                'dataset': dataset,
                'shipname': shipname,
                'flag': flag,
                'imo': imo,
                'geartypes': ', '.join(geartypes)
            })
    except Exception as e:
        print(f"Error flattening data: {e}")
    return flattened

# Loop over IMOs and fetch/flatten results
for i, imo in enumerate(imos):
    print(f"[{i+1}/{len(imos)}] Fetching IMO {imo}...")
    try:
        # Example API call using gfw_utils
        url = f"{get_base_url()}/vessels/search"
        params = {
            "query": imo,
            "datasets[0]": "public-global-vessel-identity:latest"
        }
        headers = get_headers()
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            raw_data = response.json()
            raw_data['imo'] = imo  # Keep track of which IMO the data belongs to
            flattened_data = flatten_data(raw_data)  # Pass raw_data directly
            all_results.extend(flattened_data)
        else:
            print(f"❌ Failed to fetch data for IMO {imo}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching IMO {imo}: {e}")

# Convert results to DataFrame
results_df = pd.DataFrame(all_results)

# Save to CSV
output_file = "/Users/levilina/Documents/Coding/marine-data-learning/data/processed/gfw_vessel_data.csv"
results_df.to_csv(output_file, index=False)
print(f"✅ All done! Data saved to {output_file}")
