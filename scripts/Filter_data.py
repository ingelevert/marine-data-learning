import pandas as pd
import re
from pathlib import Path  # Import Path for handling URLs

# Load the scraped data
file_path = "/Users/levilina/Documents/Coding/marine-data-learning/data/raw/scraped_vessel_data.csv"
df = pd.read_csv(file_path)

# Extract vessel name from Full Description or URL
def extract_name(row):
    if pd.notna(row['Full Description']):
        # Look for something that resembles a name before IMO
        match = re.search(r'^([A-Z0-9\- ]+?)(?:\s*IMO|\s*$)', row['Full Description'], re.IGNORECASE)
        if match:
            return match.group(1).strip().title()
    if pd.notna(row['URL']):
        return Path(row['URL']).stem.replace('-', ' ').title()
    return None

# Apply extraction
df['name'] = df.apply(extract_name, axis=1)

# Cleaned DataFrame with relevant fields
df_cleaned = df[['name', 'IMO', 'URL']].dropna(subset=['IMO', 'name'])

# Save the cleaned data to a new CSV file
output_file = "/Users/levilina/Documents/Coding/marine-data-learning/data/processed/cleaned_vessel_data.csv"
df_cleaned.to_csv(output_file, index=False, header=True)
print(f"✅ Cleaned data saved to {output_file}")