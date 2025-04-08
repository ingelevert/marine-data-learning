import pandas as pd

input_path = "/Users/levilina/Documents/Coding/marine-data-learning/data/raw/vessel_analysis_report.csv"
output_path = "/Users/levilina/Documents/Coding/marine-data-learning/data/processed/imo_name_id_only_cleaned.csv"

df = pd.read_csv(input_path)

columns_needed = ["IMO", "Vessel Name", "Vessel ID"]
df_filtered = df[columns_needed]

df_cleaned = df_filtered.dropna(subset=["Vessel ID"])

#save
df_cleaned.to_csv(output_path, index=False)

output_path
