import pandas as pd

# Step 1: Read the CSV file. Ensure the CSV is in the same folder as this script.
try:
    df = pd.read_csv('/Users/levilina/Documents/Coding/marine-data-learning/data/processed/gfw_vessel_data.csv', header=None, engine='python')
except Exception as e:
    print("Error reading CSV:", e)
    exit()

print("Initial Data Preview:")
print(df.head())

# Step 2: If your data is all in one column, split it based on commas.
if df.shape[1] == 1:
    df = df[0].str.split(',', expand=True)
    # Strip extra whitespace from each cell.
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Rename columns generically. Adjust these names as needed.
    df.columns = [f'Column{i+1}' for i in range(df.shape[1])]

print("\nCleaned Data Preview:")
print(df.head())

# Step 3: Save the cleaned data to a new CSV file.
df.to_csv('gfw_vessel_data_clean.csv', index=False)
print("\nCleaned data saved to 'gfw_vessel_data_clean.csv'")