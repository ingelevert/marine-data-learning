import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
#Load vessel URLs from CSV
input_csv = "/Users/levilina/Documents/Coding/marine-data-learning/data/raw/Hyperlinked_Vessel_URLs.csv"  # Replace with your actual file name
url_column_name = "url"         # Adjust if your column has a different header
df_urls = pd.read_csv(input_csv)
urls = df_urls[url_column_name].dropna().unique().tolist()

#Function to extract vessel data from each page (HTML)
def extract_vessel_data(soup):
    try:
        table = soup.find("table", class_="table")
        rows = table.find_all("tr")

        data = {}
        for row in rows:
            cols = row.find_all("td")
            if len(cols) == 2:
                key = cols[0].text.strip()
                value = cols[1].text.strip()
                data[key] = value

        headline = soup.find("div", class_="shipyard-small-info")
        if headline:
            imo = headline.text.split("IMO:")[-1].split("|")[0].strip()
            data["IMO"] = imo

        description = soup.find("p", style=lambda s: s and "margin-top" in s)
        if description:
            data["Full Description"] = description.text.strip()

        return data

    except Exception as e:
        print("Error parsing:", e)
        return {}

#Loops through each URL and scrape data
results = []
for url in urls:
    print(f"Scraping {url}...")
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        vessel_data = extract_vessel_data(soup)
        vessel_data["URL"] = url
        results.append(vessel_data)
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
    time.sleep(1)  # ill be polite and not cook the server or get blocked

#Save all results to CSV
output_csv = "scraped_vessel_data.csv"
pd.DataFrame(results).to_csv(output_csv, index=False)
print(f"✅ Done! Data saved to {output_csv}")
