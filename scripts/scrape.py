import requests
from bs4 import BeautifulSoup
import csv
import time
import re

CATALOG_BASE = "https://www.trusteddocks.com/catalog/vessels/country/189-senegal?page="
VESSEL_BASE = "https://www.trusteddocks.com"
OUTPUT_FILE = "data/trusteddocks_scraped_senegal.csv"

MAX_PAGES = 25


def get_vessel_links(page):
    url = f"{CATALOG_BASE}{page}"
    print(f"🌐 Fetching catalog page {page}...")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Failed to load page {page}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.select("a[href^='/vessel/']")
    vessel_urls = list({VESSEL_BASE + link['href'] for link in links})
    return vessel_urls


def get_text_after_label(soup, label):
    labels = soup.find_all(string=re.compile(f"^{label}[:\|]?", re.IGNORECASE))
    for lbl in labels:
        next_elem = lbl.find_next()
        if next_elem:
            return next_elem.text.strip()
    return ""


def scrape_vessel_info(url):
    print(f"🔍 Scraping vessel: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Failed to fetch vessel: {url}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    name_tag = soup.find("h1")
    name = name_tag.get_text(strip=True) if name_tag else ""

    summary_tag = soup.find("div", class_="vessel-summary")
    summary_text = summary_tag.get_text(" ", strip=True) if summary_tag else ""

    match_imo = re.search(r"IMO[:\|]?\s*(\d+)", summary_text)
    imo = match_imo.group(1) if match_imo else ""

    match_type = re.search(r"\d+\s*-\s*(.*?)(,|$)", summary_text)
    vessel_type = match_type.group(1).strip() if match_type else ""

    data = {
        "url": url,
        "imo": imo,
        "name": name,
        "type": vessel_type,
        "flag": get_text_after_label(soup, "Flag"),
        "length_m": get_text_after_label(soup, "Length (m/ft)").split("/")[0].strip(),
        "beam_m": get_text_after_label(soup, "Beam (m/ft)").split("/")[0].strip(),
        "builder": get_text_after_label(soup, "Builder"),
        "year_built": get_text_after_label(soup, "Year of Build"),
        "callsign": get_text_after_label(soup, "Callsign"),
        "mmsi": get_text_after_label(soup, "MMSI")
    }

    if not data["imo"] and not data["name"]:
        return None

    return data


def main():
    all_links = []
    for page in range(MAX_PAGES):
        links = get_vessel_links(page)
        all_links.extend(links)
        time.sleep(0.4)

    all_data = []
    for url in all_links:
        result = scrape_vessel_info(url)
        if result:
            all_data.append(result)
        time.sleep(0.5)

    if all_data:
        keys = sorted({key for d in all_data for key in d.keys()})
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_data)

        print(f"\n✅ Done! Saved {len(all_data)} vessels to {OUTPUT_FILE}")
    else:
        print("⚠️ No vessel data was scraped.")


if __name__ == "__main__":
    main()