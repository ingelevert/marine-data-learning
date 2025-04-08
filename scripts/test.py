import requests
from scripts.gfw_api_v3 import get_headers

headers = get_headers()
imo = "9714446"

url = "https://gateway.api.globalfishingwatch.org/v3/vessels/search"
params = {
    "query": imo,
    "datasets[0]": "public-global-vessel-identity:latest"
}

response = requests.get(url, headers=headers, params=params)
print("Status:", response.status_code)
print("Response:", response.text)
