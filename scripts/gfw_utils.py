import os
from dotenv import load_dotenv

# Load from absolute path
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=env_path)

def load_token():
    return os.getenv("GFW_API_TOKEN")

def get_base_url():
    return "https://gateway.api.globalfishingwatch.org/v3"

def get_headers():
    return {
        "Authorization": f"Bearer {load_token()}"
    }
