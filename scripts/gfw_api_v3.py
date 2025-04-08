import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

class GFWApiClient:
    """Client for interacting with Global Fishing Watch API v3 based on testing results"""
    
    def __init__(self, env_path=None):
        """Initialize the GFW API client"""
        # Load environment variables
        if env_path is None:
            env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        
        load_dotenv(dotenv_path=env_path)
        
        # Get token and base URL
        self.token = os.getenv("GFW_API_TOKEN")
        if not self.token:
            raise ValueError("GFW_API_TOKEN not found in environment variables")
            
        self.base_url = "https://gateway.api.globalfishingwatch.org/v3"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def _handle_request(self, endpoint, params=None, method="GET"):
        """Handle API requests with error handling and rate limiting"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=self.headers, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Response body: {e.response.text}")
            raise
        except Exception as e:
            print(f"Error: {e}")
            raise
    
    def get_datasets(self, limit=100, offset=0, include="metadata"):
        """Get available datasets"""
        params = {
            "limit": limit,
            "offset": offset,
            "include": include
        }
        return self._handle_request("/datasets", params=params)
    
    def get_public_datasets(self):
        """Get public datasets that might be available to all users"""
        # Try to get well-known public datasets
        known_public_datasets = [
            "public-global-vessel-identity:latest",
            "public-global-fishing-effort:latest",
            "public-global-vessel-tracking:latest"
        ]
        
        results = []
        for dataset_id in known_public_datasets:
            try:
                dataset = self.get_dataset(dataset_id)
                results.append(dataset)
                print(f"Successfully retrieved dataset: {dataset_id}")
            except Exception as e:
                print(f"Could not access dataset {dataset_id}: {e}")
                
        return results
    
    def get_dataset(self, dataset_id):
        """Get a specific dataset by ID"""
        endpoint = f"/datasets/{dataset_id}"
        return self._handle_request(endpoint)
    
    def search_vessels(self, datasets, query=None, limit=10, offset=0):
        """
        Search for vessels
        
        Args:
            datasets (list): List of dataset IDs (required)
            query (str): Search query
            limit (int): Results per page
            offset (int): Pagination offset
        """
        if not datasets:
            raise ValueError("At least one dataset ID is required for vessel search")
            
        params = {
            "datasets": ",".join(datasets) if isinstance(datasets, list) else datasets,
            "limit": limit,
            "offset": offset
        }
        
        if query:
            params["query"] = query
            
        return self._handle_request("/vessels", params=params)
    
    def get_vessel_tracks(self, dataset_id, vessel_id, start_date, end_date):
        """Get vessel tracks"""
        endpoint = f"/vessels/{dataset_id}/{vessel_id}/tracks"
        params = {
            "start_date": start_date,
            "end_date": end_date
        }
        return self._handle_request(endpoint, params)
    
    def save_to_json(self, data, filename):
        """Save API response to JSON file"""
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {filename}")