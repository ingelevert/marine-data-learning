from gfw_api_v3 import GFWApiV3Client
import json

def main():
    # Create a client instance
    client = GFWApiV3Client()
    
    print("\n===== TESTING GFW API v3 CONNECTION =====\n")
    
    endpoints_to_try = [
        # Simple tests to discover API structure
        {"name": "Get datasets", "function": client.get_datasets, "args": {}},
        {"name": "Test vessels API", "function": client.vessels_api_test, "args": {}},
        {"name": "Get available endpoints", "function": client.get_available_endpoints, "args": {}},
        
        # Try to discover a known dataset to use in subsequent requests
        {"name": "Get fishing vessels", "function": client.search_fishing_vessels, 
         "args": {"flag": "SEN"}},
    ]
    
    results = {}
    
    # Try each endpoint
    for test in endpoints_to_try:
        print(f"\n----- Testing: {test['name']} -----")
        try:
            result = test["function"](**test["args"])
            print(f"SUCCESS! Response received")
            
            # Save successful results
            results[test["name"]] = {
                "success": True,
                "data": result
            }
            
            # Print summary of the response
            if isinstance(result, dict):
                print(f"Response keys: {list(result.keys())}")
                
                # If we got entries, print the count
                if "entries" in result:
                    print(f"Found {len(result['entries'])} entries")
                    
                    # If we have at least one entry, print some details
                    if result["entries"]:
                        print("\nSample entry:")
                        entry = result["entries"][0]
                        for key, value in entry.items():
                            print(f"  {key}: {value}")
            else:
                print(f"Response type: {type(result)}")
            
        except Exception as e:
            print(f"FAILED: {e}")
            results[test["name"]] = {
                "success": False,
                "error": str(e)
            }
    
    # Save all results to a file for reference
    with open("gfw_api_test_results.json", "w") as f:
        # Use a simple dict conversion for the results to make them JSON serializable
        simplified_results = {k: {"success": v["success"], 
                                 "error": v.get("error", None),
                                 "data_keys": list(v["data"].keys()) if v["success"] and isinstance(v["data"], dict) else None}
                             for k, v in results.items()}
        json.dump(simplified_results, f, indent=2)
    
    print("\n\n===== TESTING COMPLETE =====")
    print(f"Results saved to gfw_api_test_results.json")
    
    # Check if any tests succeeded
    successes = [name for name, result in results.items() if result["success"]]
    if successes:
        print(f"\nSuccessful endpoints: {', '.join(successes)}")
    else:
        print("\nNo successful API calls. Please verify your API token and permissions.")

if __name__ == "__main__":
    main()