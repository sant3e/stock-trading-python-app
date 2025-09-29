import requests
import os
import time
import csv
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Check if the key was actually loaded
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY not found. Make sure it's set in your .env file.")

LIMIT = 1000

# Parametrize the base URL
base_url = "https://api.polygon.io/v3/reference/tickers"
params = {
    "market": "stocks",
    "active": "true",
    "order": "asc",
    "limit": LIMIT,
    "sort": "ticker",
    "apiKey": POLYGON_API_KEY
}

tickers = []

with requests.Session() as session:
    next_url = base_url

    while next_url:
        # slice the URL for printing to avoid too long output
        print(f"Fetching data from: {next_url[:100]}...")
        
        if next_url == base_url:
            response = session.get(next_url, params=params)
        else:
            response = session.get(f"{next_url}&apiKey={POLYGON_API_KEY}")

        if response.status_code == 200:
            data = response.json()
            
            results = data.get('results', [])
            tickers.extend(results)
            
            next_url = data.get('next_url')
            
            print(f"Successfully fetched {len(results)} tickers. Total tickers: {len(tickers)}")

            # Add a delay to respect the rate limit (5 requests/minute)
            # From the pricing: https://polygon.io/pricing we see that the free tier allows 5 requests per minute
            # This means we have time per request = 60 seconds / 5 requests = 12 seconds/request (add 1 second buffer)
            if next_url: # Only sleep if there is a next page to fetch
                print("Waiting 13 seconds to respect rate limit...")
                time.sleep(13)

        else:
            print(f"Error fetching data: {response.status_code}")
            print(f"Response Body: {response.text}")
            break

print(f"\nFinished fetching. Total tickers found: {len(tickers)}")

# Save to CSV
if tickers:
    # Define the name of the output file
    csv_filename = "tickers.csv"
    
    # The 'tickers' list contains dictionaries. 
    # Get the CSV headers from the keys of the first dictionary in the list.
    headers = tickers[0].keys()

    # Write to the file (newline='' prevents extra blank rows)
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        # Create a DictWriter object, which can map dictionaries to CSV rows
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        # Write the header row to the CSV file
        writer.writeheader()
        # Write all the ticker data to the CSV file
        writer.writerows(tickers)

    print(f"\nData successfully written to {csv_filename}")
else:
    print("\nNo tickers were fetched, so no CSV file was created.")