import requests
import os
import time
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv

def run_stock_job():
    """
    Connects to the Polygon.io API to fetch a complete list of all active stock tickers.
    Handles pagination and rate limiting.

    Returns:
        list: A list of dictionaries, where each dictionary represents a ticker.
              Returns an empty list if the API key is not found or an error occurs.
    """
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
                break # Exit the loop on error

    print(f"\nFinished fetching. Total tickers found: {len(tickers)}")
    return tickers

# This block allows the script to be run directly from the command line
if __name__ == "__main__":
    # Call the function to execute the job
    all_tickers = run_stock_job()
    
    # You could now pass `all_tickers` to another function for processing,
    # like saving to a database or writing to a CSV.
    if all_tickers:
        print(f"\nJob complete. Successfully retrieved {len(all_tickers)} tickers.")
    else:
        print("\nJob finished, but no tickers were retrieved.")