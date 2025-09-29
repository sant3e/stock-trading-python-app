import requests
import os
import time
import snowflake.connector
from datetime import datetime, timezone
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


def get_snowflake_conn():
    """
    Establishes and returns a Snowflake connection using credentials from environment variables.
    """
    load_dotenv()  # Ensure env vars are loaded

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ZACH_BEGINNER_DB"),  # default to your DB
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),                   # default to your schema
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    return conn


def load_tickers_to_snowflake(tickers, conn, table_name="STOCK_TICKER"):
    """
    Loads a list of ticker dictionaries into the specified Snowflake table.
    Skips loading if data for today's DS already exists.

    Parameters:
        tickers (list): List of ticker dicts from Polygon API.
        conn: Active Snowflake connection.
        table_name (str): Target table name (default: STOCK_TICKER).
    """
    cursor = conn.cursor()
    today = datetime.now(timezone.utc).date() # For the DS column

    try:
        # Check if data for today already exists
        check_sql = f"SELECT COUNT(1) FROM {table_name} WHERE DS = %s"
        cursor.execute(check_sql, (today,))
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"Data for DS={today} already exists in {table_name}. Skipping load.")
            return  # Exit early — no insert needed
        
        # Data insertion
        rows_to_insert = []
        for ticker in tickers:
            row = (
                ticker.get('ticker'),
                ticker.get('name'),
                ticker.get('market'),
                ticker.get('locale'),
                ticker.get('primary_exchange'),
                ticker.get('type'),
                ticker.get('active'),
                ticker.get('currency_name'),
                ticker.get('cik'),
                ticker.get('composite_figi'),
                ticker.get('share_class_figi'),
                ticker.get('last_updated_utc'),  # Should already be ISO format
                today  # DS: DATE type column
            )
            rows_to_insert.append(row)

        # Build insert statement
        insert_sql = f"""
        INSERT INTO {table_name} (
            TICKER, NAME, MARKET, LOCALE, PRIMARY_EXCHANGE, "TYPE", ACTIVE,
            CURRENCY_NAME, CIK, COMPOSITE_FIGI, SHARE_CLASS_FIGI, LAST_UPDATED_UTC, DS
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        print(f"Inserting {len(rows_to_insert)} rows into {table_name}...")
        cursor.executemany(insert_sql, rows_to_insert)
        conn.commit()
        print("Data successfully loaded into Snowflake.")

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


if __name__ == "__main__":
    # Call the function to execute the job
    all_tickers = run_stock_job()
    
    # You could now pass `all_tickers` to another function for processing,
    # like saving to a database or writing to a CSV.
    if all_tickers:
        print(f"\nJob complete. Successfully retrieved {len(all_tickers)} tickers.")

        # Attempt to load into Snowflake. Credentials must be provided via environment variables.
        try:
            conn = get_snowflake_conn()
            try:
                # Default table is PUBLIC.TICKERS; you can override by setting SNOWFLAKE_TABLE env var
                table = os.getenv('SNOWFLAKE_TABLE', 'PUBLIC.TICKERS')
                load_tickers_to_snowflake(all_tickers, conn, table_name=table)
            finally:
                conn.close()
        except Exception as e:
            print('Error loading to Snowflake:', str(e))
    else:
        print("\nJob finished, but no tickers were retrieved.")