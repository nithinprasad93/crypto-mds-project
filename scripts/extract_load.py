import os
import sys
import json
import requests
import snowflake.connector

def fetch_crypto_prices(api_key):
    """Fetches top 10 cryptocurrencies by market cap."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "aud",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false",
        "x_cg_demo_api_key": api_key
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error fetching CoinGecko data: {response.text}")
        sys.exit(1)
    return response.json()

def fetch_crypto_news(api_key):
    """Fetches recent news headlines mentioning cryptocurrency."""
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "cryptocurrency OR bitcoin OR ethereum",
        "sortBy": "publishedAt",
        "pageSize": 20,
        "language": "en",
        "apiKey": api_key
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error fetching NewsAPI data: {response.text}")
        sys.exit(1)
    return response.json().get("articles", [])

def load_to_snowflake(data_list, table_name):
    """Connects to Snowflake and appends raw JSON rows."""
    # Connect using our secure Environment Variables
    conn = snowflake.connector.connect(
        user=os.getenv('SNOW_USER'),
        password=os.getenv('SNOW_PASS'),
        account=os.getenv('SNOW_ACCOUNT'),
        role='GITHUB_ACTIONS_ROLE',
        warehouse='COMPUTE_WH',
        database='CRYPTO_DB',
        schema='RAW'
    )
    cur = conn.cursor()
    
    # Insert loop using Snowflake's PARSE_JSON
    inserted_count = 0
    for item in data_list:
        query = f"INSERT INTO {table_name} (src_json) SELECT PARSE_JSON(%s)"
        cur.execute(query, (json.dumps(item),))
        inserted_count += 1
        
    print(f"Successfully loaded {inserted_count} rows into {table_name}.")
    conn.close()

if __name__ == "__main__":
    print("Starting pipeline ingestion step...")
    
    # Grab API tokens from environment
    cg_key = os.getenv('CG_API_KEY')
    news_key = os.getenv('NEWS_API_KEY')
    
    # Extract
    print("Extracting market and news data via REST APIs...")
    prices_data = fetch_crypto_prices(cg_key)
    news_data = fetch_crypto_news(news_key)
    
    # Load (Ensure you create RAW_NEWS table in Snowflake worksheet first!)
    print("Loading data into Snowflake Variant layers...")
    load_to_snowflake(prices_data, "RAW_MARKET_DATA")
    load_to_snowflake(news_data, "RAW_NEWS_DATA")
    
    print("Phase 2 Ingestion Complete.")