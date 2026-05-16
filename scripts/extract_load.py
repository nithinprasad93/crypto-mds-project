import os
import sys
import json
import time
import requests
import snowflake.connector

MAX_RETRIES = 3


def fetch_with_retry(url, params, label):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                print(f"[{label}] Fetched successfully on attempt {attempt}.")
                return response
            elif response.status_code == 429:
                wait = 2 ** attempt
                print(f"[{label}] Rate limited (429). Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"[{label}] HTTP {response.status_code}: {response.text}")
                sys.exit(1)
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"[{label}] Request error on attempt {attempt}: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    print(f"[{label}] All {MAX_RETRIES} attempts failed. Exiting.")
    sys.exit(1)


def fetch_crypto_prices(api_key):
    print(f"[CoinGecko] Fetching top 10 crypto prices...")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "aud",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false",
        "x_cg_demo_api_key": api_key
    }
    return fetch_with_retry(url, params, "CoinGecko").json()


def fetch_crypto_news(api_key):
    print(f"[NewsAPI] Fetching recent crypto news headlines...")
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "cryptocurrency OR bitcoin OR ethereum",
        "sortBy": "publishedAt",
        "pageSize": 20,
        "language": "en",
        "apiKey": api_key
    }
    return fetch_with_retry(url, params, "NewsAPI").json().get("articles", [])


def load_to_snowflake(data_list, table_name):
    print(f"[Snowflake] Connecting to load {len(data_list)} rows into {table_name}...")
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOW_USER'),
            password=os.getenv('SNOW_PASS'),
            account=os.getenv('SNOW_ACCOUNT'),
            role='ACCOUNTADMIN',
            warehouse='COMPUTE_WH',
            database='CRYPTO_DB',
            schema='RAW'
        )
    except Exception as e:
        print(f"[Snowflake] Connection failed: {e}")
        sys.exit(1)

    cur = conn.cursor()
    inserted_count = 0
    for item in data_list:
        cur.execute(
            f"INSERT INTO {table_name} (src_json) SELECT PARSE_JSON(%s)",
            (json.dumps(item),)
        )
        inserted_count += 1

    print(f"[Snowflake] Successfully loaded {inserted_count} rows into {table_name}.")
    conn.close()


if __name__ == "__main__":
    print("=== Starting pipeline ingestion step ===")

    cg_key = os.getenv('CG_API_KEY')
    news_key = os.getenv('NEWS_API_KEY')

    prices_data = fetch_crypto_prices(cg_key)
    news_data = fetch_crypto_news(news_key)

    load_to_snowflake(prices_data, "RAW_MARKET_DATA")
    load_to_snowflake(news_data, "RAW_NEWS_DATA")

    print("=== Ingestion complete ===")
