---
title: Crypto Analytics Control Room
---

# 🪙 Crypto Analytics Control Room

Real-time spot pricing and news sentiment powered by our dbt pipeline.

```sql btc_price
select price_aud from crypto_data.stg_crypto_prices where ticker = 'btc'
```

```sql eth_price
select price_aud from crypto_data.stg_crypto_prices where ticker = 'eth'
```

```sql bnb_price
select price_aud from crypto_data.stg_crypto_prices where ticker = 'bnb'
```

```sql xrp_price
select price_aud from crypto_data.stg_crypto_prices where ticker = 'xrp'
```

```sql all_prices
select
    upper(ticker) as ticker,
    coin_id,
    price_aud,
    market_cap,
    observation_timestamp
from crypto_data.stg_crypto_prices
order by market_cap desc
```

```sql market_cap_chart
select
    upper(ticker) as ticker,
    round(market_cap / 1000000000, 2) as market_cap_bn
from crypto_data.stg_crypto_prices
order by market_cap desc
```

```sql bullish_count
select count(*) as count from crypto_data.fct_crypto_vibe where sentiment_score > 0
```

```sql bearish_count
select count(*) as count from crypto_data.fct_crypto_vibe where sentiment_score < 0
```

```sql neutral_count
select count(*) as count from crypto_data.fct_crypto_vibe where sentiment_score = 0
```

```sql news_feed
select
    news_date,
    headline,
    market_sentiment,
    sentiment_score
from crypto_data.fct_crypto_vibe
order by news_date desc
```

## Market Snapshot

<Grid cols=4>
  <BigValue data={btc_price} value=price_aud title="Bitcoin" subtitle="BTC · AUD" fmt="$#,##0"/>
  <BigValue data={eth_price} value=price_aud title="Ethereum" subtitle="ETH · AUD" fmt="$#,##0"/>
  <BigValue data={bnb_price} value=price_aud title="BNB" subtitle="BNB · AUD" fmt="$#,##0"/>
  <BigValue data={xrp_price} value=price_aud title="XRP" subtitle="XRP · AUD" fmt="$#,##0.00"/>
</Grid>

## Market Capitalisation

<BarChart
    data={market_cap_chart}
    x=ticker
    y=market_cap_bn
    sort=true
    yAxisTitle="Market Cap (AUD Billions)"
    title="Market Cap by Coin"
/>

## All Coins

<DataTable data={all_prices} sort=market_cap sortOrder=desc>
  <Column id=ticker title="Ticker"/>
  <Column id=coin_id title="Coin"/>
  <Column id=price_aud title="Price (AUD)" fmt="$#,##0.00" align=right/>
  <Column id=market_cap title="Market Cap (AUD)" fmt="#,##0" align=right/>
  <Column id=observation_timestamp title="Last Updated"/>
</DataTable>

---

## News Sentiment

<Grid cols=3>
  <BigValue data={bullish_count} value=count title="🚀 Bullish Headlines"/>
  <BigValue data={bearish_count} value=count title="📉 Bearish Headlines"/>
  <BigValue data={neutral_count} value=count title="😐 Neutral Headlines"/>
</Grid>

### Latest Headlines

<DataTable data={news_feed} rows=20>
  <Column id=headline title="Headline"/>
  <Column id=market_sentiment title="Sentiment" align=center/>
  <Column id=news_date title="Published"/>
</DataTable>
