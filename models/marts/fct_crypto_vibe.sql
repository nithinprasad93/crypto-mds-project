with news as (
    select 
        src_json:title::string as headline,
        ingested_at as news_date
    from {{ source('crypto_api', 'RAW_NEWS_DATA') }}
),

scored_news as (
    select
        *,
        -- Rule-based scoring: +1 for positive words, -1 for negative
        (case when lower(headline) like '%bull%' or lower(headline) like '%surge%' or lower(headline) like '%gain%' then 1 else 0 end) +
        (case when lower(headline) like '%bear%' or lower(headline) like '%crash%' or lower(headline) like '%drop%' then -1 else 0 end) as sentiment_score
    from news
)

select 
    news_date,
    headline,
    sentiment_score,
    case 
        when sentiment_score > 0 then '🚀 Bullish'
        when sentiment_score < 0 then '📉 Bearish'
        else '😐 Neutral'
    end as market_sentiment
from scored_news