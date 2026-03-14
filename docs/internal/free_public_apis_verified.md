# Verified Free & Public APIs for Bronze Layer

**Last Verified:** 2026-03-08  
**All APIs tested and working**

## 100% Free - No Auth Required ✅

### 1. openFDA (US Government)
- **URL:** https://api.fda.gov
- **Rate Limit:** 240 requests/minute, 120,000/day
- **Data:** Drug recalls, food recalls, device recalls, adverse events
- **Auth:** None required
- **Status:** ✅ VERIFIED WORKING

**Example Endpoints:**
- `/food/enforcement.json` - Food recalls
- `/drug/enforcement.json` - Drug recalls
- `/device/enforcement.json` - Medical device recalls

**Odibi Config:**
```yaml
connections:
  openfda:
    type: http
    base_url: "https://api.fda.gov"
```

---

### 2. Frankfurter (Currency Rates)
- **URL:** https://api.frankfurter.app
- **Rate Limit:** No official limit (be respectful)
- **Data:** ECB currency exchange rates, historical data
- **Auth:** None required
- **Status:** ✅ VERIFIED WORKING

**Example Endpoints:**
- `/latest?from=USD` - Latest rates
- `/2024-01-01..2024-12-31?from=USD` - Historical range

**Odibi Config:**
```yaml
connections:
  frankfurter:
    type: http
    base_url: "https://api.frankfurter.app"
```

---

### 3. REST Countries
- **URL:** https://restcountries.com/v3.1
- **Rate Limit:** No official limit
- **Data:** Country info (population, currencies, languages, flags)
- **Auth:** None required
- **Status:** ✅ VERIFIED WORKING

**Example Endpoints:**
- `/all` - All countries
- `/region/europe` - Countries by region
- `/name/france` - Search by name

**Odibi Config:**
```yaml
connections:
  restcountries:
    type: http
    base_url: "https://restcountries.com/v3.1"
```

---

### 4. JSON Placeholder (Test Data)
- **URL:** https://jsonplaceholder.typicode.com
- **Rate Limit:** None
- **Data:** Fake posts, comments, users (for testing)
- **Auth:** None required
- **Status:** ✅ VERIFIED WORKING

**Example Endpoints:**
- `/posts` - 100 fake posts
- `/users` - 10 fake users
- `/todos` - 200 todos

---

### 5. Cat Facts API
- **URL:** https://catfact.ninja
- **Rate Limit:** None specified
- **Data:** Random cat facts
- **Auth:** None required
- **Status:** ✅ VERIFIED WORKING

---

### 6. Official Joke API
- **URL:** https://official-joke-api.appspot.com
- **Rate Limit:** None specified
- **Data:** Random jokes
- **Auth:** None required
- **Status:** ✅ VERIFIED WORKING

---

## Free Tier - API Key Required (Free Signup) 🔑

### 7. CoinGecko (Crypto Data)
- **URL:** https://api.coingecko.com/api/v3
- **Rate Limit:** 10-30 calls/min (free tier)
- **Data:** Crypto prices, market cap, volume
- **Auth:** API key (optional for basic, required for higher limits)
- **Signup:** https://www.coingecko.com/en/api
- **Status:** ✅ VERIFIED WORKING (without key, limited)

**Free Tier:**
- 10,000 calls/month
- 10-30 calls/minute
- Historical data access

**Odibi Config:**
```yaml
connections:
  coingecko:
    type: http
    base_url: "https://api.coingecko.com/api/v3"
    headers:
      x-cg-demo-api-key: ${COINGECKO_API_KEY}  # Optional
```

---

### 8. AlphaVantage (Stock Data)
- **URL:** https://www.alphavantage.co
- **Rate Limit:** 25 requests/day (free tier)
- **Data:** Stock prices, forex, crypto, economic indicators
- **Auth:** API key required
- **Signup:** https://www.alphavantage.co/support/#api-key
- **Status:** ✅ VERIFIED WORKING

**Free Tier:**
- 25 API calls/day
- Real-time & historical data
- Technical indicators

**Odibi Config:**
```yaml
connections:
  alphavantage:
    type: http
    base_url: "https://www.alphavantage.co"

# Pass apikey as param:
options:
  params:
    apikey: ${ALPHAVANTAGE_API_KEY}
```

---

## More Free APIs to Consider

### Weather
- **OpenWeatherMap:** Free tier (60 calls/min)
- **WeatherAPI:** Free tier (1M calls/month)

### News
- **NewsAPI:** Free tier (100 requests/day)
- **The Guardian:** Free (requires key)

### Social/Entertainment  
- **Reddit:** Free (OAuth required)
- **Spotify:** Free (OAuth required)

### Reference Data
- **DataUSA:** Free government data
- **World Bank:** Free economic indicators

---

## API Comparison Matrix

| API | Auth Required | Rate Limit | Best For | Update Frequency |
|-----|---------------|------------|----------|------------------|
| openFDA | ❌ No | 240/min | Health/safety data | Daily |
| Frankfurter | ❌ No | Unlimited* | Currency rates | Daily (ECB) |
| REST Countries | ❌ No | Unlimited* | Reference data | Weekly |
| CoinGecko | ⚠️ Optional | 10-30/min | Crypto markets | Hourly |
| AlphaVantage | ✅ Yes | 25/day | Stock data | Intraday |

*No official limit but be respectful

---

## Recommended Bronze Schedule

### Daily (2am)
- openFDA (all recall types)
- Frankfurter (currency rates)
- News APIs

### Hourly (if you have API keys)
- CoinGecko (crypto prices)
- Stock prices (within 25/day limit)

### Weekly (Sunday 3am)
- REST Countries (reference data)
- Full snapshots

---

## Rate Limiting Best Practices

**For No-Auth APIs:**
```yaml
rate_limit:
  requests_per_second: 1  # Conservative, respectful
```

**For APIs with Limits:**
```yaml
# CoinGecko (10/min free tier)
rate_limit:
  requests_per_second: 0.16  # ~10/min

# AlphaVantage (25/day)
rate_limit:
  requests_per_second: 0.016  # ~1/min (safe margin)
```

---

## NOT Free (Mentioned in ChatGPT Docs)

❌ **Polygon.io** - Requires paid subscription ($199/month minimum)  
❌ **TwelveData** - Free tier very limited (800 calls/day, 8 calls/min)  
❌ **GitHub** - Free but requires authentication token  
❌ **ExchangeRate API** - Free tier exists but limited

---

## Working Example - 100% Free

```yaml
project: bronze_free_only
engine: pandas

connections:
  openfda:
    type: http
    base_url: "https://api.fda.gov"
  
  frankfurter:
    type: http
    base_url: "https://api.frankfurter.app"
  
  bronze:
    type: local
    base_path: ./data/bronze

story:
  connection: bronze
  path: stories

system:
  connection: bronze
  path: _system

pipelines:
  - pipeline: daily_free_data
    nodes:
      # FDA Food Recalls
      - name: fda_food
        read:
          connection: openfda
          format: api
          path: /food/enforcement.json
          options:
            params:
              limit: 1000
            pagination:
              type: offset_limit
              offset_param: skip
              limit_param: limit
              limit: 1000
              max_pages: 50
            response:
              items_path: results
              add_fields:
                _fetched_at: ${date:now}
                _load_date: ${date:today}
            rate_limit:
              requests_per_second: 2
        write:
          connection: bronze
          format: parquet
          path: fda_food.parquet
          mode: append

      # Currency Rates
      - name: currency
        read:
          connection: frankfurter
          format: api
          path: /latest
          options:
            params:
              from: USD
            pagination:
              type: none
            response:
              items_path: ""
              dict_to_list: true
              add_fields:
                _fetched_at: ${date:now}
                _load_date: ${date:today}
        write:
          connection: bronze
          format: parquet
          path: currency.parquet
          mode: append
```

**Run it:**
```bash
python -m odibi run bronze_free_only.yaml
```

No API keys, no signup, 100% free! 🎉
