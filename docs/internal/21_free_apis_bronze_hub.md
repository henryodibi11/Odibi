# 21 Free Public APIs for Your Bronze Data Hub

**Last Updated:** 2026-03-08  
**Status:** All verified working  
**Config File:** `configs/bronze_20_free_apis.yaml`

## Quick Stats

- **Total APIs:** 21
- **No Auth Required:** 16 (76%)
- **Free Tier (generous):** 5 (24%)
- **Total Storage/Year:** ~500MB-1GB
- **Est. Records/Month:** ~100K+

---

## 100% Free - No Authentication (16 APIs)

### 1. 🏛️ openFDA - US Government Data
**URL:** https://api.fda.gov  
**Data:** Drug/food/device recalls, adverse events  
**Rate Limit:** 240/min, 120K/day  
**Best For:** Health/safety data, compliance monitoring  
**Update Frequency:** Daily

**Example Endpoint:**
```
https://api.fda.gov/food/enforcement.json?limit=100
```

---

### 2. 🌍 REST Countries
**URL:** https://restcountries.com  
**Data:** 250+ countries (population, currencies, languages)  
**Rate Limit:** Unlimited  
**Best For:** Reference data, geography apps  
**Update Frequency:** Weekly (as needed)

**Example Endpoint:**
```
https://restcountries.com/v3.1/all
```

---

### 3. 🎮 PokéAPI
**URL:** https://pokeapi.co  
**Data:** 1000+ Pokémon (stats, abilities, types)  
**Rate Limit:** Unlimited (be respectful)  
**Best For:** Gaming data, learning APIs  
**Update Frequency:** Static (updates with new games)

**Example Endpoint:**
```
https://pokeapi.co/api/v2/pokemon?limit=100
```

---

### 4. 📝 JSON Placeholder
**URL:** https://jsonplaceholder.typicode.com  
**Data:** Fake posts, users, comments (testing)  
**Rate Limit:** Unlimited  
**Best For:** Testing, prototyping, learning  
**Update Frequency:** Static

**Example Endpoint:**
```
https://jsonplaceholder.typicode.com/posts
```

---

### 5. 🐱 Cat Facts API
**URL:** https://catfact.ninja  
**Data:** Random cat facts  
**Rate Limit:** Unlimited  
**Best For:** Fun data, testing pagination  
**Update Frequency:** Static

---

### 6. 🐕 Dog API
**URL:** https://dog.ceo/api  
**Data:** Dog breeds, random images  
**Rate Limit:** Unlimited  
**Best For:** Image APIs, fun projects  
**Update Frequency:** Static

---

### 7. 💡 Advice Slip
**URL:** https://api.adviceslip.com  
**Data:** Random advice  
**Rate Limit:** Unlimited  
**Best For:** Fun content, testing  
**Update Frequency:** Static

---

### 8. 🎤 Kanye Rest
**URL:** https://api.kanye.rest  
**Data:** Kanye West quotes  
**Rate Limit:** Unlimited  
**Best For:** Fun content  
**Update Frequency:** Static

---

### 9. 🎲 Bored API
**URL:** https://www.boredapi.com/api  
**Data:** Activity suggestions  
**Rate Limit:** Unlimited  
**Best For:** Fun apps, recommendation systems  
**Update Frequency:** Static

---

### 10. 🌐 IP API
**URL:** https://ipapi.co  
**Data:** IP geolocation (country, city, lat/lon)  
**Rate Limit:** 1000/day (no key), 30K/month (with key)  
**Best For:** User geolocation, analytics  
**Update Frequency:** Real-time

**Example Endpoint:**
```
https://ipapi.co/json/
```

---

### 11. 🎂 Agify - Age Prediction
**URL:** https://api.agify.io  
**Data:** Predict age from first name  
**Rate Limit:** 1000/day  
**Best For:** Demographics, data enrichment  
**Update Frequency:** Static

**Example Endpoint:**
```
https://api.agify.io/?name=michael
```

---

### 12. ⚧ Genderize - Gender Prediction
**URL:** https://api.genderize.io  
**Data:** Predict gender from first name  
**Rate Limit:** 1000/day  
**Best For:** Demographics, data enrichment  
**Update Frequency:** Static

---

### 13. 🌏 Nationalize - Nationality Prediction
**URL:** https://api.nationalize.io  
**Data:** Predict nationality from name  
**Rate Limit:** 1000/day  
**Best For:** Demographics, data analysis  
**Update Frequency:** Static

---

### 14. 👥 Random User Generator
**URL:** https://randomuser.me  
**Data:** Fake user profiles (name, email, photo)  
**Rate Limit:** Unlimited  
**Best For:** Testing, seed data  
**Update Frequency:** Dynamic

**Example Endpoint:**
```
https://randomuser.me/api/?results=100
```

---

### 15. 🎨 DiceBear Avatars
**URL:** https://api.dicebear.com  
**Data:** Generate avatar images  
**Rate Limit:** Unlimited (open source)  
**Best For:** User avatars, profile pictures  
**Update Frequency:** Static

---

### 16. 🔢 Numbers API
**URL:** http://numbersapi.com  
**Data:** Math/date/trivia facts about numbers  
**Rate Limit:** Unlimited  
**Best For:** Fun facts, education  
**Update Frequency:** Static

**Example Endpoint:**
```
http://numbersapi.com/42
```

---

## Free with Generous Tiers (5 APIs)

### 17. 💰 CoinGecko - Crypto Data
**URL:** https://api.coingecko.com/api/v3  
**Data:** 10,000+ coins (prices, market cap, volume)  
**Rate Limit:** 10-30/min (no auth), higher with key  
**Best For:** Crypto tracking, financial analysis  
**Update Frequency:** Real-time

**Example Endpoint:**
```
https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=100
```

---

### 18. 🚀 NASA - Space Data
**URL:** https://api.nasa.gov  
**Data:** APOD, Mars rover photos, asteroids  
**Rate Limit:** 30/hour (DEMO_KEY), 1000/hour (free key)  
**Best For:** Space data, education  
**Update Frequency:** Daily

**Example Endpoint:**
```
https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY
```

---

### 19. ☁️ Open-Meteo - Weather Data
**URL:** https://api.open-meteo.com  
**Data:** Weather forecasts, historical weather  
**Rate Limit:** 10,000/day  
**Best For:** Weather apps, climate analysis  
**Update Frequency:** Hourly

**Example Endpoint:**
```
https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&current_weather=true
```

---

### 20. 💱 Frankfurter - Currency Rates
**URL:** https://api.frankfurter.app  
**Data:** ECB currency exchange rates  
**Rate Limit:** Unlimited (be respectful)  
**Best For:** Currency conversion, finance  
**Update Frequency:** Daily

**Example Endpoint:**
```
https://api.frankfurter.app/latest?from=USD
```

---

### 21. 💎 Gemini - Crypto Exchange Data
**URL:** https://api.gemini.com  
**Data:** Real-time crypto ticker data  
**Rate Limit:** 120/min (public endpoints)  
**Best For:** Crypto market data  
**Update Frequency:** Real-time

**Example Endpoint:**
```
https://api.gemini.com/v2/ticker/btcusd
```

---

## Data Collection Strategy by Frequency

### ⏰ Hourly (Real-time market data)
- CoinGecko crypto prices
- Gemini ticker data
- Open-Meteo weather

### 📅 Daily (Regular updates)
- openFDA recalls
- NASA APOD
- IP geolocation trends
- Frankfurter currency rates

### 📆 Weekly (Reference data)
- REST Countries
- PokéAPI (if new games released)

### 🎲 On-Demand (Testing/fun data)
- JSON Placeholder
- Random User Generator
- Cat Facts, Dog API
- All "fun" APIs

---

## Estimated Data Volume

| API | Records/Day | Records/Month | Storage/Year |
|-----|-------------|---------------|--------------|
| FDA Food | 1,000 | 30,000 | ~40MB |
| FDA Drug | 800 | 25,000 | ~35MB |
| Countries | 250/week | 1,000 | ~2MB |
| Pokémon | 500 | 500 | ~5MB |
| Crypto (hourly) | 2,400 | 72,000 | ~120MB |
| Weather (hourly) | 720 | 21,600 | ~30MB |
| Random Users | 100 | 3,000 | ~10MB |
| NASA APOD | 1 | 30 | ~1MB |
| **TOTAL** | **~6K/day** | **~150K/month** | **~250MB/year** |

---

## Quick Start by Category

### 🎯 Best for Learning APIs
1. JSON Placeholder (simplest)
2. Cat Facts (pagination practice)
3. PokéAPI (nested data)
4. Random User Generator (complex structure)

### 💼 Best for Production Bronze Layer
1. openFDA (valuable health data)
2. REST Countries (reference data)
3. CoinGecko (financial data)
4. Open-Meteo (weather data)

### 🎮 Best for Fun Projects
1. PokéAPI (gaming)
2. NASA (space exploration)
3. Dog/Cat APIs (images)
4. Bored API (recommendations)

### 📊 Best for Demographics/Analytics
1. IP API (geolocation)
2. Agify/Genderize/Nationalize (name analysis)
3. Random User Generator (test data)

---

## Test Command

**Test all 21 APIs:**
```bash
# Run specific pipelines
python -m odibi run configs/bronze_20_free_apis.yaml --pipeline bronze_fda_food
python -m odibi run configs/bronze_20_free_apis.yaml --pipeline bronze_pokemon
python -m odibi run configs/bronze_20_free_apis.yaml --pipeline bronze_crypto_coingecko
```

---

## Scheduling Recommendations

**Daily 2am - Core Data:**
```bash
bronze_fda_food
bronze_countries (weekly only)
bronze_nasa_apod
```

**Hourly - Market Data:**
```bash
bronze_crypto_coingecko
bronze_crypto_gemini
bronze_weather
```

**Weekly - Reference Data:**
```bash
bronze_countries
bronze_pokemon
bronze_dog_images
```

---

## Next Steps

1. ✅ Run test: `python -m odibi run configs/bronze_20_free_apis.yaml --pipeline bronze_fda_food`
2. Set up Windows Task Scheduler for daily/hourly runs
3. Build silver layer transformations
4. Create dashboards/visualizations

**You now have 21 diverse data sources for FREE!** 🎉
