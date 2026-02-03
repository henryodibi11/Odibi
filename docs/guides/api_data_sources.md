# Fetching Data from APIs

This guide covers how to pull data from REST APIs into your Odibi pipelines. Whether you're pulling from public APIs like openFDA or internal company APIs, this guide will get you started.

## What is an API?

An **API (Application Programming Interface)** is a way for programs to talk to each other. When you hear "REST API" in data engineering, it usually means:

- A **URL** you can call to get data (like `https://api.fda.gov/food/enforcement.json`)
- The data comes back as **JSON** (a text format that looks like nested key-value pairs)
- You might need an **API key** or token to authenticate

### Example: What an API Response Looks Like

When you call `https://api.fda.gov/food/enforcement.json?limit=2`, you get:

```json
{
  "meta": {
    "results": {
      "skip": 0,
      "limit": 2,
      "total": 25847
    }
  },
  "results": [
    {
      "recall_number": "F-0276-2017",
      "status": "Terminated",
      "city": "Davie",
      "state": "FL",
      "classification": "Class II"
    },
    {
      "recall_number": "F-0865-2017",
      "status": "Terminated", 
      "city": "Millbrae",
      "state": "CA",
      "classification": "Class II"
    }
  ]
}
```

Notice:
- The actual data is nested inside `"results"`
- There's metadata telling you there are 25,847 total records
- You only got 2 because of `limit=2`

## How Odibi Fetches API Data

Odibi uses `format: api` to fetch data from REST APIs. It handles:

1. **Pagination** - Automatically fetches all pages of data
2. **Response parsing** - Extracts the data from nested JSON
3. **Retries** - Handles temporary failures gracefully
4. **Rate limiting** - Respects API limits

## Quick Start Example

Here's a complete working example that fetches FDA food recall data:

```yaml
project: my_api_project

connections:
  # HTTP connection - defines the base URL and auth
  my_api:
    type: http
    base_url: "https://api.fda.gov"
    headers:
      User-Agent: "my-app/1.0"

  local_storage:
    type: local
    base_path: ./data

story:
  connection: local_storage
  path: stories

system:
  connection: local_storage
  path: _system

pipelines:
  - pipeline: fetch_fda_data
    nodes:
      - name: fda_recalls
        read:
          connection: my_api
          format: api                    # <-- This tells Odibi to use the API fetcher
          path: /food/enforcement.json   # <-- The API endpoint
          options:
            params:                      # Query parameters to send
              limit: 1000
            
            pagination:                  # How to get all pages
              type: offset_limit
              offset_param: skip
              limit_param: limit
              limit: 1000
              max_pages: 10
            
            response:                    # Where to find the data in JSON
              items_path: results

        write:
          connection: local_storage
          format: parquet
          path: bronze/fda_recalls.parquet
          mode: overwrite
```

Run it:
```bash
python -m odibi run my_pipeline.yaml
```

## Understanding API Configuration

### Connection Setup

First, define an HTTP connection with the API's base URL:

```yaml
connections:
  my_api:
    type: http
    base_url: "https://api.example.com"
    headers:
      User-Agent: "odibi-pipeline/1.0"
      Accept: "application/json"
```

### Authentication

Different APIs use different authentication methods:

#### No Authentication (Public APIs)
```yaml
connections:
  public_api:
    type: http
    base_url: "https://api.fda.gov"
```

#### API Key in Header
```yaml
connections:
  api_with_key:
    type: http
    base_url: "https://api.example.com"
    auth:
      mode: api_key
      api_key: "${MY_API_KEY}"      # Use environment variable
      header_name: "X-API-Key"       # Default is X-API-Key
```

#### Bearer Token
```yaml
connections:
  api_with_token:
    type: http
    base_url: "https://api.example.com"
    auth:
      mode: bearer
      token: "${API_TOKEN}"
```

#### Basic Auth (Username/Password)
```yaml
connections:
  api_with_basic:
    type: http
    base_url: "https://api.example.com"
    auth:
      mode: basic
      username: "${API_USER}"
      password: "${API_PASSWORD}"
```

## Pagination Strategies

Most APIs don't return all data at once—they **paginate** (return data in chunks). Odibi supports 4 pagination types:

### 1. Offset/Limit (Most Common)

Used by: openFDA, many REST APIs

The API uses `skip` (or `offset`) and `limit` parameters:
- Page 1: `?skip=0&limit=100`
- Page 2: `?skip=100&limit=100`
- Page 3: `?skip=200&limit=100`

```yaml
options:
  pagination:
    type: offset_limit
    offset_param: skip        # Parameter name for offset (default: "offset")
    limit_param: limit        # Parameter name for limit (default: "limit")
    limit: 1000               # Records per page
    max_pages: 100            # Safety limit
    stop_on_empty: true       # Stop when no more results
```

### 2. Page Number

Used by: Some older APIs

The API uses page numbers:
- Page 1: `?page=1&per_page=100`
- Page 2: `?page=2&per_page=100`

```yaml
options:
  pagination:
    type: page_number
    page_param: page          # Parameter name for page number
    page_size_param: per_page # Parameter name for page size
    page_size: 100            # Records per page
    start_page: 1             # First page number (usually 1)
    max_pages: 50
```

### 3. Cursor-Based

Used by: Twitter, Slack, modern APIs

The API returns a "cursor" or "next_token" in each response:

```yaml
options:
  pagination:
    type: cursor
    cursor_param: next_token       # Parameter name to send cursor
    cursor_path: meta.next_cursor  # Where to find cursor in response
    max_pages: 100
```

### 4. Link Header (GitHub Style)

Used by: GitHub, GitLab

The API returns a `Link` header with the next URL:

```yaml
options:
  pagination:
    type: link_header
    link_rel: next            # Usually "next"
    max_pages: 50
```

### No Pagination

For APIs that return all data in one response:

```yaml
options:
  pagination:
    type: none
```

## Response Parsing

APIs return data in different structures. Tell Odibi where to find the actual records:

### Data at Root Level
```json
[{"id": 1}, {"id": 2}]
```
```yaml
options:
  response:
    items_path: ""   # Empty = root is the array
```

### Data in a Key
```json
{"results": [{"id": 1}, {"id": 2}]}
```
```yaml
options:
  response:
    items_path: results
```

### Nested Data
```json
{"data": {"items": [{"id": 1}, {"id": 2}]}}
```
```yaml
options:
  response:
    items_path: data.items
```

### OData APIs
```json
{"value": [{"id": 1}, {"id": 2}], "@odata.nextLink": "..."}
```
```yaml
options:
  response:
    items_path: value
```

### Adding Metadata Fields

You can add fields to every record using static values or **date variables**:

```yaml
options:
  response:
    items_path: results
    add_fields:
      _fetched_at: "$now"         # Adds current UTC timestamp
      _load_date: "$today"        # Adds today's date
      _source: "fda_api"          # Adds a constant value
```

## Date Variables {#date-variables}

When adding fields to API responses, use these special variables for automatic date injection:

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `$now` | Current UTC timestamp (ISO 8601) | `2024-01-15T10:30:00+00:00` |
| `$today` | Today's date | `2024-01-15` |
| `$yesterday` | Yesterday's date | `2024-01-14` |
| `$date` | Alias for `$today` | `2024-01-15` |
| `$7_days_ago` | 7 days before today | `2024-01-08` |
| `$30_days_ago` | 30 days before today | `2023-12-16` |
| `$90_days_ago` | 90 days before today | `2023-10-17` |
| `$start_of_week` | Monday of current week | `2024-01-15` |
| `$start_of_month` | First of current month | `2024-01-01` |
| `$start_of_year` | First of current year | `2024-01-01` |

### Example: Tracking Data Freshness

```yaml
options:
  response:
    items_path: results
    add_fields:
      _fetched_at: "$now"           # When this record was fetched
      _load_date: "$today"          # Date-based partitioning key
      _week_start: "$start_of_week" # For weekly aggregations
```

### Example: Audit Trail

```yaml
options:
  response:
    items_path: data
    add_fields:
      _source_system: "vendor_api"
      _ingestion_timestamp: "$now"
      _batch_date: "$today"
```

!!! tip "Why use date variables?"
    Adding `_fetched_at: "$now"` to every API record lets you:
    
    - Track when data was ingested (not just when it was created)
    - Debug stale data issues
    - Implement incremental processing based on fetch time
    - Create audit trails for compliance

## HTTP Settings

Configure timeouts, retries, and rate limiting:

```yaml
options:
  http:
    timeout_s: 60                 # Request timeout in seconds
    
    retries:
      max_attempts: 5             # Total attempts (including first)
      backoff:
        base_s: 1.0               # Initial wait between retries
        max_s: 60.0               # Maximum wait time
      retry_on_status:            # HTTP codes to retry
        - 429                     # Too Many Requests
        - 500                     # Server Error
        - 502                     # Bad Gateway
        - 503                     # Service Unavailable
        - 504                     # Gateway Timeout
    
    rate_limit:
      type: auto                  # Respects Retry-After headers
      # OR fixed rate:
      # type: fixed
      # requests_per_second: 2
```

## Query Parameters

Send parameters with your request:

```yaml
options:
  params:
    limit: 1000
    status: active
    sort: created_at
    # For search/filter syntax (varies by API):
    search: "report_date:[20240101+TO+20241231]"
```

## Complete Examples

### Example 1: openFDA Food Recalls

```yaml
project: fda_recalls

connections:
  openfda:
    type: http
    base_url: "https://api.fda.gov"
    headers:
      User-Agent: "odibi-pipeline/1.0"

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
  - pipeline: food_recalls
    nodes:
      - name: fda_food_recalls
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
              max_pages: 100
            response:
              items_path: results
              add_fields:
                _fetched_at: "$now"
        write:
          connection: bronze
          format: parquet
          path: fda_food_recalls.parquet
          mode: overwrite
```

### Example 2: GitHub API (Link Header Pagination)

```yaml
project: github_data

connections:
  github:
    type: http
    base_url: "https://api.github.com"
    headers:
      Accept: "application/vnd.github+json"
    auth:
      mode: bearer
      token: "${GITHUB_TOKEN}"

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
  - pipeline: repo_issues
    nodes:
      - name: issues
        read:
          connection: github
          format: api
          path: /repos/henryodibi11/Odibi/issues
          options:
            params:
              state: all
              per_page: 100
            pagination:
              type: link_header
              link_rel: next
              max_pages: 10
            response:
              items_path: ""    # GitHub returns array at root
        write:
          connection: bronze
          format: parquet
          path: github_issues.parquet
          mode: overwrite
```

### Example 3: OData API

```yaml
project: odata_example

connections:
  odata_api:
    type: http
    base_url: "https://services.odata.org/V4/Northwind/Northwind.svc"

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
  - pipeline: customers
    nodes:
      - name: customers
        read:
          connection: odata_api
          format: api
          path: /Customers
          options:
            params:
              $format: json
              $top: 100
            pagination:
              type: offset_limit
              offset_param: $skip
              limit_param: $top
              limit: 100
              max_pages: 10
            response:
              items_path: value
        write:
          connection: bronze
          format: parquet
          path: customers.parquet
          mode: overwrite
```

### Example 4: Cursor-Based API

```yaml
project: cursor_api

connections:
  my_api:
    type: http
    base_url: "https://api.example.com"
    auth:
      mode: bearer
      token: "${API_TOKEN}"

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
  - pipeline: fetch_data
    nodes:
      - name: records
        read:
          connection: my_api
          format: api
          path: /v1/records
          options:
            params:
              page_size: 500
            pagination:
              type: cursor
              cursor_param: cursor
              cursor_path: pagination.next_cursor
              max_pages: 50
            response:
              items_path: data
        write:
          connection: bronze
          format: parquet
          path: records.parquet
          mode: overwrite
```

## Step-by-Step: Figuring Out a New API

When you get a request to pull data from an API you've never used, here's the workflow:

### Step 1: Test the API in Your Browser or Terminal

Before writing any YAML, test the API works:

```bash
# Simple test - just hit the endpoint
curl "https://api.fda.gov/food/enforcement.json?limit=1"

# With an API key (if required)
curl -H "X-API-Key: your-key-here" "https://api.example.com/data"

# With a bearer token
curl -H "Authorization: Bearer your-token" "https://api.example.com/data"
```

Or just paste the URL in your browser for public APIs.

### Step 2: Look at the Response

Save the response and examine it:

```bash
curl "https://api.fda.gov/food/enforcement.json?limit=2" > response.json
```

Open `response.json` and find:

1. **Where is the data array?** Look for `[]` brackets
   - At root level: `[{...}, {...}]` → `items_path: ""`
   - In a key: `{"results": [{...}]}` → `items_path: results`
   - Nested: `{"data": {"items": [{...}]}}` → `items_path: data.items`

2. **Is there pagination info?** Look for:
   - `"total": 25000` → There's more data than you got
   - `"next_cursor": "abc123"` → Cursor pagination
   - `"skip": 0, "limit": 100` → Offset pagination

### Step 3: Find Pagination from the Docs

Read the API docs and look for:

| Docs say... | Pagination type | Example params |
|-------------|-----------------|----------------|
| "skip/limit", "offset/limit" | `offset_limit` | `?skip=0&limit=100` |
| "page/per_page", "page/size" | `page_number` | `?page=1&per_page=100` |
| "cursor", "next_token", "continuation" | `cursor` | `?cursor=abc123` |
| "Link header", "RFC 5988" | `link_header` | Check response headers |

### Step 4: Test Pagination Manually

Verify pagination works before automating:

```bash
# Page 1
curl "https://api.example.com/data?skip=0&limit=10"

# Page 2 - should return different data
curl "https://api.example.com/data?skip=10&limit=10"
```

### Step 5: Write Your Odibi YAML

Now you know:
- ✅ Base URL
- ✅ Endpoint path
- ✅ Auth method (if any)
- ✅ Pagination type and params
- ✅ Where data lives in response (`items_path`)

Write the YAML and test with a small `max_pages: 2` first.

### Step 6: Run and Verify

```bash
# Test run with limited pages
python -m odibi run my_pipeline.yaml

# Check the output
python -c "import pandas as pd; df = pd.read_parquet('output.parquet'); print(len(df), df.columns.tolist()[:5])"
```

---

## How to Find API Documentation

When working with a new API, you need to find:

1. **Base URL** - Where the API lives (e.g., `https://api.fda.gov`)
2. **Endpoints** - The paths for different data (e.g., `/food/enforcement.json`)
3. **Authentication** - How to prove who you are
4. **Pagination** - How to get all the data
5. **Response format** - Where the data is in the JSON

### Tips for Reading API Docs

| Look for... | Maps to... |
|-------------|------------|
| "Base URL" or "API Host" | `base_url` in connection |
| "Endpoints" or "Resources" | `path` in read config |
| "Authentication", "API Keys" | `auth` in connection |
| "Pagination", "skip/limit", "page/per_page" | `pagination` in options |
| "Response", "Example response" | `items_path` in response |

### Common API Documentation Sites

- **openFDA**: https://open.fda.gov/apis/
- **GitHub**: https://docs.github.com/en/rest
- **OData**: https://www.odata.org/documentation/

## Troubleshooting

### "HTTP Error 401: Unauthorized"
- Check your API key or token
- Verify the auth mode matches what the API expects
- Make sure environment variables are set

### "HTTP Error 404: Not Found"
- Check the endpoint path
- Some APIs need a trailing slash, some don't
- Verify the base URL is correct

### "HTTP Error 429: Too Many Requests"
- The API is rate limiting you
- Add rate limiting config:
  ```yaml
  http:
    rate_limit:
      type: fixed
      requests_per_second: 1
  ```

### No data returned
- Check `items_path` matches where data is in the response
- Test the API URL in a browser or with `curl`
- Look at the raw response:
  ```bash
  curl "https://api.fda.gov/food/enforcement.json?limit=1"
  ```

### "Connection timed out"
- Increase timeout:
  ```yaml
  http:
    timeout_s: 120
  ```

## Quick Reference

```yaml
read:
  connection: my_api           # HTTP connection name
  format: api                  # Required for API fetching
  path: /endpoint              # API endpoint path
  options:
    params:                    # Query parameters
      key: value
    
    pagination:
      type: offset_limit       # offset_limit | page_number | cursor | link_header | none
      # ... pagination-specific options
      max_pages: 100           # Safety limit
    
    response:
      items_path: results      # Dotted path to data array
      add_fields:              # Optional fields to add
        _fetched_at: "$now"
    
    http:
      timeout_s: 60
      retries:
        max_attempts: 5
      rate_limit:
        type: auto
```

## Next Steps

- Try the [openFDA example](../examples/canonical/README.md) to see it in action
- Learn about [incremental loading](../patterns/incremental_stateful.md) for weekly API pulls
- Set up [alerting](../features/alerting.md) to know when API pulls fail
