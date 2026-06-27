# Fetching Data from APIs (Corrected Guide)

This guide covers how to pull data from REST APIs into your Odibi pipelines. All examples have been validated against the actual Odibi codebase.

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
              max_pages: 10
            
            response:
              items_path: results
              add_fields:
                _fetched_at: ${date:now}
                _load_date: ${date:today}
            
            # Retry configuration (top-level, not under http)
            retry:
              max_retries: 5
              backoff_factor: 2.0
              retry_codes: [429, 500, 502, 503, 504]
            
            # Rate limiting (top-level, not under http)
            rate_limit:
              requests_per_second: 2
            
            # HTTP settings
            http:
              timeout_s: 60

        write:
          connection: local_storage
          format: parquet
          path: bronze/fda_recalls.parquet
          mode: overwrite
```

## Authentication

Different APIs use different authentication methods:

### No Authentication (Public APIs)
```yaml
connections:
  public_api:
    type: http
    base_url: "https://api.fda.gov"
```

### API Key in Header (CORRECT METHOD)
```yaml
connections:
  api_with_key:
    type: http
    base_url: "https://api.example.com"
    headers:
      X-API-Key: "${MY_API_KEY}"  # Put API key in headers
      User-Agent: "odibi-pipeline/1.0"
```

### Bearer Token
```yaml
connections:
  api_with_token:
    type: http
    base_url: "https://api.example.com"
    auth:
      mode: bearer
      token: "${API_TOKEN}"
```

### Basic Auth (Username/Password)
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

### 1. Offset/Limit (Most Common)
```yaml
options:
  pagination:
    type: offset_limit
    offset_param: skip
    limit_param: limit
    limit: 1000
    max_pages: 100
    stop_on_empty: true
```

### 2. Page Number
```yaml
options:
  pagination:
    type: page_number
    page_param: page
    page_size_param: per_page
    page_size: 100
    start_page: 1
    max_pages: 50
```

### 3. Cursor-Based
```yaml
options:
  pagination:
    type: cursor
    cursor_param: next_token
    cursor_path: meta.next_cursor
    max_pages: 100
```

### 4. Link Header (GitHub Style)
```yaml
options:
  pagination:
    type: link_header
    link_rel: next
    max_pages: 50
```

## Response Parsing

### Data in a Key
```yaml
options:
  response:
    items_path: results  # For {"results": [...]}
```

### Nested Data
```yaml
options:
  response:
    items_path: data.items  # For {"data": {"items": [...]}}
```

### Adding Metadata Fields
```yaml
options:
  response:
    items_path: results
    add_fields:
      _fetched_at: ${date:now}
      _load_date: ${date:today}
      _source: "fda_api"
```

## Date Variables

### Global Syntax (Works Anywhere)
```yaml
params:
  start_date: ${date:-7d}              # 7 days ago
  end_date: ${date:today}              # Today
  compact: ${date:today:%Y%m%d}        # Custom format: 20240108
```

### Shortcuts (API params/add_fields only)
```yaml
response:
  add_fields:
    _fetched_at: $now
    _load_date: $today
    _week_ago: $7_days_ago
```

## HTTP Settings (CORRECTED)

```yaml
options:
  # Retry configuration (top-level)
  retry:
    max_retries: 5
    backoff_factor: 2.0
    retry_codes: [429, 500, 502, 503, 504]
  
  # Rate limiting (top-level)
  rate_limit:
    requests_per_second: 2  # Fixed rate; omit for auto
  
  # HTTP settings (nested)
  http:
    timeout_s: 60
```

## POST APIs

Use `request_body` (not `json_body`):

```yaml
read:
  connection: my_api
  format: api
  path: /v1/search
  options:
    method: POST
    request_body:  # CORRECT field name
      filters:
        status: ["active"]
      columns:
        - id
        - name
    pagination:
      type: offset_limit
      offset_param: start
      limit_param: rows
      limit: 1000
    response:
      items_path: result
```

## Complete Working Example

```yaml
project: github_data

connections:
  github:
    type: http
    base_url: "https://api.github.com"
    headers:
      Accept: "application/vnd.github+json"
      User-Agent: "odibi-pipeline/1.0"
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
              items_path: ""  # GitHub returns array at root
              add_fields:
                _fetched_at: ${date:now}
            
            retry:
              max_retries: 3
              backoff_factor: 1.5
            
            rate_limit:
              requests_per_second: 1
            
            http:
              timeout_s: 30

        write:
          connection: bronze
          format: parquet
          path: github_issues.parquet
          mode: overwrite
```

## Common Mistakes to Avoid

❌ **Wrong: API key auth with mode**
```yaml
auth:
  mode: api_key
  api_key: "${MY_API_KEY}"  # This won't work!
```

✅ **Correct: API key in headers**
```yaml
headers:
  X-API-Key: "${MY_API_KEY}"
```

---

❌ **Wrong: json_body for POST**
```yaml
options:
  json_body: {...}  # Wrong field name
```

✅ **Correct: request_body**
```yaml
options:
  request_body: {...}
```

---

❌ **Wrong: retry under http**
```yaml
options:
  http:
    retries:
      max_attempts: 5  # Ignored!
```

✅ **Correct: retry at top level**
```yaml
options:
  retry:
    max_retries: 5
```

---

❌ **Wrong: rate_limit under http with type**
```yaml
options:
  http:
    rate_limit:
      type: fixed
      requests_per_second: 2
```

✅ **Correct: rate_limit at top level**
```yaml
options:
  rate_limit:
    requests_per_second: 2  # Or omit entirely for auto
```

## Quick Reference

```yaml
read:
  connection: my_api
  format: api
  path: /endpoint
  options:
    method: GET  # or POST
    params:
      key: value
    
    request_body:  # For POST/PUT/PATCH
      data: {...}
    
    pagination:
      type: offset_limit  # or page_number, cursor, link_header, none
      # ... type-specific params
    
    response:
      items_path: results
      add_fields:
        _fetched_at: ${date:now}
    
    retry:  # Top-level
      max_retries: 5
      backoff_factor: 2.0
      retry_codes: [429, 500, 502, 503, 504]
    
    rate_limit:  # Top-level
      requests_per_second: 2
    
    http:  # Nested
      timeout_s: 60
```
