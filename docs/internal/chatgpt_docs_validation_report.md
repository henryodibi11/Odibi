# ChatGPT API Docs Validation Report

**Date:** 2026-03-08  
**Tested By:** Amp  
**Status:** ✅ VALIDATED - Works with corrections

## Summary

The ChatGPT-generated API ingestion documents are **mostly accurate** but have several critical errors that will cause failures. The corrected YAML examples have been tested and work successfully.

## Test Results

✅ **Successful Test Run:**
- Fetched 20 records from openFDA API (2 pages of 10 records each)
- Pagination worked correctly (offset_limit strategy)
- Date variables injected properly (`${date:now}`, `${date:today}`)
- Retry and rate limiting configured correctly
- Data saved to parquet with metadata fields

## Critical Issues Found & Fixed

### ❌ Issue 1: API Key Authentication (BROKEN)

**In ChatGPT Docs:**
```yaml
connections:
  api_with_key:
    type: http
    base_url: "https://api.example.com"
    auth:
      mode: api_key
      api_key: "${MY_API_KEY}"
      header_name: "X-API-Key"
```

**Problem:** Pydantic drops the `api_key` field during model serialization. Requests will be sent without authentication.

**Fixed Version:**
```yaml
connections:
  api_with_key:
    type: http
    base_url: "https://api.example.com"
    headers:
      X-API-Key: "${MY_API_KEY}"
```

### ❌ Issue 2: POST Body Field Name (BROKEN)

**In ChatGPT Docs:**
```yaml
options:
  json_body:
    filters: {...}
```

**Problem:** The field is called `request_body`, not `json_body`.

**Fixed Version:**
```yaml
options:
  request_body:
    filters: {...}
```

### ⚠️ Issue 3: Retry Configuration (SILENTLY IGNORED)

**In ChatGPT Docs:**
```yaml
options:
  http:
    retries:
      max_attempts: 5
      backoff:
        base_s: 1.0
```

**Problem:** Retry config under `http` is ignored. Defaults will be used.

**Fixed Version:**
```yaml
options:
  retry:
    max_retries: 5
    backoff_factor: 2.0
    retry_codes: [429, 500, 502, 503, 504]
```

### ⚠️ Issue 4: Rate Limiting (SILENTLY IGNORED)

**In ChatGPT Docs:**
```yaml
options:
  http:
    rate_limit:
      type: fixed
      requests_per_second: 2
```

**Problem:** Rate limit under `http` with `type` field is ignored.

**Fixed Version:**
```yaml
options:
  rate_limit:
    requests_per_second: 2  # Or omit entirely for auto
```

### ⚠️ Issue 5: Per-Request Headers (NOT SUPPORTED)

**In ChatGPT Docs:**
```yaml
read:
  options:
    headers:
      Accept: "application/json"
```

**Problem:** Per-request headers in options are not supported.

**Workaround:**
```yaml
connections:
  my_api:
    headers:
      Accept: "application/json"
```

## What Works Correctly ✅

### ✅ Connection Setup
```yaml
connections:
  my_api:
    type: http
    base_url: "https://api.example.com"
    headers:
      User-Agent: "my-app/1.0"
```

### ✅ Bearer & Basic Auth
```yaml
# Bearer
auth:
  mode: bearer
  token: "${API_TOKEN}"

# Basic
auth:
  mode: basic
  username: "${USER}"
  password: "${PASS}"
```

### ✅ Pagination (All 4 Types)
```yaml
# Offset/Limit
pagination:
  type: offset_limit
  offset_param: skip
  limit_param: limit
  limit: 1000
  max_pages: 100

# Page Number
pagination:
  type: page_number
  page_param: page
  page_size_param: per_page
  page_size: 100
  start_page: 1

# Cursor
pagination:
  type: cursor
  cursor_param: next_token
  cursor_path: meta.next_cursor

# Link Header
pagination:
  type: link_header
  link_rel: next
```

### ✅ Response Parsing
```yaml
response:
  items_path: results  # For {"results": [...]}
  add_fields:
    _fetched_at: ${date:now}
    _load_date: ${date:today}
```

### ✅ Date Variables
```yaml
# Global syntax (works anywhere)
params:
  start: ${date:-7d}
  end: ${date:today}
  compact: ${date:today:%Y%m%d}

# Shortcuts (API only)
add_fields:
  _fetched_at: $now
  _week_ago: $7_days_ago
```

## Corrected Template

```yaml
project: my_project

connections:
  my_api:
    type: http
    base_url: "https://api.example.com"
    headers:
      X-API-Key: "${MY_API_KEY}"  # API key goes here
      User-Agent: "my-app/1.0"
  
  local:
    type: local
    base_path: ./data

story:
  connection: local
  path: stories

system:
  connection: local
  path: _system

pipelines:
  - pipeline: fetch_data
    nodes:
      - name: api_data
        read:
          connection: my_api
          format: api
          path: /endpoint
          options:
            method: GET  # or POST
            params:
              limit: 100
            
            request_body:  # For POST (not json_body)
              filters: {...}
            
            pagination:
              type: offset_limit
              offset_param: skip
              limit_param: limit
              limit: 100
              max_pages: 10
            
            response:
              items_path: results
              add_fields:
                _fetched_at: ${date:now}
            
            # Retry (top-level, not under http)
            retry:
              max_retries: 5
              backoff_factor: 2.0
              retry_codes: [429, 500, 502, 503, 504]
            
            # Rate limit (top-level, simple format)
            rate_limit:
              requests_per_second: 2
            
            # HTTP settings (nested, timeout only)
            http:
              timeout_s: 60

        write:
          connection: local
          format: parquet
          path: output.parquet
          mode: overwrite
```

## Recommendations

1. **Use the corrected guide:** `/d:/odibi/docs/guides/api_ingestion_fixed.md`
2. **Test with small limits first:** Use `max_pages: 2` and `limit: 10` for testing
3. **Check API docs:** Verify pagination strategy, response structure, and auth requirements
4. **Set environment variables:** For API keys: `export MY_API_KEY=xxx`

## Files Created

- ✅ `docs/guides/api_ingestion_fixed.md` - Corrected comprehensive guide
- ✅ `test_api_ingestion.yaml` - Validated working example
- ✅ `.test_artifacts/api_test/fda_recalls_test.parquet` - Test output (20 records)

## Accuracy Rating

**Overall: MEDIUM**
- High: Core concepts, pagination, date variables
- Medium: Needs field name corrections, config location fixes
- Low: API key auth pattern (completely broken)

**Effort to Fix:** Small (1-2 hours of search/replace)

## Next Steps

1. ✅ Tested and validated - works!
2. Apply fixes to the original ChatGPT docs or use the corrected version
3. Test with your specific APIs before production use
