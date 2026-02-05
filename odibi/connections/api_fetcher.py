"""API Fetcher - Paginated REST API data fetching for Odibi.

This module provides a flexible API fetching system that supports:
- Multiple pagination strategies (offset/limit, cursor, link-header, page-number)
- Nested response parsing (e.g., results, data.items, value)
- Rate limiting and retries
- Incremental loading with date filtering

Example YAML usage:
```yaml
nodes:
  - name: fda_recalls
    read:
      connection: openfda
      format: api
      path: /food/enforcement.json
      options:
        pagination:
          type: offset_limit
          offset_param: skip
          limit_param: limit
          limit: 1000
          max_pages: 100
        response:
          items_path: results
```
"""

from __future__ import annotations

import json
import re
import time

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Protocol, Tuple, Type
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

import pandas as pd

from odibi.utils.logging_context import get_logging_context


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ApiRequest:
    """Represents an API request to be made."""

    method: str
    url: str
    params: Dict[str, Any] = field(default_factory=dict)
    json_body: Optional[Dict[str, Any]] = None


@dataclass
class ApiPage:
    """Represents a single page of API response."""

    items: List[Dict[str, Any]]
    raw: Dict[str, Any]
    headers: Dict[str, str]
    request: ApiRequest
    status_code: int = 200


@dataclass
class FetchResult:
    """Result of a safe fetch operation that preserves partial data on error.

    Attributes:
        df: DataFrame with all successfully fetched records (may be partial)
        error: Exception that occurred, or None if fetch completed successfully
        pages_fetched: Number of pages successfully fetched before error/completion
        complete: True if all pages were fetched, False if stopped due to error
    """

    df: pd.DataFrame
    error: Optional[Exception]
    pages_fetched: int
    complete: bool


# =============================================================================
# Response Extractor
# =============================================================================


def get_date_variables() -> Dict[str, str]:
    """Return date variable substitution map.

    Available variables:
    - $now: Current UTC timestamp (ISO 8601)
    - $today, $date: Today's date (YYYY-MM-DD)
    - $yesterday: Yesterday's date
    - $7_days_ago, $30_days_ago, $90_days_ago: Relative dates
    - $start_of_week: Monday of current week
    - $start_of_month: First day of current month
    - $start_of_year: First day of current year
    - $today_compact, $yesterday_compact, etc.: YYYYMMDD format (for APIs like openFDA)
    """
    import datetime

    utc_now = datetime.datetime.now(datetime.timezone.utc)
    today = utc_now.date()

    return {
        # ISO format (YYYY-MM-DD)
        "$now": utc_now.isoformat(),
        "$today": today.isoformat(),
        "$yesterday": (today - datetime.timedelta(days=1)).isoformat(),
        "$date": today.isoformat(),
        "$7_days_ago": (today - datetime.timedelta(days=7)).isoformat(),
        "$30_days_ago": (today - datetime.timedelta(days=30)).isoformat(),
        "$90_days_ago": (today - datetime.timedelta(days=90)).isoformat(),
        "$start_of_week": (today - datetime.timedelta(days=today.weekday())).isoformat(),
        "$start_of_month": today.replace(day=1).isoformat(),
        "$start_of_year": today.replace(month=1, day=1).isoformat(),
        # Compact format (YYYYMMDD) for APIs like openFDA
        "$today_compact": today.strftime("%Y%m%d"),
        "$yesterday_compact": (today - datetime.timedelta(days=1)).strftime("%Y%m%d"),
        "$7_days_ago_compact": (today - datetime.timedelta(days=7)).strftime("%Y%m%d"),
        "$30_days_ago_compact": (today - datetime.timedelta(days=30)).strftime("%Y%m%d"),
        "$90_days_ago_compact": (today - datetime.timedelta(days=90)).strftime("%Y%m%d"),
        "$start_of_week_compact": (today - datetime.timedelta(days=today.weekday())).strftime(
            "%Y%m%d"
        ),
        "$start_of_month_compact": today.replace(day=1).strftime("%Y%m%d"),
        "$start_of_year_compact": today.replace(month=1, day=1).strftime("%Y%m%d"),
    }


def substitute_date_variables(value: Any) -> Any:
    """Substitute date variables in a string value.

    Supports two syntaxes:

    1. **$variable** syntax (predefined):
       - `$now`, `$today`, `$yesterday`, `$7_days_ago`, etc.
       - `$today_compact`, `$30_days_ago_compact`, etc. (YYYYMMDD format)

    2. **{expression:format}** syntax (flexible):
       - `{today}` or `{today:%Y-%m-%d}` - today with optional format
       - `{now}` or `{now:%Y-%m-%dT%H:%M:%S}` - current datetime
       - `{-7d}` or `{-7d:%Y%m%d}` - 7 days ago with format
       - `{-30d:%Y%m%d}` - 30 days ago in YYYYMMDD
       - `{+1d}` - tomorrow
       - `{start_of_month:%Y-%m-%d}` - first of month

    Supports nested dicts and strings. Returns the value unchanged if not a string.
    """
    if isinstance(value, str):
        # First, handle $variable syntax
        date_vars = get_date_variables()
        for var, replacement in date_vars.items():
            if var in value:
                value = value.replace(var, replacement)

        # Then handle {expression:format} syntax
        value = _substitute_curly_brace_dates(value)

        return value
    elif isinstance(value, dict):
        return {k: substitute_date_variables(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [substitute_date_variables(v) for v in value]
    return value


def _substitute_curly_brace_dates(value: str) -> str:
    """Handle {expression:format} date substitution.

    Examples:
        {today} -> 2024-01-15
        {today:%Y%m%d} -> 20240115
        {-30d} -> 2023-12-16
        {-30d:%Y%m%d} -> 20231216
        {now:%Y-%m-%dT%H:%M:%SZ} -> 2024-01-15T10:30:00Z
        {start_of_month} -> 2024-01-01
    """
    import datetime
    import re

    utc_now = datetime.datetime.now(datetime.timezone.utc)
    today = utc_now.date()

    # Pattern: {expression} or {expression:format}
    pattern = r"\{([^}:]+)(?::([^}]+))?\}"

    def replace_match(match):
        expr = match.group(1).strip()
        fmt = match.group(2)  # Optional format string

        # Calculate the date based on expression
        if expr == "now":
            dt = utc_now
            default_fmt = "%Y-%m-%dT%H:%M:%S%z"
        elif expr == "today" or expr == "date":
            dt = today
            default_fmt = "%Y-%m-%d"
        elif expr == "yesterday":
            dt = today - datetime.timedelta(days=1)
            default_fmt = "%Y-%m-%d"
        elif expr == "start_of_week":
            dt = today - datetime.timedelta(days=today.weekday())
            default_fmt = "%Y-%m-%d"
        elif expr == "start_of_month":
            dt = today.replace(day=1)
            default_fmt = "%Y-%m-%d"
        elif expr == "start_of_year":
            dt = today.replace(month=1, day=1)
            default_fmt = "%Y-%m-%d"
        elif re.match(r"^[+-]?\d+d$", expr):
            # Relative days: -7d, +1d, -30d
            days = int(expr.replace("d", ""))
            dt = today + datetime.timedelta(days=days)
            default_fmt = "%Y-%m-%d"
        else:
            # Unknown expression, return as-is
            return match.group(0)

        # Use provided format or default
        use_fmt = fmt if fmt else default_fmt

        # Handle datetime vs date objects
        if isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime):
            # Convert date to datetime for strftime compatibility
            dt = datetime.datetime.combine(dt, datetime.time.min)

        return dt.strftime(use_fmt)

    return re.sub(pattern, replace_match, value)


class ResponseExtractor:
    """Extracts items from API responses using a dotted path."""

    def __init__(self, items_path: str = "", add_fields: Optional[Dict[str, Any]] = None):
        """Initialize extractor.

        Args:
            items_path: Dotted path to items (e.g., "results", "data.items", "").
                       Empty string means response is the list itself.
            add_fields: Fields to add to each record (e.g., {"_fetched_at": "$now"})
        """
        self.items_path = items_path
        self.add_fields = add_fields or {}

    def extract_items(self, response_json: Any) -> List[Dict[str, Any]]:
        """Extract items from response using the configured path."""
        if not self.items_path:
            # Response itself should be a list
            items = response_json if isinstance(response_json, list) else []
        else:
            # Navigate dotted path
            obj = response_json
            for key in self.items_path.split("."):
                if isinstance(obj, dict) and key in obj:
                    obj = obj[key]
                else:
                    return []
            items = obj if isinstance(obj, list) else []

        # Add extra fields with date variable substitution
        if self.add_fields:
            for item in items:
                for k, v in self.add_fields.items():
                    item[k] = substitute_date_variables(v) if isinstance(v, str) else v

        return items


# =============================================================================
# Pagination Strategies
# =============================================================================


class PaginationStrategy(Protocol):
    """Protocol for pagination strategies."""

    def initial_request(self, base: ApiRequest) -> ApiRequest:
        """Create the initial request."""
        ...

    def next_request(self, page: ApiPage) -> Optional[ApiRequest]:
        """Create the next request, or None if done."""
        ...


class OffsetLimitPagination:
    """Pagination using offset/skip and limit parameters.

    For GET requests, pagination params are added to URL query string.
    For POST/PUT/PATCH requests, pagination params are added to JSON body.
    """

    def __init__(
        self,
        offset_param: str = "offset",
        limit_param: str = "limit",
        limit: int = 100,
        max_pages: int = 100,
        stop_on_empty: bool = True,
        start_offset: int = 0,
    ):
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.limit = limit
        self.max_pages = max_pages
        self.stop_on_empty = stop_on_empty
        self.start_offset = start_offset
        self._current_offset = start_offset
        self._page_count = 0

    def _apply_pagination(self, base: ApiRequest, offset: int) -> ApiRequest:
        """Apply pagination params to either params or json_body based on method."""
        # For POST/PUT/PATCH with a body, add pagination to body
        if base.method in ("POST", "PUT", "PATCH") and base.json_body is not None:
            body = dict(base.json_body)
            body[self.offset_param] = offset
            body[self.limit_param] = self.limit
            return ApiRequest(
                method=base.method,
                url=base.url,
                params=base.params,
                json_body=body,
            )
        else:
            # For GET or requests without body, add to URL params
            params = dict(base.params)
            params[self.offset_param] = offset
            params[self.limit_param] = self.limit
            return ApiRequest(
                method=base.method,
                url=base.url,
                params=params,
                json_body=base.json_body,
            )

    def initial_request(self, base: ApiRequest) -> ApiRequest:
        self._current_offset = self.start_offset
        self._page_count = 0
        return self._apply_pagination(base, self.start_offset)

    def next_request(self, page: ApiPage) -> Optional[ApiRequest]:
        self._page_count += 1

        # Stop conditions
        if self._page_count >= self.max_pages:
            return None
        if self.stop_on_empty and len(page.items) == 0:
            return None
        if len(page.items) < self.limit:
            # Last page - got fewer than requested
            return None

        self._current_offset += self.limit
        return self._apply_pagination(page.request, self._current_offset)


class PageNumberPagination:
    """Pagination using page number and page size parameters."""

    def __init__(
        self,
        page_param: str = "page",
        page_size_param: str = "per_page",
        page_size: int = 100,
        start_page: int = 1,
        max_pages: int = 100,
        stop_on_empty: bool = True,
    ):
        self.page_param = page_param
        self.page_size_param = page_size_param
        self.page_size = page_size
        self.start_page = start_page
        self.max_pages = max_pages
        self.stop_on_empty = stop_on_empty
        self._current_page = start_page
        self._page_count = 0

    def initial_request(self, base: ApiRequest) -> ApiRequest:
        self._current_page = self.start_page
        self._page_count = 0
        params = dict(base.params)
        params[self.page_param] = self.start_page
        params[self.page_size_param] = self.page_size
        return ApiRequest(
            method=base.method,
            url=base.url,
            params=params,
            json_body=base.json_body,
        )

    def next_request(self, page: ApiPage) -> Optional[ApiRequest]:
        self._page_count += 1

        if self._page_count >= self.max_pages:
            return None
        if self.stop_on_empty and len(page.items) == 0:
            return None
        if len(page.items) < self.page_size:
            return None

        self._current_page += 1
        params = dict(page.request.params)
        params[self.page_param] = self._current_page
        return ApiRequest(
            method=page.request.method,
            url=page.request.url,
            params=params,
            json_body=page.request.json_body,
        )


class CursorPagination:
    """Pagination using cursor/next_token pattern."""

    def __init__(
        self,
        cursor_param: str = "cursor",
        cursor_path: str = "next_cursor",
        max_pages: int = 100,
        stop_when_cursor_missing: bool = True,
    ):
        self.cursor_param = cursor_param
        self.cursor_path = cursor_path
        self.max_pages = max_pages
        self.stop_when_cursor_missing = stop_when_cursor_missing
        self._page_count = 0

    def initial_request(self, base: ApiRequest) -> ApiRequest:
        self._page_count = 0
        return base

    def _get_cursor(self, response: Dict[str, Any]) -> Optional[str]:
        """Extract cursor from response using dotted path."""
        obj = response
        for key in self.cursor_path.split("."):
            if isinstance(obj, dict) and key in obj:
                obj = obj[key]
            else:
                return None
        return str(obj) if obj else None

    def next_request(self, page: ApiPage) -> Optional[ApiRequest]:
        self._page_count += 1

        if self._page_count >= self.max_pages:
            return None

        cursor = self._get_cursor(page.raw)
        if not cursor and self.stop_when_cursor_missing:
            return None

        params = dict(page.request.params)
        params[self.cursor_param] = cursor
        return ApiRequest(
            method=page.request.method,
            url=page.request.url,
            params=params,
            json_body=page.request.json_body,
        )


class LinkHeaderPagination:
    """Pagination using RFC 5988 Link headers (GitHub style)."""

    def __init__(self, link_rel: str = "next", max_pages: int = 100):
        self.link_rel = link_rel
        self.max_pages = max_pages
        self._page_count = 0

    def initial_request(self, base: ApiRequest) -> ApiRequest:
        self._page_count = 0
        return base

    def _parse_link_header(self, link_header: str) -> Dict[str, str]:
        """Parse RFC 5988 Link header."""
        links = {}
        for part in link_header.split(","):
            match = re.match(r'<([^>]+)>;\s*rel="?([^"]+)"?', part.strip())
            if match:
                url, rel = match.groups()
                links[rel] = url
        return links

    def next_request(self, page: ApiPage) -> Optional[ApiRequest]:
        self._page_count += 1

        if self._page_count >= self.max_pages:
            return None

        link_header = page.headers.get("link", "")
        links = self._parse_link_header(link_header)
        next_url = links.get(self.link_rel)

        if not next_url:
            return None

        # Parse the URL to extract any query params
        parsed = urlparse(next_url)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

        return ApiRequest(
            method=page.request.method,
            url=f"{parsed.scheme}://{parsed.netloc}{parsed.path}",
            params=params,
            json_body=page.request.json_body,
        )


class NoPagination:
    """No pagination - single request only."""

    def initial_request(self, base: ApiRequest) -> ApiRequest:
        return base

    def next_request(self, page: ApiPage) -> Optional[ApiRequest]:
        return None


# =============================================================================
# Retry Policy
# =============================================================================


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay_s: float = 1.0
    max_delay_s: float = 60.0
    exponential_base: float = 2.0
    retry_on_status: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number."""
        delay = self.base_delay_s * (self.exponential_base ** (attempt - 1))
        return min(delay, self.max_delay_s)


# =============================================================================
# Rate Limiter
# =============================================================================


class RateLimiter:
    """Simple rate limiter with auto-detection from headers."""

    def __init__(
        self,
        mode: str = "auto",
        requests_per_second: Optional[float] = None,
    ):
        self.mode = mode
        self.requests_per_second = requests_per_second
        self._last_request_time: Optional[float] = None

    def wait_if_needed(self, headers: Optional[Dict[str, str]] = None):
        """Wait if rate limiting is needed."""
        ctx = get_logging_context()

        if self.mode == "fixed" and self.requests_per_second:
            min_interval = 1.0 / self.requests_per_second
            if self._last_request_time:
                elapsed = time.time() - self._last_request_time
                if elapsed < min_interval:
                    sleep_time = min_interval - elapsed
                    ctx.debug("Rate limiting", sleep_seconds=sleep_time)
                    time.sleep(sleep_time)

        elif self.mode == "auto" and headers:
            # Check for Retry-After header (common for 429)
            retry_after = headers.get("retry-after")
            if retry_after:
                try:
                    sleep_time = float(retry_after)
                    ctx.info("Rate limited by Retry-After header", sleep_seconds=sleep_time)
                    time.sleep(sleep_time)
                    return
                except ValueError:
                    pass

            # Check GitHub-style rate limit headers
            remaining = headers.get("x-ratelimit-remaining")
            reset_time = headers.get("x-ratelimit-reset")
            if remaining == "0" and reset_time:
                try:
                    reset_ts = float(reset_time)
                    sleep_time = max(0, reset_ts - time.time() + 1)
                    ctx.info("Rate limited by x-ratelimit headers", sleep_seconds=sleep_time)
                    time.sleep(sleep_time)
                except ValueError:
                    pass

        self._last_request_time = time.time()


# =============================================================================
# API Fetcher
# =============================================================================


PAGINATION_STRATEGIES: Dict[str, Type[PaginationStrategy]] = {
    "offset_limit": OffsetLimitPagination,
    "page_number": PageNumberPagination,
    "cursor": CursorPagination,
    "link_header": LinkHeaderPagination,
    "none": NoPagination,
}


def create_pagination_strategy(config: Dict[str, Any]) -> PaginationStrategy:
    """Create a pagination strategy from config dict."""
    strategy_type = config.get("type", "none")
    strategy_class = PAGINATION_STRATEGIES.get(strategy_type)

    if not strategy_class:
        raise ValueError(f"Unknown pagination type: {strategy_type}")

    # Filter config to only include valid init params
    import inspect

    sig = inspect.signature(strategy_class.__init__)
    valid_params = {k for k in sig.parameters.keys() if k != "self"}
    init_args = {k: v for k, v in config.items() if k in valid_params}

    return strategy_class(**init_args)


class ApiFetcher:
    """Orchestrates paginated API fetching with retries and rate limiting."""

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        extractor: Optional[ResponseExtractor] = None,
        paginator: Optional[PaginationStrategy] = None,
        retry_policy: Optional[RetryPolicy] = None,
        rate_limiter: Optional[RateLimiter] = None,
        timeout_s: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.extractor = extractor or ResponseExtractor()
        self.paginator = paginator or NoPagination()
        self.retry_policy = retry_policy or RetryPolicy()
        self.rate_limiter = rate_limiter or RateLimiter()
        self.timeout_s = timeout_s
        self._ctx = get_logging_context()

    def _make_request(self, request: ApiRequest) -> Tuple[int, Dict[str, str], Any]:
        """Make a single HTTP request with retry logic."""
        url = request.url
        if request.params:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{urlencode(request.params, doseq=True, safe='+:[]')}"

        data = None
        headers = dict(self.headers)
        if request.json_body is not None:
            data = json.dumps(request.json_body).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")

        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                self._ctx.debug(
                    "API request",
                    method=request.method,
                    url=url,
                    attempt=attempt,
                )

                req = Request(url, data=data, headers=headers, method=request.method.upper())
                with urlopen(req, timeout=self.timeout_s) as resp:
                    status = resp.status
                    resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                    raw = resp.read()
                    body = json.loads(raw) if raw else {}
                    return status, resp_headers, body

            except HTTPError as e:
                status = e.code
                resp_headers = {k.lower(): v for k, v in e.headers.items()}

                if status in self.retry_policy.retry_on_status:
                    if attempt < self.retry_policy.max_attempts:
                        delay = self.retry_policy.get_delay(attempt)

                        # Check for Retry-After header
                        retry_after = resp_headers.get("retry-after")
                        if retry_after:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                pass

                        self._ctx.warning(
                            "Retrying after error",
                            status=status,
                            attempt=attempt,
                            delay=delay,
                        )
                        time.sleep(delay)
                        continue

                # Re-raise if not retryable or max attempts reached
                raise

            except (URLError, TimeoutError) as e:
                if attempt < self.retry_policy.max_attempts:
                    delay = self.retry_policy.get_delay(attempt)
                    self._ctx.warning(
                        "Retrying after network error",
                        error=str(e),
                        attempt=attempt,
                        delay=delay,
                    )
                    time.sleep(delay)
                    continue
                raise

        # Should not reach here, but just in case
        raise RuntimeError("Max retry attempts exceeded")

    def _fetch_page(self, request: ApiRequest) -> ApiPage:
        """Fetch a single page of results."""
        status, headers, body = self._make_request(request)
        items = self.extractor.extract_items(body)

        self._ctx.debug(
            "Page fetched",
            status=status,
            items_count=len(items),
        )

        return ApiPage(
            items=items,
            raw=body,
            headers=headers,
            request=request,
            status_code=status,
        )

    def fetch_iter(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        request_body: Optional[Dict[str, Any]] = None,
    ) -> Iterator[ApiPage]:
        """Iterate through all pages of results.

        Args:
            endpoint: API endpoint path or full URL
            params: Query parameters (for GET) or merged into body (for POST)
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            request_body: JSON body for POST/PUT/PATCH requests

        Yields:
            ApiPage objects for each page of results.
        """
        # Substitute date variables in params and request_body
        resolved_params = substitute_date_variables(params) if params else {}
        resolved_body = substitute_date_variables(request_body) if request_body else None

        # If endpoint is already a full URL, use it directly
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            url = endpoint
        else:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # For POST/PUT/PATCH, merge params into body if body exists
        if method.upper() in ("POST", "PUT", "PATCH") and resolved_body is not None:
            # Params become part of the JSON body, not URL query string
            final_body = {**resolved_body, **resolved_params} if resolved_params else resolved_body
            final_params = {}
        else:
            final_body = resolved_body
            final_params = resolved_params

        base_request = ApiRequest(
            method=method.upper(),
            url=url,
            params=final_params,
            json_body=final_body,
        )

        request = self.paginator.initial_request(base_request)
        page_num = 0

        while request is not None:
            page_num += 1
            self._ctx.info("Fetching page", page=page_num, method=request.method, url=request.url)

            page = self._fetch_page(request)
            yield page

            # Rate limit before next request
            self.rate_limiter.wait_if_needed(page.headers)

            request = self.paginator.next_request(page)

    def fetch_dataframe(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_records: Optional[int] = None,
        method: str = "GET",
        request_body: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Fetch all pages and return as a single DataFrame.

        Args:
            endpoint: API endpoint relative to base_url
            params: Query parameters
            max_records: Maximum records to return (safety limit)
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            request_body: JSON body for POST/PUT/PATCH requests

        Returns:
            DataFrame with all results concatenated
        """
        frames = []
        total_records = 0

        for page in self.fetch_iter(endpoint, params, method, request_body):
            if not page.items:
                continue

            df = pd.json_normalize(page.items, sep="_")
            frames.append(df)
            total_records += len(page.items)

            if max_records and total_records >= max_records:
                self._ctx.info("Reached max_records limit", max_records=max_records)
                break

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        self._ctx.info("Fetch complete", total_records=len(result))
        return result

    def fetch_dataframe_safe(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_records: Optional[int] = None,
        method: str = "GET",
        request_body: Optional[Dict[str, Any]] = None,
    ) -> FetchResult:
        """Fetch all pages, preserving partial data on error.

        Unlike fetch_dataframe(), this method catches errors and returns
        whatever data was successfully collected before the failure.

        Args:
            endpoint: API endpoint relative to base_url
            params: Query parameters
            max_records: Maximum records to return (safety limit)
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            request_body: JSON body for POST/PUT/PATCH requests

        Returns:
            FetchResult with df (partial or complete), error info, and metadata
        """
        frames = []
        total_records = 0
        pages_fetched = 0
        error: Optional[Exception] = None

        try:
            for page in self.fetch_iter(endpoint, params, method, request_body):
                pages_fetched += 1

                if not page.items:
                    continue

                df = pd.json_normalize(page.items, sep="_")
                frames.append(df)
                total_records += len(page.items)

                if max_records and total_records >= max_records:
                    self._ctx.info("Reached max_records limit", max_records=max_records)
                    break
        except Exception as e:
            error = e
            self._ctx.error(
                "Fetch failed, returning partial results",
                pages_fetched=pages_fetched,
                records_collected=total_records,
                error=str(e),
            )

        if not frames:
            result_df = pd.DataFrame()
        else:
            result_df = pd.concat(frames, ignore_index=True)

        complete = error is None
        if complete:
            self._ctx.info("Fetch complete", total_records=len(result_df))

        return FetchResult(
            df=result_df,
            error=error,
            pages_fetched=pages_fetched,
            complete=complete,
        )


# =============================================================================
# Factory function for easy YAML-driven creation
# =============================================================================


def create_api_fetcher(
    base_url: str,
    headers: Optional[Dict[str, str]] = None,
    options: Optional[Dict[str, Any]] = None,
) -> ApiFetcher:
    """Create an ApiFetcher from configuration options.

    Args:
        base_url: Base URL for the API
        headers: HTTP headers including auth
        options: Configuration dict with pagination, response, http settings

    Returns:
        Configured ApiFetcher instance
    """
    options = options or {}

    # Pagination
    pagination_config = options.get("pagination", {"type": "none"})
    paginator = create_pagination_strategy(pagination_config)

    # Response extraction
    response_config = options.get("response", {})
    extractor = ResponseExtractor(
        items_path=response_config.get("items_path", ""),
        add_fields=response_config.get("add_fields"),
    )

    # Retry policy (matches ApiRetryConfig schema)
    retry_config = options.get("retry", {})
    retry_policy = RetryPolicy(
        max_attempts=retry_config.get("max_retries", 3),
        base_delay_s=1.0,
        max_delay_s=60.0,
        exponential_base=retry_config.get("backoff_factor", 2.0),
        retry_on_status=retry_config.get("retry_codes", [429, 500, 502, 503, 504]),
    )

    # Rate limiter (matches ApiRateLimitConfig schema)
    rate_limit_config = options.get("rate_limit", {})
    rate_limiter = RateLimiter(
        mode="fixed" if rate_limit_config.get("requests_per_second") else "auto",
        requests_per_second=rate_limit_config.get("requests_per_second"),
    )

    # HTTP timeout (optional, not in schema but useful)
    http_config = options.get("http", {})
    timeout_s = http_config.get("timeout_s", 30.0)

    return ApiFetcher(
        base_url=base_url,
        headers=headers,
        extractor=extractor,
        paginator=paginator,
        retry_policy=retry_policy,
        rate_limiter=rate_limiter,
        timeout_s=timeout_s,
    )
