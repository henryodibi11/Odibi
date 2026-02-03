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


# =============================================================================
# Response Extractor
# =============================================================================


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
            import datetime

            utc_now = datetime.datetime.now(datetime.timezone.utc)
            today = utc_now.date()

            # Date variable substitution map
            # $now - full UTC timestamp in ISO format
            # $today - today's date (YYYY-MM-DD)
            # $yesterday - yesterday's date
            # $date - alias for $today
            # $7_days_ago, $30_days_ago, $90_days_ago - relative dates
            # $start_of_week - Monday of current week
            # $start_of_month - First day of current month
            # $start_of_year - First day of current year
            date_vars = {
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
            }

            for item in items:
                for k, v in self.add_fields.items():
                    if isinstance(v, str) and v in date_vars:
                        item[k] = date_vars[v]
                    else:
                        item[k] = v

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
    """Pagination using offset/skip and limit parameters."""

    def __init__(
        self,
        offset_param: str = "offset",
        limit_param: str = "limit",
        limit: int = 100,
        max_pages: int = 100,
        stop_on_empty: bool = True,
    ):
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.limit = limit
        self.max_pages = max_pages
        self.stop_on_empty = stop_on_empty
        self._current_offset = 0
        self._page_count = 0

    def initial_request(self, base: ApiRequest) -> ApiRequest:
        self._current_offset = 0
        self._page_count = 0
        params = dict(base.params)
        params[self.offset_param] = 0
        params[self.limit_param] = self.limit
        return ApiRequest(
            method=base.method,
            url=base.url,
            params=params,
            json_body=base.json_body,
        )

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
        params = dict(page.request.params)
        params[self.offset_param] = self._current_offset
        return ApiRequest(
            method=page.request.method,
            url=page.request.url,
            params=params,
            json_body=page.request.json_body,
        )


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
            url = f"{url}{sep}{urlencode(request.params, doseq=True)}"

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
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Iterator[ApiPage]:
        """Iterate through all pages of results.

        Yields:
            ApiPage objects for each page of results.
        """
        # If endpoint is already a full URL, use it directly
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            url = endpoint
        else:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
        base_request = ApiRequest(
            method="GET",
            url=url,
            params=params or {},
        )

        request = self.paginator.initial_request(base_request)
        page_num = 0

        while request is not None:
            page_num += 1
            self._ctx.info("Fetching page", page=page_num, url=request.url)

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
    ) -> pd.DataFrame:
        """Fetch all pages and return as a single DataFrame.

        Args:
            endpoint: API endpoint relative to base_url
            params: Query parameters
            max_records: Maximum records to return (safety limit)

        Returns:
            DataFrame with all results concatenated
        """
        frames = []
        total_records = 0

        for page in self.fetch_iter(endpoint, params):
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

    # HTTP settings
    http_config = options.get("http", {})
    timeout_s = http_config.get("timeout_s", 30.0)

    # Retry policy
    retry_config = http_config.get("retries", {})
    backoff_config = retry_config.get("backoff", {})
    retry_policy = RetryPolicy(
        max_attempts=retry_config.get("max_attempts", 3),
        base_delay_s=backoff_config.get("base_s", 1.0),
        max_delay_s=backoff_config.get("max_s", 60.0),
        exponential_base=backoff_config.get("exponential_base", 2.0),
        retry_on_status=retry_config.get("retry_on_status", [429, 500, 502, 503, 504]),
    )

    # Rate limiter
    rate_limit_config = http_config.get("rate_limit", {})
    rate_limiter = RateLimiter(
        mode=rate_limit_config.get("type", "auto"),
        requests_per_second=rate_limit_config.get("requests_per_second"),
    )

    return ApiFetcher(
        base_url=base_url,
        headers=headers,
        extractor=extractor,
        paginator=paginator,
        retry_policy=retry_policy,
        rate_limiter=rate_limiter,
        timeout_s=timeout_s,
    )
