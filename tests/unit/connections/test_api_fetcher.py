"""Tests for API Fetcher."""

import json
from unittest.mock import MagicMock, patch

import pytest

from odibi.connections.api_fetcher import (
    ApiRequest,
    CursorPagination,
    LinkHeaderPagination,
    NoPagination,
    OffsetLimitPagination,
    ResponseExtractor,
    RetryPolicy,
    create_api_fetcher,
    create_pagination_strategy,
)


class TestResponseExtractor:
    """Tests for ResponseExtractor."""

    def test_extract_root_list(self):
        """Test extracting when response is a list."""
        extractor = ResponseExtractor(items_path="")
        response = [{"id": 1}, {"id": 2}]
        items = extractor.extract_items(response)
        assert items == [{"id": 1}, {"id": 2}]

    def test_extract_nested_path(self):
        """Test extracting with dotted path."""
        extractor = ResponseExtractor(items_path="data.items")
        response = {"data": {"items": [{"id": 1}], "meta": {}}}
        items = extractor.extract_items(response)
        assert items == [{"id": 1}]

    def test_extract_single_key(self):
        """Test extracting with single key (openFDA style)."""
        extractor = ResponseExtractor(items_path="results")
        response = {"meta": {}, "results": [{"recall_number": "R-001"}]}
        items = extractor.extract_items(response)
        assert items == [{"recall_number": "R-001"}]

    def test_extract_missing_path(self):
        """Test extracting when path doesn't exist."""
        extractor = ResponseExtractor(items_path="missing.path")
        response = {"data": []}
        items = extractor.extract_items(response)
        assert items == []

    def test_add_fields(self):
        """Test adding fields to extracted items."""
        extractor = ResponseExtractor(
            items_path="results",
            add_fields={"source": "test", "_fetched_at": "$now"},
        )
        response = {"results": [{"id": 1}]}
        items = extractor.extract_items(response)
        assert items[0]["source"] == "test"
        assert "_fetched_at" in items[0]

    def test_date_variables(self):
        """Test date variable substitution in add_fields."""
        import datetime

        extractor = ResponseExtractor(
            items_path="",
            add_fields={
                "_now": "$now",
                "_today": "$today",
                "_yesterday": "$yesterday",
                "_date": "$date",
                "_7_days": "$7_days_ago",
                "_30_days": "$30_days_ago",
                "_90_days": "$90_days_ago",
                "_week_start": "$start_of_week",
                "_month_start": "$start_of_month",
                "_year_start": "$start_of_year",
                "_static": "constant_value",
            },
        )
        response = [{"id": 1}]
        items = extractor.extract_items(response)

        today = datetime.date.today()
        item = items[0]

        # Static value preserved
        assert item["_static"] == "constant_value"

        # Date values are ISO format strings
        assert item["_today"] == today.isoformat()
        assert item["_date"] == today.isoformat()  # Alias
        assert item["_yesterday"] == (today - datetime.timedelta(days=1)).isoformat()
        assert item["_7_days"] == (today - datetime.timedelta(days=7)).isoformat()
        assert item["_30_days"] == (today - datetime.timedelta(days=30)).isoformat()
        assert item["_90_days"] == (today - datetime.timedelta(days=90)).isoformat()
        assert item["_week_start"] == (today - datetime.timedelta(days=today.weekday())).isoformat()
        assert item["_month_start"] == today.replace(day=1).isoformat()
        assert item["_year_start"] == today.replace(month=1, day=1).isoformat()

        # $now has timezone info (ends with +00:00)
        assert "+00:00" in item["_now"] or "Z" in item["_now"]

    def test_curly_brace_date_syntax(self):
        """Test {expression:format} date variable syntax."""
        import datetime

        from odibi.connections.api_fetcher import substitute_date_variables

        today = datetime.date.today()

        # Basic expressions
        assert substitute_date_variables("{today}") == today.isoformat()
        assert substitute_date_variables("{yesterday}") == (
            today - datetime.timedelta(days=1)
        ).isoformat()

        # Custom format
        assert substitute_date_variables("{today:%Y%m%d}") == today.strftime("%Y%m%d")
        assert substitute_date_variables("{today:%d/%m/%Y}") == today.strftime(
            "%d/%m/%Y"
        )

        # Relative days
        assert substitute_date_variables("{-7d}") == (
            today - datetime.timedelta(days=7)
        ).isoformat()
        assert substitute_date_variables("{-30d:%Y%m%d}") == (
            today - datetime.timedelta(days=30)
        ).strftime("%Y%m%d")
        assert substitute_date_variables("{+1d}") == (
            today + datetime.timedelta(days=1)
        ).isoformat()

        # Period starts
        assert substitute_date_variables("{start_of_month}") == today.replace(
            day=1
        ).isoformat()
        assert substitute_date_variables("{start_of_year:%Y%m%d}") == today.replace(
            month=1, day=1
        ).strftime("%Y%m%d")

        # Embedded in string (like openFDA)
        result = substitute_date_variables(
            "report_date:[{-30d:%Y%m%d}+TO+{today:%Y%m%d}]"
        )
        expected_start = (today - datetime.timedelta(days=30)).strftime("%Y%m%d")
        expected_end = today.strftime("%Y%m%d")
        assert result == f"report_date:[{expected_start}+TO+{expected_end}]"

        # Unknown expressions preserved
        assert substitute_date_variables("{unknown}") == "{unknown}"


class TestOffsetLimitPagination:
    """Tests for OffsetLimitPagination."""

    def test_initial_request(self):
        """Test initial request has offset 0."""
        paginator = OffsetLimitPagination(
            offset_param="skip",
            limit_param="limit",
            limit=100,
        )
        base = ApiRequest(method="GET", url="https://api.test.com/data")
        req = paginator.initial_request(base)
        assert req.params["skip"] == 0
        assert req.params["limit"] == 100

    def test_next_request_increments_offset(self):
        """Test next request increments offset."""
        paginator = OffsetLimitPagination(limit=100)
        base = ApiRequest(method="GET", url="https://api.test.com/data")
        req = paginator.initial_request(base)

        # Simulate a full page
        from odibi.connections.api_fetcher import ApiPage

        page = ApiPage(
            items=[{"id": i} for i in range(100)],
            raw={},
            headers={},
            request=req,
        )

        next_req = paginator.next_request(page)
        assert next_req is not None
        assert next_req.params["offset"] == 100

    def test_stops_on_empty(self):
        """Test pagination stops on empty results."""
        paginator = OffsetLimitPagination(limit=100, stop_on_empty=True)
        base = ApiRequest(method="GET", url="https://api.test.com/data")
        req = paginator.initial_request(base)

        from odibi.connections.api_fetcher import ApiPage

        page = ApiPage(items=[], raw={}, headers={}, request=req)
        assert paginator.next_request(page) is None

    def test_stops_on_partial_page(self):
        """Test pagination stops when fewer items than limit."""
        paginator = OffsetLimitPagination(limit=100)
        base = ApiRequest(method="GET", url="https://api.test.com/data")
        req = paginator.initial_request(base)

        from odibi.connections.api_fetcher import ApiPage

        page = ApiPage(
            items=[{"id": i} for i in range(50)],  # Less than limit
            raw={},
            headers={},
            request=req,
        )
        assert paginator.next_request(page) is None

    def test_respects_max_pages(self):
        """Test pagination respects max_pages limit."""
        paginator = OffsetLimitPagination(limit=10, max_pages=2)
        base = ApiRequest(method="GET", url="https://api.test.com/data")
        req = paginator.initial_request(base)

        from odibi.connections.api_fetcher import ApiPage

        # First page
        page1 = ApiPage(
            items=[{"id": i} for i in range(10)],
            raw={},
            headers={},
            request=req,
        )
        req2 = paginator.next_request(page1)
        assert req2 is not None

        # Second page - should stop
        page2 = ApiPage(
            items=[{"id": i} for i in range(10)],
            raw={},
            headers={},
            request=req2,
        )
        assert paginator.next_request(page2) is None

    def test_post_request_pagination_in_body(self):
        """Test that POST requests have pagination params in JSON body."""
        paginator = OffsetLimitPagination(
            offset_param="start",
            limit_param="rows",
            limit=100,
            start_offset=1,  # FDA Data Dashboard is 1-indexed
        )
        base = ApiRequest(
            method="POST",
            url="https://api.test.com/data",
            json_body={"filters": {"status": ["active"]}},
        )
        req = paginator.initial_request(base)

        # Pagination should be in body, not params
        assert req.json_body["start"] == 1
        assert req.json_body["rows"] == 100
        assert req.json_body["filters"] == {"status": ["active"]}
        assert "start" not in req.params
        assert "rows" not in req.params

        from odibi.connections.api_fetcher import ApiPage

        # Simulate full page
        page = ApiPage(
            items=[{"id": i} for i in range(100)],
            raw={},
            headers={},
            request=req,
        )

        next_req = paginator.next_request(page)
        assert next_req is not None
        assert next_req.json_body["start"] == 101
        assert next_req.json_body["rows"] == 100
        # Original body should be preserved
        assert next_req.json_body["filters"] == {"status": ["active"]}

    def test_get_request_pagination_in_params(self):
        """Test that GET requests have pagination params in URL params."""
        paginator = OffsetLimitPagination(
            offset_param="skip",
            limit_param="limit",
            limit=50,
        )
        base = ApiRequest(
            method="GET",
            url="https://api.test.com/data",
        )
        req = paginator.initial_request(base)

        # Pagination should be in params
        assert req.params["skip"] == 0
        assert req.params["limit"] == 50
        assert req.json_body is None


class TestCursorPagination:
    """Tests for CursorPagination."""

    def test_extracts_cursor(self):
        """Test cursor extraction from response."""
        paginator = CursorPagination(
            cursor_param="next_token",
            cursor_path="meta.next_cursor",
        )
        base = ApiRequest(method="GET", url="https://api.test.com/data")
        req = paginator.initial_request(base)

        from odibi.connections.api_fetcher import ApiPage

        page = ApiPage(
            items=[{"id": 1}],
            raw={"meta": {"next_cursor": "abc123"}, "data": []},
            headers={},
            request=req,
        )
        next_req = paginator.next_request(page)
        assert next_req is not None
        assert next_req.params["next_token"] == "abc123"

    def test_stops_when_cursor_missing(self):
        """Test pagination stops when cursor is missing."""
        paginator = CursorPagination(
            cursor_param="cursor",
            cursor_path="next",
            stop_when_cursor_missing=True,
        )
        base = ApiRequest(method="GET", url="https://api.test.com/data")
        req = paginator.initial_request(base)

        from odibi.connections.api_fetcher import ApiPage

        page = ApiPage(
            items=[{"id": 1}],
            raw={"data": []},  # No cursor
            headers={},
            request=req,
        )
        assert paginator.next_request(page) is None


class TestLinkHeaderPagination:
    """Tests for LinkHeaderPagination."""

    def test_parses_link_header(self):
        """Test parsing RFC 5988 Link header."""
        paginator = LinkHeaderPagination(link_rel="next")
        base = ApiRequest(method="GET", url="https://api.github.com/repos")
        req = paginator.initial_request(base)

        from odibi.connections.api_fetcher import ApiPage

        page = ApiPage(
            items=[{"id": 1}],
            raw=[],
            headers={
                "link": '<https://api.github.com/repos?page=2>; rel="next", '
                '<https://api.github.com/repos?page=10>; rel="last"'
            },
            request=req,
        )
        next_req = paginator.next_request(page)
        assert next_req is not None
        assert next_req.params.get("page") == "2"

    def test_stops_when_no_next_link(self):
        """Test pagination stops when no next link."""
        paginator = LinkHeaderPagination(link_rel="next")
        base = ApiRequest(method="GET", url="https://api.github.com/repos")
        req = paginator.initial_request(base)

        from odibi.connections.api_fetcher import ApiPage

        page = ApiPage(
            items=[{"id": 1}],
            raw=[],
            headers={"link": '<https://api.github.com/repos?page=1>; rel="first"'},
            request=req,
        )
        assert paginator.next_request(page) is None


class TestCreatePaginationStrategy:
    """Tests for create_pagination_strategy factory."""

    def test_creates_offset_limit(self):
        """Test creating offset/limit strategy."""
        strategy = create_pagination_strategy(
            {"type": "offset_limit", "offset_param": "skip", "limit": 500}
        )
        assert isinstance(strategy, OffsetLimitPagination)

    def test_creates_cursor(self):
        """Test creating cursor strategy."""
        strategy = create_pagination_strategy(
            {"type": "cursor", "cursor_param": "token", "cursor_path": "next"}
        )
        assert isinstance(strategy, CursorPagination)

    def test_creates_none(self):
        """Test creating no pagination strategy."""
        strategy = create_pagination_strategy({"type": "none"})
        assert isinstance(strategy, NoPagination)

    def test_invalid_type_raises(self):
        """Test invalid type raises error."""
        with pytest.raises(ValueError, match="Unknown pagination type"):
            create_pagination_strategy({"type": "invalid"})


class TestRetryPolicy:
    """Tests for RetryPolicy."""

    def test_exponential_backoff(self):
        """Test exponential backoff calculation."""
        policy = RetryPolicy(base_delay_s=1.0, exponential_base=2.0)
        assert policy.get_delay(1) == 1.0
        assert policy.get_delay(2) == 2.0
        assert policy.get_delay(3) == 4.0

    def test_respects_max_delay(self):
        """Test delay is capped at max."""
        policy = RetryPolicy(base_delay_s=1.0, max_delay_s=5.0, exponential_base=2.0)
        assert policy.get_delay(10) == 5.0


class TestCreateApiFetcher:
    """Tests for create_api_fetcher factory."""

    def test_creates_with_defaults(self):
        """Test creating fetcher with minimal config."""
        fetcher = create_api_fetcher(
            base_url="https://api.test.com",
            headers={"X-API-Key": "test"},
        )
        assert fetcher.base_url == "https://api.test.com"
        assert fetcher.headers["X-API-Key"] == "test"

    def test_creates_with_full_config(self):
        """Test creating fetcher with full config."""
        fetcher = create_api_fetcher(
            base_url="https://api.fda.gov",
            options={
                "pagination": {
                    "type": "offset_limit",
                    "offset_param": "skip",
                    "limit_param": "limit",
                    "limit": 1000,
                    "max_pages": 50,
                },
                "response": {"items_path": "results"},
                "http": {
                    "timeout_s": 60,
                    "retries": {"max_attempts": 5},
                    "rate_limit": {"type": "fixed", "requests_per_second": 2},
                },
            },
        )
        assert isinstance(fetcher.paginator, OffsetLimitPagination)
        assert fetcher.extractor.items_path == "results"
        assert fetcher.timeout_s == 60


class TestApiFetcherIntegration:
    """Integration tests for ApiFetcher (mocked HTTP)."""

    def test_fetch_single_page(self):
        """Test fetching a single page of results."""
        with patch("odibi.connections.api_fetcher.urlopen") as mock_urlopen:
            # Mock response
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.headers = {}
            mock_response.read.return_value = json.dumps(
                {"results": [{"id": 1}, {"id": 2}]}
            ).encode()
            mock_response.__enter__ = lambda s: s
            mock_response.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_response

            fetcher = create_api_fetcher(
                base_url="https://api.test.com",
                options={
                    "pagination": {"type": "none"},
                    "response": {"items_path": "results"},
                },
            )

            df = fetcher.fetch_dataframe("/data")
            assert len(df) == 2
            assert list(df["id"]) == [1, 2]

    def test_fetch_multiple_pages(self):
        """Test fetching multiple pages."""
        call_count = [0]

        def mock_urlopen_side_effect(req, timeout=None):
            call_count[0] += 1
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.headers = {}

            # Return different data based on call count
            if call_count[0] == 1:
                data = {"results": [{"id": i} for i in range(10)]}
            elif call_count[0] == 2:
                data = {"results": [{"id": i} for i in range(10, 20)]}
            else:
                data = {"results": [{"id": 20}]}  # Partial page - stops pagination

            mock_response.read.return_value = json.dumps(data).encode()
            mock_response.__enter__ = lambda s: s
            mock_response.__exit__ = MagicMock(return_value=False)
            return mock_response

        with patch(
            "odibi.connections.api_fetcher.urlopen",
            side_effect=mock_urlopen_side_effect,
        ):
            fetcher = create_api_fetcher(
                base_url="https://api.test.com",
                options={
                    "pagination": {
                        "type": "offset_limit",
                        "limit": 10,
                        "max_pages": 10,
                    },
                    "response": {"items_path": "results"},
                },
            )

            df = fetcher.fetch_dataframe("/data")
            assert len(df) == 21  # 10 + 10 + 1
            assert call_count[0] == 3
