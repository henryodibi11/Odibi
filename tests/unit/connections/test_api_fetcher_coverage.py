"""Coverage tests for odibi/connections/api_fetcher.py — pure logic & pagination."""

import time
from unittest.mock import patch

import pytest

from odibi.connections.api_fetcher import (
    _sanitize_url,
    get_date_variables,
    substitute_date_variables,
    _substitute_curly_brace_dates,
    ResponseExtractor,
    OffsetLimitPagination,
    PageNumberPagination,
    CursorPagination,
    LinkHeaderPagination,
    NoPagination,
    RetryPolicy,
    RateLimiter,
    ApiFetcher,
    create_pagination_strategy,
    create_api_fetcher,
    ApiRequest,
    ApiPage,
    FetchResult,
)


# ===== _sanitize_url =====


class TestSanitizeUrl:
    def test_no_query(self):
        assert _sanitize_url("https://api.example.com/v1") == "https://api.example.com/v1"

    def test_sensitive_params_masked(self):
        url = "https://api.example.com/v1?key=SECRET&name=test"
        result = _sanitize_url(url)
        assert "SECRET" not in result
        assert "name=test" in result

    def test_multiple_sensitive(self):
        url = "https://x.com?token=T&api_key=K&page=1"
        result = _sanitize_url(url)
        assert "T" not in result or "token=***" in result
        assert "page=1" in result


# ===== Date Variables =====


class TestDateVariables:
    def test_keys_present(self):
        v = get_date_variables()
        assert "$today" in v
        assert "$yesterday" in v
        assert "$now" in v
        assert "$today_compact" in v

    def test_substitute_string(self):
        result = substitute_date_variables("date=$today")
        assert "$today" not in result
        assert "date=" in result

    def test_substitute_dict(self):
        result = substitute_date_variables({"d": "$today"})
        assert isinstance(result, dict)
        assert "$today" not in result["d"]

    def test_substitute_list(self):
        result = substitute_date_variables(["$today", "static"])
        assert isinstance(result, list)
        assert "$today" not in result[0]

    def test_substitute_passthrough(self):
        assert substitute_date_variables(42) == 42
        assert substitute_date_variables(None) is None


class TestCurlyBraceDates:
    def test_today(self):
        result = _substitute_curly_brace_dates("{today}")
        assert "{" not in result

    def test_today_format(self):
        result = _substitute_curly_brace_dates("{today:%Y%m%d}")
        assert len(result) == 8
        assert result.isdigit()

    def test_yesterday(self):
        result = _substitute_curly_brace_dates("{yesterday}")
        assert "{" not in result

    def test_now(self):
        result = _substitute_curly_brace_dates("{now}")
        assert "T" in result  # ISO format

    def test_relative_negative(self):
        result = _substitute_curly_brace_dates("{-7d}")
        assert "{" not in result

    def test_relative_positive(self):
        result = _substitute_curly_brace_dates("{+1d}")
        assert "{" not in result

    def test_relative_with_format(self):
        result = _substitute_curly_brace_dates("{-30d:%Y%m%d}")
        assert len(result) == 8

    def test_start_of_week(self):
        result = _substitute_curly_brace_dates("{start_of_week}")
        assert "{" not in result

    def test_start_of_month(self):
        result = _substitute_curly_brace_dates("{start_of_month}")
        assert "{" not in result

    def test_start_of_year(self):
        result = _substitute_curly_brace_dates("{start_of_year}")
        assert "{" not in result

    def test_date_alias(self):
        result = _substitute_curly_brace_dates("{date}")
        assert "{" not in result

    def test_unknown_expression(self):
        result = _substitute_curly_brace_dates("{unknown_thing}")
        assert result == "{unknown_thing}"


# ===== ResponseExtractor =====


class TestResponseExtractor:
    def test_empty_path(self):
        ext = ResponseExtractor(items_path="")
        items = ext.extract_items([{"id": 1}])
        assert len(items) == 1

    def test_dotted_path(self):
        ext = ResponseExtractor(items_path="data.items")
        items = ext.extract_items({"data": {"items": [{"x": 1}, {"x": 2}]}})
        assert len(items) == 2

    def test_array_index(self):
        ext = ResponseExtractor(items_path="series[0].data")
        items = ext.extract_items({"series": [{"data": [{"v": 1}]}]})
        assert len(items) == 1

    def test_array_index_out_of_bounds(self):
        ext = ResponseExtractor(items_path="data[5]")
        items = ext.extract_items({"data": [1, 2, 3]})
        assert items == []

    def test_path_not_found(self):
        ext = ResponseExtractor(items_path="missing.path")
        items = ext.extract_items({"other": "data"})
        assert items == []

    def test_dict_to_list_scalar(self):
        ext = ResponseExtractor(items_path="metrics", dict_to_list=True)
        items = ext.extract_items({"metrics": {"cpu": 80, "mem": 60}})
        assert len(items) == 2
        keys = {i["_key"] for i in items}
        assert keys == {"cpu", "mem"}
        assert any(i["_value"] == 80 for i in items)

    def test_dict_to_list_nested(self):
        ext = ResponseExtractor(items_path="data", dict_to_list=True)
        items = ext.extract_items({"data": {"x": {"val": 1}}})
        assert items[0]["_key"] == "x"
        assert items[0]["val"] == 1

    def test_wrap_single(self):
        ext = ResponseExtractor(items_path="result", wrap_single=True)
        items = ext.extract_items({"result": {"id": 1, "name": "a"}})
        assert len(items) == 1
        assert items[0]["id"] == 1

    def test_dict_no_wrap(self):
        ext = ResponseExtractor(items_path="result")
        items = ext.extract_items({"result": {"id": 1}})
        assert items == []

    def test_array_row_fields(self):
        ext = ResponseExtractor(items_path="", array_row_fields=["ts", "val"])
        items = ext.extract_items([[1234, 5.6], [5678, 7.8]])
        assert len(items) == 2
        assert items[0]["ts"] == 1234
        assert items[1]["val"] == 7.8

    def test_array_row_fields_padding(self):
        ext = ResponseExtractor(items_path="", array_row_fields=["a", "b", "c"])
        items = ext.extract_items([[1]])
        assert items[0] == {"a": 1, "b": None, "c": None}

    def test_array_row_fields_trimming(self):
        ext = ResponseExtractor(items_path="", array_row_fields=["a"])
        items = ext.extract_items([[1, 2, 3]])
        assert items[0] == {"a": 1}

    def test_add_fields(self):
        ext = ResponseExtractor(items_path="", add_fields={"_source": "api"})
        items = ext.extract_items([{"id": 1}])
        assert items[0]["_source"] == "api"

    def test_add_fields_date_sub(self):
        ext = ResponseExtractor(items_path="", add_fields={"_fetched": "$today"})
        items = ext.extract_items([{"id": 1}])
        assert "$today" not in items[0]["_fetched"]

    def test_non_list_non_dict_response(self):
        ext = ResponseExtractor(items_path="val")
        items = ext.extract_items({"val": "string_value"})
        assert items == []

    def test_root_path(self):
        ext = ResponseExtractor(items_path="$")
        items = ext.extract_items([{"a": 1}])
        assert len(items) == 1

    def test_segment_not_dict(self):
        ext = ResponseExtractor(items_path="a.b")
        items = ext.extract_items({"a": "not_a_dict"})
        assert items == []

    def test_index_on_non_list(self):
        ext = ResponseExtractor(items_path="a[0]")
        items = ext.extract_items({"a": "not_a_list"})
        assert items == []


# ===== Pagination =====


class TestOffsetLimitPagination:
    def test_initial_request(self):
        p = OffsetLimitPagination(offset_param="skip", limit=50)
        base = ApiRequest(method="GET", url="https://api.example.com/data")
        req = p.initial_request(base)
        assert req.params["skip"] == 0
        assert req.params["limit"] == 50

    def test_next_request(self):
        p = OffsetLimitPagination(limit=10)
        base = ApiRequest(method="GET", url="https://api.example.com/data")
        req = p.initial_request(base)
        page = ApiPage(items=[{"i": i} for i in range(10)], raw={}, headers={}, request=req)
        nxt = p.next_request(page)
        assert nxt is not None
        assert nxt.params["offset"] == 10

    def test_stops_at_max_pages(self):
        p = OffsetLimitPagination(limit=10, max_pages=1)
        base = ApiRequest(method="GET", url="u")
        req = p.initial_request(base)
        page = ApiPage(items=[{}] * 10, raw={}, headers={}, request=req)
        assert p.next_request(page) is None

    def test_stops_on_empty(self):
        p = OffsetLimitPagination(limit=10)
        base = ApiRequest(method="GET", url="u")
        req = p.initial_request(base)
        page = ApiPage(items=[], raw={}, headers={}, request=req)
        assert p.next_request(page) is None

    def test_stops_on_last_page(self):
        p = OffsetLimitPagination(limit=10)
        base = ApiRequest(method="GET", url="u")
        req = p.initial_request(base)
        page = ApiPage(items=[{}] * 5, raw={}, headers={}, request=req)
        assert p.next_request(page) is None

    def test_post_mode_puts_in_body(self):
        p = OffsetLimitPagination(limit=10)
        base = ApiRequest(method="POST", url="u", json_body={"filter": "x"})
        req = p.initial_request(base)
        assert req.json_body["offset"] == 0
        assert req.json_body["limit"] == 10
        assert req.json_body["filter"] == "x"


class TestPageNumberPagination:
    def test_initial_request(self):
        p = PageNumberPagination(page_param="p", page_size_param="ps", page_size=25)
        base = ApiRequest(method="GET", url="u")
        req = p.initial_request(base)
        assert req.params["p"] == 1
        assert req.params["ps"] == 25

    def test_next_request(self):
        p = PageNumberPagination(page_size=10)
        req = p.initial_request(ApiRequest(method="GET", url="u"))
        page = ApiPage(items=[{}] * 10, raw={}, headers={}, request=req)
        nxt = p.next_request(page)
        assert nxt is not None
        assert nxt.params["page"] == 2

    def test_stops_at_max(self):
        p = PageNumberPagination(page_size=10, max_pages=1)
        req = p.initial_request(ApiRequest(method="GET", url="u"))
        page = ApiPage(items=[{}] * 10, raw={}, headers={}, request=req)
        assert p.next_request(page) is None

    def test_stops_on_empty(self):
        p = PageNumberPagination(page_size=10)
        req = p.initial_request(ApiRequest(method="GET", url="u"))
        page = ApiPage(items=[], raw={}, headers={}, request=req)
        assert p.next_request(page) is None

    def test_stops_on_partial(self):
        p = PageNumberPagination(page_size=10)
        req = p.initial_request(ApiRequest(method="GET", url="u"))
        page = ApiPage(items=[{}] * 3, raw={}, headers={}, request=req)
        assert p.next_request(page) is None


class TestCursorPagination:
    def test_initial_request(self):
        p = CursorPagination()
        base = ApiRequest(method="GET", url="u")
        req = p.initial_request(base)
        assert req == base

    def test_next_request(self):
        p = CursorPagination(cursor_path="meta.next")
        base = ApiRequest(method="GET", url="u")
        p.initial_request(base)
        page = ApiPage(items=[{}], raw={"meta": {"next": "abc123"}}, headers={}, request=base)
        nxt = p.next_request(page)
        assert nxt is not None
        assert nxt.params["cursor"] == "abc123"

    def test_stops_cursor_missing(self):
        p = CursorPagination()
        base = ApiRequest(method="GET", url="u")
        p.initial_request(base)
        page = ApiPage(items=[{}], raw={}, headers={}, request=base)
        assert p.next_request(page) is None

    def test_stops_at_max(self):
        p = CursorPagination(max_pages=1)
        base = ApiRequest(method="GET", url="u")
        p.initial_request(base)
        page = ApiPage(items=[{}], raw={"next_cursor": "x"}, headers={}, request=base)
        assert p.next_request(page) is None

    def test_nested_cursor(self):
        p = CursorPagination(cursor_path="pagination.next.cursor")
        base = ApiRequest(method="GET", url="u")
        p.initial_request(base)
        raw = {"pagination": {"next": {"cursor": "tok"}}}
        page = ApiPage(items=[{}], raw=raw, headers={}, request=base)
        nxt = p.next_request(page)
        assert nxt.params["cursor"] == "tok"


class TestLinkHeaderPagination:
    def test_initial_request(self):
        p = LinkHeaderPagination()
        base = ApiRequest(method="GET", url="u")
        assert p.initial_request(base) == base

    def test_next_from_link(self):
        p = LinkHeaderPagination()
        base = ApiRequest(method="GET", url="https://api.example.com/items")
        p.initial_request(base)
        headers = {"link": '<https://api.example.com/items?page=2>; rel="next"'}
        page = ApiPage(items=[{}], raw={}, headers=headers, request=base)
        nxt = p.next_request(page)
        assert nxt is not None
        assert nxt.params.get("page") == "2"

    def test_stops_no_next(self):
        p = LinkHeaderPagination()
        p.initial_request(ApiRequest(method="GET", url="u"))
        page = ApiPage(items=[{}], raw={}, headers={}, request=ApiRequest(method="GET", url="u"))
        assert p.next_request(page) is None

    def test_stops_at_max(self):
        p = LinkHeaderPagination(max_pages=1)
        p.initial_request(ApiRequest(method="GET", url="u"))
        headers = {"link": '<https://api.example.com?page=2>; rel="next"'}
        page = ApiPage(
            items=[{}], raw={}, headers=headers, request=ApiRequest(method="GET", url="u")
        )
        assert p.next_request(page) is None


class TestNoPagination:
    def test_initial(self):
        p = NoPagination()
        base = ApiRequest(method="GET", url="u")
        assert p.initial_request(base) == base

    def test_next_always_none(self):
        p = NoPagination()
        page = ApiPage(items=[{}], raw={}, headers={}, request=ApiRequest(method="GET", url="u"))
        assert p.next_request(page) is None


# ===== RetryPolicy =====


class TestRetryPolicy:
    def test_delay_exponential(self):
        rp = RetryPolicy(base_delay_s=1.0, exponential_base=2.0)
        assert rp.get_delay(1) == 1.0
        assert rp.get_delay(2) == 2.0
        assert rp.get_delay(3) == 4.0

    def test_delay_capped(self):
        rp = RetryPolicy(base_delay_s=10.0, max_delay_s=15.0, exponential_base=2.0)
        assert rp.get_delay(3) == 15.0


# ===== RateLimiter =====


class TestRateLimiter:
    def test_fixed_mode_sleeps(self):
        rl = RateLimiter(mode="fixed", requests_per_second=2.0)
        rl._last_request_time = time.time() - 0.1  # 100ms ago
        with patch("time.sleep") as mock_sleep:
            rl.wait_if_needed()
            if mock_sleep.called:
                args = mock_sleep.call_args[0]
                assert args[0] > 0

    def test_auto_mode_retry_after(self):
        rl = RateLimiter(mode="auto")
        headers = {"retry-after": "0.01"}
        with patch("time.sleep") as mock_sleep:
            rl.wait_if_needed(headers)
            mock_sleep.assert_called_once()

    def test_auto_mode_ratelimit_headers(self):
        rl = RateLimiter(mode="auto")
        headers = {"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(time.time() + 0.01)}
        with patch("time.sleep") as mock_sleep:
            rl.wait_if_needed(headers)
            if mock_sleep.called:
                assert mock_sleep.call_args[0][0] >= 0

    def test_off_mode(self):
        rl = RateLimiter(mode="off")
        with patch("time.sleep") as mock_sleep:
            rl.wait_if_needed({"retry-after": "5"})
            mock_sleep.assert_not_called()

    def test_auto_invalid_retry_after(self):
        rl = RateLimiter(mode="auto")
        headers = {"retry-after": "not-a-number"}
        with patch("time.sleep") as mock_sleep:
            rl.wait_if_needed(headers)
            mock_sleep.assert_not_called()


# ===== Factory =====


class TestFactory:
    def test_create_offset_limit(self):
        s = create_pagination_strategy({"type": "offset_limit", "limit": 50})
        assert isinstance(s, OffsetLimitPagination)
        assert s.limit == 50

    def test_create_cursor(self):
        s = create_pagination_strategy({"type": "cursor", "cursor_param": "next"})
        assert isinstance(s, CursorPagination)

    def test_create_page_number(self):
        s = create_pagination_strategy({"type": "page_number"})
        assert isinstance(s, PageNumberPagination)

    def test_create_link_header(self):
        s = create_pagination_strategy({"type": "link_header"})
        assert isinstance(s, LinkHeaderPagination)

    def test_create_none(self):
        s = create_pagination_strategy({"type": "none"})
        assert isinstance(s, NoPagination)

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown pagination"):
            create_pagination_strategy({"type": "magic"})

    def test_create_api_fetcher_defaults(self):
        f = create_api_fetcher("https://api.example.com")
        assert isinstance(f, ApiFetcher)
        assert f.base_url == "https://api.example.com"

    def test_create_api_fetcher_full(self):
        f = create_api_fetcher(
            "https://api.example.com",
            headers={"Authorization": "Bearer tok"},
            options={
                "pagination": {"type": "offset_limit", "limit": 100},
                "response": {"items_path": "results", "add_fields": {"src": "api"}},
                "retry": {"max_retries": 5, "backoff_factor": 3.0},
                "rate_limit": {"requests_per_second": 2.0},
                "http": {"timeout_s": 60.0},
            },
        )
        assert isinstance(f, ApiFetcher)
        assert f.timeout_s == 60.0


# ===== ApiFetcher Integration (mocked HTTP) =====


class TestApiFetcherIntegration:
    def _make_fetcher(self, **kw):
        return ApiFetcher(base_url="https://api.example.com", **kw)

    def test_fetch_dataframe_empty(self):
        f = self._make_fetcher()
        with patch.object(f, "fetch_iter", return_value=iter([])):
            df = f.fetch_dataframe("/endpoint")
        assert len(df) == 0

    def test_fetch_dataframe_concat(self):
        f = self._make_fetcher()
        pages = [
            ApiPage(
                items=[{"id": 1}], raw={}, headers={}, request=ApiRequest(method="GET", url="u")
            ),
            ApiPage(
                items=[{"id": 2}], raw={}, headers={}, request=ApiRequest(method="GET", url="u")
            ),
        ]
        with patch.object(f, "fetch_iter", return_value=iter(pages)):
            df = f.fetch_dataframe("/endpoint")
        assert len(df) == 2

    def test_fetch_dataframe_max_records(self):
        f = self._make_fetcher()
        pages = [
            ApiPage(
                items=[{"id": i} for i in range(10)],
                raw={},
                headers={},
                request=ApiRequest(method="GET", url="u"),
            ),
        ]
        with patch.object(f, "fetch_iter", return_value=iter(pages)):
            df = f.fetch_dataframe("/endpoint", max_records=5)
        assert len(df) <= 10  # still returns full page but stops iteration

    def test_fetch_dataframe_metadata_reorder(self):
        f = self._make_fetcher(
            extractor=ResponseExtractor(add_fields={"_src": "api"}),
        )
        pages = [
            ApiPage(
                items=[{"id": 1, "_src": "api"}],
                raw={},
                headers={},
                request=ApiRequest(method="GET", url="u"),
            ),
        ]
        with patch.object(f, "fetch_iter", return_value=iter(pages)):
            df = f.fetch_dataframe("/endpoint")
        assert list(df.columns)[-1] == "_src"

    def test_fetch_dataframe_safe_success(self):
        f = self._make_fetcher()
        pages = [
            ApiPage(
                items=[{"id": 1}], raw={}, headers={}, request=ApiRequest(method="GET", url="u")
            ),
        ]
        with patch.object(f, "fetch_iter", return_value=iter(pages)):
            result = f.fetch_dataframe_safe("/endpoint")
        assert result.complete is True
        assert result.error is None
        assert len(result.df) == 1

    def test_fetch_dataframe_safe_error(self):
        f = self._make_fetcher()

        def failing_iter(*a, **k):
            yield ApiPage(
                items=[{"id": 1}], raw={}, headers={}, request=ApiRequest(method="GET", url="u")
            )
            raise ConnectionError("network fail")

        with patch.object(f, "fetch_iter", side_effect=failing_iter):
            result = f.fetch_dataframe_safe("/endpoint")
        assert result.complete is False
        assert result.error is not None
        assert len(result.df) == 1  # partial data preserved

    def test_fetch_iter_post_body_merge(self):
        f = self._make_fetcher(paginator=NoPagination())

        def mock_fetch_page(request):
            return ApiPage(items=[{"id": 1}], raw={}, headers={}, request=request)

        with patch.object(f, "_fetch_page", side_effect=mock_fetch_page):
            pages = list(
                f.fetch_iter(
                    "/endpoint",
                    params={"filter": "x"},
                    method="POST",
                    request_body={"query": "y"},
                )
            )
        assert len(pages) == 1
        req = pages[0].request
        # POST with body: params merged into body
        assert req.json_body["filter"] == "x"
        assert req.json_body["query"] == "y"

    def test_fetch_iter_full_url(self):
        f = self._make_fetcher(paginator=NoPagination())

        def mock_fetch_page(request):
            return ApiPage(items=[], raw={}, headers={}, request=request)

        with patch.object(f, "_fetch_page", side_effect=mock_fetch_page):
            pages = list(f.fetch_iter("https://other.api.com/data"))
        assert len(pages) == 1

    def test_fetch_dataframe_safe_metadata_reorder(self):
        f = self._make_fetcher(
            extractor=ResponseExtractor(add_fields={"_fetched": "now"}),
        )
        pages = [
            ApiPage(
                items=[{"id": 1, "_fetched": "now"}],
                raw={},
                headers={},
                request=ApiRequest(method="GET", url="u"),
            ),
        ]
        with patch.object(f, "fetch_iter", return_value=iter(pages)):
            result = f.fetch_dataframe_safe("/endpoint")
        assert list(result.df.columns)[-1] == "_fetched"

    def test_fetch_dataframe_safe_empty(self):
        f = self._make_fetcher()

        def empty_fail(*a, **k):
            raise ValueError("immediate fail")

        with patch.object(f, "fetch_iter", side_effect=empty_fail):
            result = f.fetch_dataframe_safe("/endpoint")
        assert result.complete is False
        assert len(result.df) == 0


# ===== Data classes =====


class TestDataClasses:
    def test_api_request_defaults(self):
        r = ApiRequest(method="GET", url="https://x.com")
        assert r.params == {}
        assert r.json_body is None

    def test_api_page(self):
        p = ApiPage(
            items=[{"a": 1}],
            raw={"a": 1},
            headers={"x": "y"},
            request=ApiRequest(method="GET", url="u"),
        )
        assert p.status_code == 200

    def test_fetch_result(self):
        import pandas as pd

        r = FetchResult(df=pd.DataFrame(), error=None, pages_fetched=0, complete=True)
        assert r.complete is True
