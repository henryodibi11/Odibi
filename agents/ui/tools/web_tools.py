"""Web search and browsing tools.

Search the web and read documentation.
"""

import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote_plus

import requests


@dataclass
class WebSearchResult:
    """Result of a web search."""

    success: bool
    query: str
    results: list[dict] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class WebPageResult:
    """Result of reading a web page."""

    success: bool
    url: str
    title: str = ""
    content: str = ""
    error: Optional[str] = None


def web_search(
    query: str,
    max_results: int = 5,
    timeout: int = 10,
) -> WebSearchResult:
    """Search the web using DuckDuckGo.

    Args:
        query: Search query.
        max_results: Maximum results to return.
        timeout: Request timeout in seconds.

    Returns:
        WebSearchResult with search results.
    """
    try:
        from duckduckgo_search import DDGS

        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))

        formatted_results = [
            {
                "title": r.get("title", ""),
                "url": r.get("href", r.get("link", "")),
                "snippet": r.get("body", r.get("snippet", "")),
            }
            for r in results
        ]

        return WebSearchResult(
            success=True,
            query=query,
            results=formatted_results,
        )

    except ImportError:
        return _web_search_fallback(query, max_results, timeout)

    except Exception as e:
        return WebSearchResult(
            success=False,
            query=query,
            error=str(e),
        )


def _web_search_fallback(
    query: str,
    max_results: int = 5,
    timeout: int = 10,
) -> WebSearchResult:
    """Fallback web search using DuckDuckGo HTML.

    Used when duckduckgo_search package is not available.
    """
    try:
        url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }

        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()

        results = []
        result_pattern = re.compile(
            r'<a rel="nofollow" class="result__a" href="([^"]+)"[^>]*>([^<]+)</a>',
            re.IGNORECASE,
        )
        snippet_pattern = re.compile(
            r'<a class="result__snippet"[^>]*>([^<]+)</a>',
            re.IGNORECASE,
        )

        matches = result_pattern.findall(response.text)
        snippets = snippet_pattern.findall(response.text)

        for i, (url, title) in enumerate(matches[:max_results]):
            snippet = snippets[i] if i < len(snippets) else ""
            results.append(
                {
                    "title": title.strip(),
                    "url": url,
                    "snippet": snippet.strip(),
                }
            )

        if not results:
            return WebSearchResult(
                success=True,
                query=query,
                results=[],
                error="No results found. Try installing: pip install duckduckgo-search",
            )

        return WebSearchResult(
            success=True,
            query=query,
            results=results,
        )

    except Exception as e:
        return WebSearchResult(
            success=False,
            query=query,
            error=f"Search failed: {e}. Try installing: pip install duckduckgo-search",
        )


def read_web_page(
    url: str,
    timeout: int = 15,
    max_length: int = 10000,
) -> WebPageResult:
    """Read and extract content from a web page.

    Args:
        url: URL to read.
        timeout: Request timeout in seconds.
        max_length: Maximum content length to return.

    Returns:
        WebPageResult with page content.
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }

        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()

        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(response.text, "html.parser")

            for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()

            title = soup.title.string if soup.title else ""

            main_content = soup.find("main") or soup.find("article") or soup.body
            if main_content:
                text = main_content.get_text(separator="\n", strip=True)
            else:
                text = soup.get_text(separator="\n", strip=True)

            lines = [line.strip() for line in text.splitlines() if line.strip()]
            content = "\n".join(lines)[:max_length]

            return WebPageResult(
                success=True,
                url=url,
                title=title or "",
                content=content,
            )

        except ImportError:
            text = re.sub(r"<[^>]+>", " ", response.text)
            text = re.sub(r"\s+", " ", text).strip()[:max_length]

            title_match = re.search(r"<title>([^<]+)</title>", response.text, re.I)
            title = title_match.group(1) if title_match else ""

            return WebPageResult(
                success=True,
                url=url,
                title=title,
                content=text,
            )

    except requests.exceptions.Timeout:
        return WebPageResult(
            success=False,
            url=url,
            error=f"Request timed out after {timeout}s",
        )

    except requests.exceptions.RequestException as e:
        return WebPageResult(
            success=False,
            url=url,
            error=str(e),
        )


def format_search_results(result: WebSearchResult) -> str:
    """Format search results for display.

    Args:
        result: WebSearchResult to format.

    Returns:
        Markdown formatted results.
    """
    if not result.success:
        return f"❌ **Search failed:** {result.error}"

    if not result.results:
        msg = f"No results found for: *{result.query}*"
        if result.error:
            msg += f"\n\n{result.error}"
        return msg

    lines = [f"🔍 **Search:** *{result.query}*\n"]

    for i, r in enumerate(result.results, 1):
        title = r.get("title", "Untitled")
        url = r.get("url", "")
        snippet = r.get("snippet", "")[:200]

        lines.append(f"**{i}. [{title}]({url})**")
        if snippet:
            lines.append(f"> {snippet}")
        lines.append("")

    return "\n".join(lines)


def format_web_page(result: WebPageResult) -> str:
    """Format web page content for display.

    Args:
        result: WebPageResult to format.

    Returns:
        Markdown formatted content.
    """
    if not result.success:
        return f"❌ **Failed to read page:** {result.error}"

    title = result.title or result.url
    content_preview = result.content[:2000]
    if len(result.content) > 2000:
        content_preview += "\n\n*[Content truncated...]*"

    return f"""📄 **{title}**
*Source: {result.url}*

---

{content_preview}
"""
