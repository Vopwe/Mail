"""
Async HTTP fetcher — crawls domains and their sub-pages.
Truly parallel with asyncio.gather + semaphore throttling.
"""
import asyncio
import logging
import httpx
from fake_useragent import UserAgent
import config

logger = logging.getLogger(__name__)
ua = UserAgent(fallback="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")


async def fetch_page(client: httpx.AsyncClient, url: str) -> tuple[str, str | None, int | None]:
    """Fetch a single page. Returns (url, html_or_none, status_code_or_none)."""
    try:
        resp = await client.get(url, follow_redirects=True)
        content_type = resp.headers.get("content-type", "")
        if resp.status_code == 200 and "text/html" in content_type:
            return url, resp.text, resp.status_code
        return url, None, resp.status_code
    except (httpx.HTTPError, httpx.TimeoutException, Exception) as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return url, None, None


async def fetch_domain_pages(base_url: str, semaphore: asyncio.Semaphore) -> list[tuple[str, str]]:
    """
    Fetch a domain's main page + common sub-pages.
    Returns list of (url, html) for successful pages.
    """
    results = []
    base_url = base_url.rstrip("/")
    timeout = float(config.get_setting("request_timeout", config.REQUEST_TIMEOUT))
    delay = float(config.get_setting("crawl_delay", config.CRAWL_DELAY))
    max_pages = int(config.get_setting("max_pages_per_domain", config.MAX_PAGES_PER_DOMAIN))

    async with semaphore:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers={"User-Agent": ua.random},
            http2=True,
            verify=False,
        ) as client:
            paths = config.COMMON_PATHS[:max_pages]
            for path in paths:
                url = base_url + path if path != "/" else base_url
                fetched_url, html, status = await fetch_page(client, url)
                if html:
                    results.append((fetched_url, html))
                await asyncio.sleep(delay)

    return results


async def _crawl_single(url_record: dict, semaphore: asyncio.Semaphore,
                         results: dict, counter: dict, total: int,
                         lock: asyncio.Lock, on_progress) -> None:
    """Crawl a single URL record and store results."""
    url_id = url_record["id"]
    base_url = url_record["url"]
    try:
        pages = await fetch_domain_pages(base_url, semaphore)
        results[url_id] = pages
    except Exception as e:
        logger.error(f"Error crawling {base_url}: {e}")
        results[url_id] = []

    async with lock:
        counter["done"] += 1
        if on_progress:
            on_progress(counter["done"], total)


async def crawl_urls(urls: list[dict], on_progress=None) -> dict[int, list[tuple[str, str]]]:
    """
    Crawl all URL records in PARALLEL using asyncio.gather.
    Semaphore controls max concurrent connections.
    Returns {url_id: [(page_url, html), ...]}.
    """
    max_conns = int(config.get_setting("max_concurrent_requests", config.MAX_CONCURRENT_REQUESTS))
    semaphore = asyncio.Semaphore(max_conns)
    results = {}
    counter = {"done": 0}
    total = len(urls)
    lock = asyncio.Lock()

    crawl_tasks = [
        _crawl_single(url_record, semaphore, results, counter, total, lock, on_progress)
        for url_record in urls
    ]

    await asyncio.gather(*crawl_tasks)
    return results
