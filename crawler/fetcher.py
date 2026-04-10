"""
Async HTTP fetcher — crawls domains and their sub-pages.
Truly parallel with asyncio.gather + semaphore throttling.
Respects robots.txt and discovers sub-pages from homepage links.
"""
import asyncio
import logging
import re
from urllib.parse import urljoin, urlparse
import httpx
from fake_useragent import UserAgent
import config

logger = logging.getLogger(__name__)
ua = UserAgent(fallback="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

# Patterns that indicate contact/about/team pages in any language
_DISCOVERY_PATTERNS = re.compile(
    r'(?i)(contact|about|team|staff|people|support|impressum|legal|'
    r'kontakt|equipe|equipo|sobre|chi-siamo|ueber-uns|über-uns|'
    r'qui-sommes-nous|notre-equipe|nuestro-equipo|our-team|meet-the-team|'
    r'get-in-touch|reach-us|write-to-us|email-us)',
)


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


async def _fetch_robots_txt(client: httpx.AsyncClient, base_url: str) -> set[str]:
    """Fetch robots.txt and return set of disallowed paths for * user-agent."""
    disallowed = set()
    try:
        resp = await client.get(base_url + "/robots.txt", follow_redirects=True)
        if resp.status_code != 200:
            return disallowed
        applies = False
        for line in resp.text.splitlines():
            line = line.strip()
            if line.lower().startswith("user-agent:"):
                agent = line.split(":", 1)[1].strip()
                applies = agent == "*"
            elif applies and line.lower().startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    disallowed.add(path)
    except Exception:
        pass
    return disallowed


def _is_path_allowed(path: str, disallowed: set[str]) -> bool:
    """Check if a path is allowed by robots.txt rules."""
    for rule in disallowed:
        if rule.endswith("*"):
            if path.startswith(rule[:-1]):
                return False
        elif path == rule or path.startswith(rule.rstrip("/") + "/"):
            return False
    return True


def _discover_sub_pages(html: str, base_url: str, domain: str) -> list[str]:
    """Parse homepage HTML to find links to contact/about/team pages."""
    discovered = []
    seen = set()
    # Simple regex to find href attributes — avoids full HTML parse overhead
    for match in re.finditer(r'href=["\']([^"\']+)["\']', html):
        href = match.group(1)
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        # Only same-domain, HTTP(S) links
        if parsed.hostname and domain not in parsed.hostname:
            continue
        if parsed.scheme not in ("http", "https", ""):
            continue
        path = parsed.path.rstrip("/").lower()
        if not path or path == "/" or path in seen:
            continue
        seen.add(path)
        if _DISCOVERY_PATTERNS.search(path):
            discovered.append(full_url)
    return discovered


async def fetch_domain_pages(base_url: str, semaphore: asyncio.Semaphore) -> list[tuple[str, str]]:
    """
    Fetch a domain's main page + discovered sub-pages.
    Respects robots.txt. Discovers relevant links from homepage.
    Returns list of (url, html) for successful pages.
    """
    results = []
    base_url = base_url.rstrip("/")
    timeout = float(config.get_setting("request_timeout", config.REQUEST_TIMEOUT))
    delay = float(config.get_setting("crawl_delay", config.CRAWL_DELAY))
    max_pages = int(config.get_setting("max_pages_per_domain", config.MAX_PAGES_PER_DOMAIN))

    parsed = urlparse(base_url)
    domain = parsed.hostname or ""

    async with semaphore:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers={"User-Agent": ua.random},
            http2=True,
            verify=False,
        ) as client:
            # 1. Check robots.txt
            disallowed = await _fetch_robots_txt(client, base_url)

            # 2. Fetch homepage first
            visited = set()
            homepage_url, homepage_html, _ = await fetch_page(client, base_url)
            visited.add("/")
            if homepage_html:
                results.append((homepage_url, homepage_html))
            await asyncio.sleep(delay)

            # 3. Build sub-page list: common paths + discovered from homepage
            sub_urls = []
            # Add common paths (skip "/" since we already fetched homepage)
            for path in config.COMMON_PATHS[1:]:
                full = base_url + path
                sub_urls.append((path, full))

            # Add discovered pages from homepage links
            if homepage_html:
                for discovered_url in _discover_sub_pages(homepage_html, base_url, domain):
                    path = urlparse(discovered_url).path
                    if path not in visited:
                        sub_urls.append((path, discovered_url))

            # 4. Crawl sub-pages up to max_pages limit, respecting robots.txt
            for path, url in sub_urls:
                if len(results) >= max_pages:
                    break
                norm_path = path.rstrip("/") or "/"
                if norm_path in visited:
                    continue
                visited.add(norm_path)

                if not _is_path_allowed(path, disallowed):
                    logger.debug(f"Blocked by robots.txt: {url}")
                    continue

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
