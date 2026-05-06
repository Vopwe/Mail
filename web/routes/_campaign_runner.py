"""
Campaign execution logic — runs in background thread.
Bing+DDG+AI URL generation (parallel combos) + async crawling + email extraction.
"""
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import tldextract
import database
import config
import tasks
from search.scraper import generate_urls
from crawler.fetcher import crawl_urls
from crawler.extractor import extract_emails

logger = logging.getLogger(__name__)


def _generate_for_combo(combo, urls_per_batch, source_mode):
    """Worker: generate URLs for a single (niche, city, country, tld) combo via Bing+DDG+AI."""
    niche, city, country, country_tld = combo
    try:
        tagged_urls = generate_urls(
            niche, city, country, country_tld,
            count=urls_per_batch,
            source_mode=source_mode,
        )
        rows = []
        for url, source in tagged_urls:
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            rows.append({
                "url": url,
                "domain": domain,
                "niche": niche,
                "city": city,
                "country": country,
                "source": source,
            })
        return rows
    except Exception as e:
        logger.error(f"URL generation failed for {niche}/{city}/{country}: {e}")
        return []


async def run_campaign(task_id: str, campaign_id: int):
    """Full campaign pipeline: generate URLs → crawl → extract emails."""
    campaign = database.get_campaign(campaign_id)
    if not campaign:
        tasks.fail_task(task_id, "Campaign not found")
        return

    try:
        await _run_campaign_steps(task_id, campaign_id, campaign)
    except Exception:
        logger.exception("Campaign %s failed", campaign_id)
        database.update_campaign_status(campaign_id, "failed")
        database.update_campaign_counts(campaign_id)
        raise


async def _run_campaign_steps(task_id: str, campaign_id: int, campaign: dict):
    locations = config.get_locations()
    niches = campaign["niches"]
    countries = campaign["countries"]
    cities = campaign["cities"]
    urls_per_batch = int(config.get_setting("urls_per_batch", config.URLS_PER_BATCH))
    source_mode = config.get_setting("url_source_mode", config.URL_SOURCE_MODE)

    database.update_campaign_status(campaign_id, "generating")
    tasks.update_task(task_id, message="Generating URLs via Bing + DDG + AI...")

    combos = []
    for country in countries:
        country_data = locations.get(country, {})
        country_tld = country_data.get("tld", ".com")
        country_cities = country_data.get("cities", [])

        if "*" in cities:
            target_cities = country_cities[:20]
        else:
            target_cities = [c for c in cities if c in country_cities]
            if not target_cities:
                target_cities = cities

        for niche in niches:
            for city in target_cities:
                combos.append((niche, city, country, country_tld))

    total_combos = len(combos)
    tasks.update_task(
        task_id,
        total=total_combos,
        message=f"Generating URLs for {total_combos} combinations (Bing+DDG+AI, mode={source_mode})...",
    )

    # Run combos in parallel — each combo runs Bing+DDG internally
    all_url_rows = []
    source_counts = {"bing": 0, "ddg": 0, "ai": 0}
    completed = 0
    lock = threading.Lock()

    # Limit parallel combos to avoid hammering search engines
    max_parallel = min(3, total_combos)

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(_generate_for_combo, combo, urls_per_batch, source_mode): combo
            for combo in combos
        }
        for future in as_completed(futures):
            combo = futures[future]
            rows = future.result()
            for row in rows:
                row["campaign_id"] = campaign_id

            with lock:
                for row in rows:
                    source = row.get("source", "unknown")
                    if source in source_counts:
                        source_counts[source] += 1
                all_url_rows.extend(rows)
                completed += 1
                niche, city, country, _ = combo
                tasks.update_task(
                    task_id,
                    progress=completed,
                    message=f"[{completed}/{total_combos}] Generated: {niche} in {city}, {country} "
                            f"(Bing:{source_counts['bing']} DDG:{source_counts['ddg']} AI:{source_counts['ai']})",
                )

    # Cross-campaign domain dedup: remove URLs already crawled elsewhere
    deduped_count = 0
    if all_url_rows:
        existing_domains = database.get_existing_domains(exclude_campaign_id=campaign_id)
        before = len(all_url_rows)
        all_url_rows = [r for r in all_url_rows if r["domain"] not in existing_domains]
        deduped_count = before - len(all_url_rows)
        if deduped_count:
            logger.info(f"Cross-campaign dedup: removed {deduped_count} duplicate domains")
            tasks.update_task(task_id, message=f"Removed {deduped_count} duplicate domains from other campaigns")

    if all_url_rows:
        database.insert_urls(all_url_rows)
    database.update_campaign_counts(campaign_id)

    database.update_campaign_status(campaign_id, "crawling")
    pending_urls = database.get_urls(campaign_id, status="pending")
    tasks.update_task(
        task_id,
        progress=0,
        total=len(pending_urls),
        message=f"Crawling {len(pending_urls)} URLs...",
    )

    def on_crawl_progress(done, total):
        tasks.update_task(task_id, progress=done, total=total,
                          message=f"Crawled {done}/{total} domains")

    crawl_results, crawl_stats = await crawl_urls(pending_urls, on_progress=on_crawl_progress)

    tasks.update_task(task_id, message="Extracting emails...")
    total_extracted = 0
    domains_with_emails = 0
    domains_without_emails = 0

    for url_record in pending_urls:
        url_id = url_record["id"]
        pages = crawl_results.get(url_id, [])

        if pages:
            database.update_url_status(url_id, "crawled")
            email_rows = []
            for page_url, html in pages:
                extracted = extract_emails(html, page_url)
                for em in extracted:
                    em["campaign_id"] = campaign_id
                    em["niche"] = url_record["niche"]
                    em["city"] = url_record["city"]
                    em["country"] = url_record["country"]
                    email_rows.append(em)

            if email_rows:
                database.insert_emails_bulk(email_rows)
                total_extracted += len(email_rows)
                domains_with_emails += 1
            else:
                domains_without_emails += 1
        else:
            database.update_url_status(url_id, "failed", error="No pages fetched")
            domains_without_emails += 1

    # Save crawl stats to campaign
    crawl_stats["domains_with_emails"] = domains_with_emails
    crawl_stats["domains_without_emails"] = domains_without_emails
    crawl_stats["total_emails_extracted"] = total_extracted
    crawl_stats["url_sources"] = source_counts
    if crawl_stats["domains_reachable"] > 0:
        crawl_stats["emails_per_domain"] = round(total_extracted / crawl_stats["domains_reachable"], 2)
    else:
        crawl_stats["emails_per_domain"] = 0
    crawl_stats["deduped_domains"] = deduped_count

    database.save_campaign_stats(campaign_id, crawl_stats)
    database.update_campaign_counts(campaign_id)
    database.update_campaign_status(campaign_id, "done")

    # Build detailed completion message
    msg = (
        f"Done! {total_extracted} emails from {len(pending_urls)} URLs. "
        f"Sources: Bing={source_counts['bing']} DDG={source_counts['ddg']} AI={source_counts['ai']} | "
        f"Reachable: {crawl_stats['domains_reachable']}/{crawl_stats['domains_total']} | "
        f"Pages: {crawl_stats['pages_fetched']} fetched, {crawl_stats['pages_failed']} failed | "
        f"Domains with emails: {domains_with_emails}"
    )
    if crawl_stats['pages_robots_blocked'] > 0:
        msg += f" | robots.txt blocked: {crawl_stats['pages_robots_blocked']}"

    tasks.complete_task(task_id, msg)
