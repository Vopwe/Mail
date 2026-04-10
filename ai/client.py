"""
AI client — DeepSeek (primary) + OpenRouter (fallback).
Uses the OpenAI SDK since both APIs are OpenAI-compatible.
"""
import re
import time
import logging
from openai import OpenAI
import tldextract
import config
from ai.prompts import SYSTEM_PROMPT, build_user_prompt

logger = logging.getLogger(__name__)

# URL regex for parsing AI responses
URL_REGEX = re.compile(r"https?://[^\s<>\"'`,\)]+")


def _get_deepseek_client() -> OpenAI:
    return OpenAI(
        api_key=config.get_setting("deepseek_api_key", config.DEEPSEEK_API_KEY),
        base_url=config.DEEPSEEK_BASE_URL,
    )


def _get_openrouter_client() -> OpenAI:
    return OpenAI(
        api_key=config.get_setting("openrouter_api_key", config.OPENROUTER_API_KEY),
        base_url=config.OPENROUTER_BASE_URL,
    )


def _call_model(client: OpenAI, model: str, messages: list, retries: int = 2) -> str | None:
    for attempt in range(retries + 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7,
                max_tokens=2000,
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.warning(f"AI call attempt {attempt + 1} failed: {e}")
            if attempt < retries:
                time.sleep(1 * (attempt + 1))
    return None


def _parse_urls(text: str) -> list[str]:
    """Extract and deduplicate valid URLs from AI response text."""
    if not text:
        return []

    raw_urls = URL_REGEX.findall(text)
    seen_domains = set()
    valid_urls = []

    for url in raw_urls:
        url = url.rstrip(".,;:)")
        ext = tldextract.extract(url)
        if not ext.domain or not ext.suffix:
            continue
        registered = f"{ext.domain}.{ext.suffix}"

        # Skip social media and aggregators
        skip = {"facebook.com", "instagram.com", "linkedin.com", "twitter.com",
                "x.com", "youtube.com", "tiktok.com", "yelp.com", "yellowpages.com",
                "google.com", "bing.com", "wikipedia.org"}
        if registered in skip:
            continue

        if registered not in seen_domains:
            seen_domains.add(registered)
            # Normalize to just the base domain
            if not url.startswith("http"):
                url = "https://" + url
            valid_urls.append(url)

    return valid_urls


def generate_urls(niche: str, city: str, country: str, country_tld: str = ".com", count: int = 20) -> list[str]:
    """Generate URLs using DeepSeek (primary) with OpenRouter fallback."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_prompt(niche, city, country, country_tld, count)},
    ]

    # Try DeepSeek first
    deepseek_key = config.get_setting("deepseek_api_key", config.DEEPSEEK_API_KEY)
    if deepseek_key and deepseek_key != "YOUR_DEEPSEEK_KEY_HERE":
        logger.info(f"Calling DeepSeek for {niche} in {city}, {country}")
        client = _get_deepseek_client()
        model = config.get_setting("deepseek_model", config.DEEPSEEK_MODEL)
        text = _call_model(client, model, messages)
        if text:
            urls = _parse_urls(text)
            if urls:
                logger.info(f"DeepSeek returned {len(urls)} URLs")
                return urls

    # Fallback to OpenRouter
    openrouter_key = config.get_setting("openrouter_api_key", config.OPENROUTER_API_KEY)
    if openrouter_key and openrouter_key != "YOUR_OPENROUTER_KEY_HERE":
        logger.info(f"Falling back to OpenRouter for {niche} in {city}, {country}")
        client = _get_openrouter_client()
        model = config.get_setting("openrouter_model", config.OPENROUTER_MODEL)
        text = _call_model(client, model, messages)
        if text:
            urls = _parse_urls(text)
            if urls:
                logger.info(f"OpenRouter returned {len(urls)} URLs")
                return urls

    logger.error(f"Both AI providers failed for {niche} in {city}, {country}")
    return []
