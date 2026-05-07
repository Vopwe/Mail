"""
AI URL generator — DeepSeek (primary) + OpenRouter (fallback).
Uses OpenAI SDK for DeepSeek (OpenAI-compatible API).
Falls back to OpenRouter httpx calls if DeepSeek not configured or fails.
"""
import json
import logging
import re
import time

import httpx

import config

logger = logging.getLogger(__name__)

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# Current preferred free models for OpenRouter fallback.
FREE_MODELS = [
    "openrouter/free",
    "google/gemma-4-31b-it:free",
    "openai/gpt-oss-120b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "openai/gpt-oss-20b:free",
]


# ── Key helpers ────────────────────────────────────────────────────────

def _get_deepseek_key() -> str | None:
    """Get DeepSeek API key from settings."""
    key = config.get_setting("deepseek_api_key", config.DEEPSEEK_API_KEY)
    if key and key != "YOUR_DEEPSEEK_KEY_HERE":
        return key
    return None


def _get_openrouter_key() -> str | None:
    """Get OpenRouter API key from settings."""
    key = config.get_setting("openrouter_api_key", config.OPENROUTER_API_KEY)
    if key and key != "YOUR_OPENROUTER_KEY_HERE":
        return key
    return None


def _get_openrouter_model() -> str:
    """Get configured OpenRouter model or first free default."""
    return config.get_setting("openrouter_model", FREE_MODELS[0])


def _candidate_openrouter_models() -> list[str]:
    configured = _get_openrouter_model().strip()
    models = []
    if configured:
        models.append(configured)
    for model in FREE_MODELS:
        if model not in models:
            models.append(model)
    return models


# ── Prompt builders ────────────────────────────────────────────────────

def _build_prompt(niche: str, city: str, country: str, count: int) -> str:
    return f"""You are a business directory expert. Generate a list of {count} real website URLs for "{niche}" businesses located in or serving {city}, {country}.

Rules:
- Return ONLY actual business website URLs (not directories like Yelp, Google, Facebook, etc.)
- Each URL should be a different company/business
- Include the full URL starting with https://
- Focus on small-to-medium local businesses that are likely to have contact emails on their websites
- Return one URL per line, nothing else — no numbering, no descriptions, no markdown

Example output format:
https://www.smithplumbing.com
https://www.acmeroofing.com
https://www.citycleaners.net"""


def _build_followup_prompt(niche: str, city: str, country: str, count: int, seen_urls: list[str]) -> str:
    seen_block = "\n".join(seen_urls[:80]) if seen_urls else "None yet"
    return f"""Generate {count} MORE real business website URLs for "{niche}" businesses in or serving {city}, {country}.

Already found URLs to avoid repeating:
{seen_block}

Rules:
- Return ONLY new business website URLs not already listed above
- No directories, social profiles, marketplaces, aggregators, or government sites
- Full URLs starting with https://
- One URL per line
- No numbering, no markdown, no commentary"""


# ── URL parser ─────────────────────────────────────────────────────────

def _parse_urls(text: str) -> list[str]:
    """Extract valid URLs from AI response text."""
    if not text:
        return []
    urls = []
    url_pattern = re.compile(r'https?://[^\s,\)\]\"\'>]+')
    for match in url_pattern.findall(text):
        url = match.rstrip(".,;:)")
        if "." in url and len(url) > 10:
            urls.append(url)
    return urls


# ── DeepSeek provider (primary) ───────────────────────────────────────

def _call_deepseek(messages: list[dict], retries: int = 2) -> str | None:
    """Call DeepSeek via OpenAI SDK. Returns response text or None."""
    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("openai package not installed, DeepSeek unavailable")
        return None

    key = _get_deepseek_key()
    if not key:
        return None

    client = OpenAI(
        api_key=key,
        base_url=config.DEEPSEEK_BASE_URL,
    )
    model = config.get_setting("deepseek_model", config.DEEPSEEK_MODEL)

    for attempt in range(retries + 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7,
                max_tokens=2000,
            )
            content = response.choices[0].message.content
            if content:
                return content
        except Exception as e:
            logger.warning("DeepSeek attempt %d failed: %s", attempt + 1, e)
            if attempt < retries:
                time.sleep(1 * (attempt + 1))

    return None


def _generate_deepseek(niche: str, city: str, country: str, count: int) -> list[str]:
    """Generate URLs using DeepSeek. Returns list of URLs or empty list."""
    key = _get_deepseek_key()
    if not key:
        logger.info("DeepSeek: no API key configured, skipping")
        return []

    logger.info("DeepSeek: generating URLs for %s in %s, %s", niche, city, country)

    collected = []
    for attempt in range(3):
        remaining = count - len({u.lower() for u in collected})
        if remaining <= 0:
            break

        if attempt == 0:
            prompt = _build_prompt(niche, city, country, remaining)
        else:
            prompt = _build_followup_prompt(niche, city, country, remaining, collected)

        messages = [{"role": "user", "content": prompt}]
        text = _call_deepseek(messages)
        if not text:
            break

        urls = _parse_urls(text)
        if not urls:
            break

        seen = {u.lower() for u in collected}
        for url in urls:
            if url.lower() not in seen:
                seen.add(url.lower())
                collected.append(url)

        logger.info("DeepSeek attempt %d: got %d URLs (total unique: %d)", attempt + 1, len(urls), len(collected))

    return collected[:count]


# ── OpenRouter provider (fallback) ────────────────────────────────────

async def _generate_openrouter(niche: str, city: str, country: str, count: int) -> list[str]:
    """Generate URLs using OpenRouter. Returns list of URLs or empty list."""
    api_key = _get_openrouter_key()
    if not api_key:
        logger.info("OpenRouter: no API key configured, skipping")
        return []

    logger.info("OpenRouter: generating URLs for %s in %s, %s (fallback)", niche, city, country)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://graphenmail.app",
        "X-Title": "GraphenMail",
    }

    collected = []
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            for attempt in range(3):
                remaining = count - len({u.lower() for u in collected})
                if remaining <= 0:
                    break

                prompt = (
                    _build_prompt(niche, city, country, remaining)
                    if attempt == 0
                    else _build_followup_prompt(niche, city, country, remaining, collected)
                )

                round_urls = []
                for model in _candidate_openrouter_models():
                    payload = {
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.7 if attempt == 0 else 0.9,
                        "max_tokens": 2000,
                    }
                    logger.info("OpenRouter: trying %s attempt=%d", model, attempt + 1)
                    resp = await client.post(OPENROUTER_API_URL, json=payload, headers=headers)

                    if resp.status_code != 200:
                        logger.warning("OpenRouter %s: HTTP %d", model, resp.status_code)
                        continue

                    data = resp.json()
                    content = _extract_openrouter_content(data)
                    urls = _parse_urls(content)
                    if urls:
                        round_urls = urls
                        logger.info("OpenRouter %s: got %d URLs", model, len(urls))
                        break

                    logger.warning("OpenRouter %s: no parseable URLs", model)

                if not round_urls:
                    break

                seen = {u.lower() for u in collected}
                for url in round_urls:
                    if url.lower() not in seen:
                        seen.add(url.lower())
                        collected.append(url)

    except Exception as e:
        logger.error("OpenRouter error: %s", e)

    return collected[:count]


def _extract_openrouter_content(data: dict) -> str:
    choices = data.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    content = message.get("content", "")
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(parts)
    return content if isinstance(content, str) else str(content)


# ── Public API ─────────────────────────────────────────────────────────

async def generate_ai_urls(niche: str, city: str, country: str, count: int = 40) -> list[str]:
    """
    Generate business URLs using AI.
    Strategy: DeepSeek first (primary), OpenRouter fallback if DeepSeek fails or not configured.
    Returns list of URL strings.
    """
    # Try DeepSeek first (synchronous, uses OpenAI SDK)
    urls = _generate_deepseek(niche, city, country, count)
    if urls:
        logger.info("DeepSeek returned %d URLs for %s in %s, %s", len(urls), niche, city, country)
        return urls

    # Fallback to OpenRouter (async, uses httpx)
    urls = await _generate_openrouter(niche, city, country, count)
    if urls:
        logger.info("OpenRouter fallback returned %d URLs for %s in %s, %s", len(urls), niche, city, country)
        return urls

    logger.warning("Both AI providers failed for %s in %s, %s", niche, city, country)
    return []
