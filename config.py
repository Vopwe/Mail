"""
Configuration — API keys, model settings, crawl behavior.
Supports runtime overrides via settings.json.
"""
import os
import json

# ─── Paths ────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_PATH = os.path.join(BASE_DIR, "settings.json")
DATABASE_PATH = os.getenv("EMAIL_DB_PATH", os.path.join(BASE_DIR, "emails.db"))
LOCATIONS_PATH = os.path.join(BASE_DIR, "data", "locations.json")
DISPOSABLE_PATH = os.path.join(BASE_DIR, "data", "disposable_domains.txt")
SPAM_TRAP_DOMAINS_PATH = os.path.join(BASE_DIR, "data", "spam_trap_domains.txt")

# ─── AI Provider Defaults ─────────────────────────────────────────────
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "YOUR_DEEPSEEK_KEY_HERE")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "YOUR_OPENROUTER_KEY_HERE")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_MODEL = "deepseek/deepseek-chat"

# ─── Crawl Defaults ──────────────────────────────────────────────────
MAX_CONCURRENT_REQUESTS = 30
REQUEST_TIMEOUT = 12
CRAWL_DELAY = 0.2
MAX_PAGES_PER_DOMAIN = 5

COMMON_PATHS = [
    "/", "/contact", "/contact-us", "/about", "/about-us",
    "/team", "/our-team", "/staff", "/people", "/support",
    "/impressum", "/legal",
]

# ─── Email Verification ──────────────────────────────────────────────
VERIFY_TIMEOUT = 10
SKIP_GENERIC_EMAILS = True

# System/bounce prefixes — these are NOT read by real people, always flagged
GENERIC_PREFIXES = [
    "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
    "mailer-daemon", "daemon", "bounce", "bounces",
    "postmaster", "hostmaster", "usenet", "news", "root",
    "nobody", "devnull", "null", "void",
]

# Known spam trap / honeypot prefixes
SPAM_TRAP_PREFIXES = [
    "spamtrap", "spam-trap", "spam_trap",
    "honeypot", "honey-pot", "honey_pot",
    "trap", "spam",
    "antispam", "anti-spam",
    "phishing", "malware",
    "blackhole", "black-hole",
    "junk", "quarantine",
    "seedlist", "seed-list",
    "example", "sample",
    "tempmail", "temp-mail",
]

# Role inboxes that should not be auto-flagged as traps.
SAFE_ROLE_PREFIXES = [
    "abuse", "admin", "billing", "careers", "compliance",
    "contact", "customerservice", "help", "hr", "info",
    "legal", "marketing", "office", "operations", "postmaster",
    "privacy", "sales", "service", "support",
]

# Ambiguous local parts that are suspicious, but not enough for a hard spam-trap label.
SOFT_RISK_PREFIXES = [
    "test", "testing", "tester",
    "asdf", "qwerty", "aaa", "zzz", "xxx",
]

# ─── URLs per AI call ────────────────────────────────────────────────
URLS_PER_BATCH = 20

# ─── AI Concurrency ──────────────────────────────────────────────────
AI_CONCURRENCY = 30

# ─── Verification Concurrency ────────────────────────────────────────
VERIFY_CONCURRENCY = 30


# ─── Runtime Settings (settings.json) ────────────────────────────────

def _load_settings() -> dict:
    if os.path.exists(SETTINGS_PATH):
        try:
            with open(SETTINGS_PATH, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def get_setting(key: str, default=None):
    """Get a runtime setting. Falls back to the provided default."""
    settings = _load_settings()
    return settings.get(key, default)


def save_settings(updates: dict):
    """Merge updates into settings.json."""
    settings = _load_settings()
    settings.update(updates)
    with open(SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2)


def get_all_settings() -> dict:
    """Return all runtime settings merged with defaults."""
    defaults = {
        "deepseek_api_key": DEEPSEEK_API_KEY,
        "deepseek_model": DEEPSEEK_MODEL,
        "openrouter_api_key": OPENROUTER_API_KEY,
        "openrouter_model": OPENROUTER_MODEL,
        "ai_concurrency": AI_CONCURRENCY,
        "verify_concurrency": VERIFY_CONCURRENCY,
        "max_concurrent_requests": MAX_CONCURRENT_REQUESTS,
        "request_timeout": REQUEST_TIMEOUT,
        "crawl_delay": CRAWL_DELAY,
        "max_pages_per_domain": MAX_PAGES_PER_DOMAIN,
        "urls_per_batch": URLS_PER_BATCH,
        "verify_timeout": VERIFY_TIMEOUT,
    }
    settings = _load_settings()
    defaults.update(settings)
    return defaults


# ─── Location Data ────────────────────────────────────────────────────

_locations_cache = None

def get_locations() -> dict:
    global _locations_cache
    if _locations_cache is None:
        with open(LOCATIONS_PATH, "r", encoding="utf-8") as f:
            _locations_cache = json.load(f)
    return _locations_cache


# ─── Disposable Domains ──────────────────────────────────────────────

_disposable_cache = None

def get_disposable_domains() -> set:
    global _disposable_cache
    if _disposable_cache is None:
        try:
            with open(DISPOSABLE_PATH, "r") as f:
                _disposable_cache = {line.strip().lower() for line in f if line.strip()}
        except FileNotFoundError:
            _disposable_cache = set()
    return _disposable_cache


# ─── Spam Trap Domains ───────────────────────────────────────────────

_spam_trap_cache = None

def get_spam_trap_domains() -> set:
    global _spam_trap_cache
    if _spam_trap_cache is None:
        try:
            with open(SPAM_TRAP_DOMAINS_PATH, "r") as f:
                _spam_trap_cache = {
                    line.strip().lower()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                }
        except FileNotFoundError:
            _spam_trap_cache = set()
    return _spam_trap_cache
