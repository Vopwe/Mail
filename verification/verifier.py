"""
Email verification — syntax, MX, SMTP, disposable + spam trap detection.
Fully parallel with asyncio.gather + domain MX cache.

When port 25 is blocked (most residential/cloud networks), falls back to
MX + DNS-based scoring that still gives useful valid/invalid results.
"""
import re
import asyncio
import logging
import threading
import uuid
import dns.resolver
import aiosmtplib
import config

logger = logging.getLogger(__name__)

EMAIL_PATTERN = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

# Well-known providers whose MX confirms the domain accepts email
KNOWN_PROVIDERS = {
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.uk", "yahoo.fr",
    "outlook.com", "hotmail.com", "hotmail.co.uk", "live.com", "msn.com",
    "aol.com", "icloud.com", "me.com", "mac.com", "protonmail.com",
    "proton.me", "zoho.com", "yandex.com", "mail.com", "gmx.com",
    "gmx.net", "fastmail.com", "tutanota.com", "hey.com",
}

# MX hosts that indicate Google Workspace, Microsoft 365, etc. (business email = likely valid domain)
BUSINESS_MX_PATTERNS = [
    "google.com", "googlemail.com",       # Google Workspace
    "outlook.com", "microsoft.com",       # Microsoft 365
    "pphosted.com", "mimecast",           # Proofpoint, Mimecast
    "messagelabs.com",                    # Broadcom
    "zoho.com",                           # Zoho
    "emailsrvr.com",                      # Rackspace
    "secureserver.net",                   # GoDaddy
]

# ── Domain MX Cache (shared across verifications) ────────────────────
_mx_cache: dict[str, tuple[bool, str | None]] = {}
_mx_lock = threading.Lock()

# ── Catch-all cache (per domain, avoids re-probing) ─────────────────
_catch_all_cache: dict[str, bool] = {}
_catch_all_lock = threading.Lock()

# ── SMTP availability cache (tested once per run) ────────────────────
_smtp_available: bool | None = None
_smtp_test_lock = asyncio.Lock() if False else None  # initialized lazily
_smtp_test_done = threading.Event()
_smtp_test_started = threading.Event()

# ── Cached sets for spam trap checks (avoid recreating per call) ─────
_safe_roles_set: set | None = None
_soft_risk_set: set | None = None


def _result_template() -> dict:
    return {
        "verification": "unknown",
        "verification_method": "pending",
        "mailbox_confidence": "unknown",
        "domain_confidence": "unknown",
        "mx_valid": None,
        "smtp_valid": None,
        "is_catch_all": 0,
    }


def _prefix_matches(local_part: str, prefix: str) -> bool:
    return local_part == prefix or (
        local_part.startswith(prefix)
        and (len(local_part) == len(prefix) or not local_part[len(prefix)].isalpha())
    )


def _get_mx_cached(domain: str) -> tuple[bool, str | None]:
    """Check MX with per-domain caching."""
    with _mx_lock:
        if domain in _mx_cache:
            return _mx_cache[domain]

    result = _check_mx_raw(domain)

    with _mx_lock:
        _mx_cache[domain] = result
    return result


def _check_mx_raw(domain: str) -> tuple[bool, str | None]:
    """Raw MX lookup. Returns (has_mx, best_mx_host)."""
    try:
        answers = dns.resolver.resolve(domain, "MX")
        if answers:
            best = sorted(answers, key=lambda r: r.preference)[0]
            return True, str(best.exchange).rstrip(".")
        return False, None
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers,
            dns.exception.DNSException, dns.exception.Timeout):
        return False, None


def _check_domain_a_record(domain: str) -> bool:
    """Check if domain has an A record (website exists)."""
    try:
        dns.resolver.resolve(domain, "A")
        return True
    except Exception:
        return False


async def _test_smtp_availability() -> bool:
    """Test if outbound port 25 works by trying a known SMTP server.
    Thread-safe: only one probe runs; all others wait for the result."""
    global _smtp_available

    # Fast path: already tested
    if _smtp_available is not None:
        return _smtp_available

    # Gate: only the first caller runs the test
    if _smtp_test_started.is_set():
        _smtp_test_done.wait(timeout=15)
        return _smtp_available if _smtp_available is not None else False

    _smtp_test_started.set()
    try:
        smtp = aiosmtplib.SMTP(
            hostname="gmail-smtp-in.l.google.com",
            port=25,
            timeout=5,
        )
        await smtp.connect()
        await smtp.quit()
        _smtp_available = True
        logger.info("SMTP port 25 is OPEN — full verification available")
    except Exception:
        _smtp_available = False
        logger.info("SMTP port 25 is BLOCKED — using MX + DNS-based verification")
    finally:
        _smtp_test_done.set()
    return _smtp_available


def check_syntax(email: str) -> bool:
    if not EMAIL_PATTERN.match(email):
        return False
    local, domain = email.split("@")
    if len(local) > 64 or len(domain) > 253:
        return False
    if ".." in email or local.startswith(".") or local.endswith("."):
        return False
    return True


def check_disposable(domain: str) -> bool:
    """Returns True if domain is disposable."""
    disposable = config.get_disposable_domains()
    return domain.lower() in disposable


def check_spam_trap(email: str, domain: str) -> str | None:
    """
    Heuristic spam trap detection.
    Returns "spam_trap", "risky", or None based on:
    1. Known spam trap domains
    2. Known trap prefixes
    3. Suspicious local-part patterns (numeric-only, random hex, keyboard mash)
    """
    global _safe_roles_set, _soft_risk_set
    local_part = email.split("@")[0].lower()
    domain_lower = domain.lower()
    if _safe_roles_set is None:
        _safe_roles_set = set(config.SAFE_ROLE_PREFIXES)
    if _soft_risk_set is None:
        _soft_risk_set = set(config.SOFT_RISK_PREFIXES)
    safe_roles = _safe_roles_set
    soft_risk_prefixes = _soft_risk_set

    # 1. Known spam trap domains
    trap_domains = config.get_spam_trap_domains()
    if domain_lower in trap_domains:
        return "spam_trap"

    if local_part in safe_roles:
        return None

    # 2. Known trap prefixes (exact match on full local part or startswith)
    for prefix in config.SPAM_TRAP_PREFIXES:
        if _prefix_matches(local_part, prefix):
            return "spam_trap"

    for prefix in soft_risk_prefixes:
        if _prefix_matches(local_part, prefix):
            return "risky"

    # 3. Suspicious patterns in local part
    stripped = local_part.replace(".", "").replace("_", "").replace("-", "")

    # Purely numeric local parts (e.g., 123456@domain.com)
    if stripped.isdigit() and len(stripped) >= 5:
        return "risky"

    # Long random hex strings (e.g., a8f9d2e4c1b3@domain.com)
    if len(stripped) >= 10 and all(c in "0123456789abcdef" for c in stripped):
        return "risky"

    # Keyboard mash patterns (e.g., asdfgh@, qwerty@, zxcvbn@)
    keyboard_patterns = [
        "asdfgh", "qwerty", "zxcvbn", "poiuyt", "lkjhgf",
        "mnbvcx", "ytrewq", "fghjkl", "abcdef", "aaaaaa",
        "bbbbbb", "cccccc", "xxxxxx", "zzzzzz",
    ]
    for pat in keyboard_patterns:
        if pat in stripped:
            return "risky"

    # Repeating character patterns (e.g., aaabbb@, 111222@)
    if len(stripped) >= 6:
        # Check if any character repeats 4+ times consecutively
        prev = ""
        count = 0
        for c in stripped:
            if c == prev:
                count += 1
                if count >= 4:
                    return "risky"
            else:
                prev = c
                count = 1

    return None


async def check_smtp(email: str, mx_host: str) -> str:
    """
    SMTP handshake verification. Returns 'valid', 'invalid', or 'unknown'.
    Does NOT send any actual email.
    """
    timeout = int(config.get_setting("verify_timeout", config.VERIFY_TIMEOUT))
    try:
        smtp = aiosmtplib.SMTP(
            hostname=mx_host,
            port=25,
            timeout=timeout,
        )
        await smtp.connect()
        await smtp.ehlo()

        # MAIL FROM
        code, _ = await smtp.execute_command(b"MAIL FROM:<verify@check.local>")
        if code >= 500:
            await smtp.quit()
            return "unknown"

        # RCPT TO — the actual check
        code, message = await smtp.execute_command(f"RCPT TO:<{email}>".encode())
        try:
            await smtp.quit()
        except Exception:
            pass

        if code == 250:
            return "valid"
        elif code in (550, 551, 552, 553, 554):
            return "invalid"
        else:
            return "unknown"

    except (aiosmtplib.SMTPException, asyncio.TimeoutError, OSError, Exception) as e:
        logger.debug(f"SMTP check failed for {email} via {mx_host}: {e}")
        return "unknown"


async def detect_catch_all(domain: str, mx_host: str) -> bool:
    """Check if a domain is catch-all. Cached per domain to avoid redundant probes."""
    with _catch_all_lock:
        if domain in _catch_all_cache:
            return _catch_all_cache[domain]

    probe_email = f"__graphenmail_probe_{uuid.uuid4().hex[:12]}@{domain}"
    is_catch_all = await check_smtp(probe_email, mx_host) == "valid"

    with _catch_all_lock:
        _catch_all_cache[domain] = is_catch_all
    return is_catch_all


def _dns_based_verify(email: str, domain: str, mx_host: str | None) -> dict:
    """
    Fallback verification when port 25 is blocked.
    Uses MX records, known providers, DNS records, and pattern analysis
    to classify emails as valid/invalid/risky.
    """
    result = _result_template()
    result["verification_method"] = "dns"
    result["mx_valid"] = 1 if mx_host else 0
    result["smtp_valid"] = None

    # Known free email providers — if MX is valid, the domain definitely accepts email
    # We can't verify the specific mailbox, but the domain is legit
    if domain.lower() in KNOWN_PROVIDERS:
        result["verification"] = "risky"
        result["verification_method"] = "dns_provider"
        result["domain_confidence"] = "high"
        return result

    if not mx_host:
        result["verification"] = "invalid"
        result["domain_confidence"] = "low"
        return result

    mx_lower = mx_host.lower()

    # Business email on Google Workspace / Microsoft 365 / known providers
    # These domains have been configured to receive email — high confidence
    for pattern in BUSINESS_MX_PATTERNS:
        if pattern in mx_lower:
            result["verification"] = "risky"
            result["verification_method"] = "dns_business_mx"
            result["domain_confidence"] = "high"
            return result

    # Domain has MX records pointing to a real mail server
    # Check if the domain also has an A record (website exists)
    has_website = _check_domain_a_record(domain)

    if has_website:
        result["verification"] = "risky"
        result["verification_method"] = "dns_domain"
        result["domain_confidence"] = "medium"
        return result

    # MX exists but no website — lower confidence
    result["verification"] = "risky"
    result["verification_method"] = "dns_mx_only"
    result["domain_confidence"] = "low"
    return result


async def verify_email(email: str) -> dict:
    """
    Full verification pipeline for a single email.
    Auto-detects if SMTP is available; falls back to DNS-based verification.
    Returns: {verification, mx_valid, smtp_valid}
    """
    result = _result_template()

    # Stage 1: Syntax
    if not check_syntax(email):
        result["verification"] = "invalid"
        result["verification_method"] = "syntax"
        result["mailbox_confidence"] = "low"
        return result

    domain = email.split("@")[1]

    # Stage 2: Disposable check
    if check_disposable(domain):
        result["verification"] = "risky"
        result["verification_method"] = "heuristic_disposable"
        result["domain_confidence"] = "low"
        return result

    # Stage 2.5: Spam trap heuristics
    trap_result = check_spam_trap(email, domain)
    if trap_result == "spam_trap":
        result["verification"] = "spam_trap"
        result["verification_method"] = "heuristic_spam_trap"
        result["mailbox_confidence"] = "low"
        return result
    if trap_result == "risky":
        result["verification"] = "risky"
        result["verification_method"] = "heuristic_risky_local"
        result["mailbox_confidence"] = "low"
        return result

    # Stage 3: MX check (cached per domain)
    has_mx, mx_host = _get_mx_cached(domain)
    result["mx_valid"] = 1 if has_mx else 0

    if not has_mx:
        result["verification"] = "invalid"
        result["verification_method"] = "dns_no_mx"
        result["domain_confidence"] = "low"
        return result

    # Stage 4: SMTP or DNS-based verification
    smtp_ok = await _test_smtp_availability()

    if smtp_ok:
        # Full SMTP verification
        smtp_result = await check_smtp(email, mx_host)
        result["smtp_valid"] = 1 if smtp_result == "valid" else (0 if smtp_result == "invalid" else None)
        result["verification_method"] = "smtp"
        result["domain_confidence"] = "high"

        if smtp_result == "valid":
            if await detect_catch_all(domain, mx_host):
                result["verification"] = "risky"
                result["verification_method"] = "smtp_catch_all"
                result["mailbox_confidence"] = "low"
                result["is_catch_all"] = 1
            else:
                result["verification"] = "valid"
                result["mailbox_confidence"] = "high"
        elif smtp_result == "invalid":
            result["verification"] = "invalid"
            result["mailbox_confidence"] = "high"
        else:
            result["verification"] = "unknown"
    else:
        # Fallback: DNS-based verification
        dns_result = _dns_based_verify(email, domain, mx_host)
        result.update(dns_result)

    return result


async def _verify_single(record: dict, semaphore: asyncio.Semaphore,
                         results: list, counter: dict, total: int,
                         lock: asyncio.Lock, on_progress) -> None:
    """Verify one email record under semaphore control."""
    async with semaphore:
        r = await verify_email(record["email"])
        r["id"] = record["id"]

    async with lock:
        results.append(r)
        counter["done"] += 1
        v = r["verification"]
        if v == "valid":
            counter["valid"] += 1
        elif v == "invalid":
            counter["invalid"] += 1
        elif v == "risky":
            counter["risky"] += 1
        elif v == "spam_trap":
            counter["spam_trap"] += 1
        else:
            counter["unknown"] += 1

        if on_progress:
            on_progress(counter["done"], total, counter)


async def verify_emails_batch(emails: list[dict], on_progress=None) -> list[dict]:
    """
    Verify a batch of email records in PARALLEL using asyncio.gather.
    No limit on batch size. Auto-detects SMTP availability.
    Concurrency controlled by verify_concurrency setting.
    """
    concurrency = int(config.get_setting("verify_concurrency", config.VERIFY_CONCURRENCY))
    semaphore = asyncio.Semaphore(concurrency)
    results = []
    counter = {"done": 0, "valid": 0, "invalid": 0, "risky": 0, "spam_trap": 0, "unknown": 0}
    total = len(emails)
    lock = asyncio.Lock()

    # Pre-test SMTP availability once before the batch
    await _test_smtp_availability()

    tasks = [
        _verify_single(record, semaphore, results, counter, total, lock, on_progress)
        for record in emails
    ]

    await asyncio.gather(*tasks)
    return results


def clear_mx_cache():
    """Clear all caches between verification runs."""
    global _mx_cache, _smtp_available, _safe_roles_set, _soft_risk_set
    with _mx_lock:
        _mx_cache.clear()
    with _catch_all_lock:
        _catch_all_cache.clear()
    _smtp_available = None
    _smtp_test_started.clear()
    _smtp_test_done.clear()
    _safe_roles_set = None
    _soft_risk_set = None
