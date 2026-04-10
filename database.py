"""
SQLite database — schema, connection, CRUD helpers.
"""
import json
import sqlite3
import threading
from contextlib import contextmanager

import config

_thread_local = threading.local()
_write_lock = threading.Lock()


def _connect_db() -> sqlite3.Connection:
    db = sqlite3.connect(
        config.DATABASE_PATH,
        timeout=30,
        uri=config.DATABASE_PATH.startswith("file:"),
    )
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        pass
    db.execute("PRAGMA foreign_keys=ON")
    return db


def get_db() -> sqlite3.Connection:
    db = getattr(_thread_local, "db", None)
    if db is None:
        db = _connect_db()
        _thread_local.db = db
        return db

    try:
        db.execute("SELECT 1")
    except sqlite3.ProgrammingError:
        db = _connect_db()
        _thread_local.db = db
    return db


def close_db():
    db = getattr(_thread_local, "db", None)
    if db is None:
        return
    db.close()
    _thread_local.db = None


@contextmanager
def _write_db():
    db = get_db()
    with _write_lock:
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise


def _decode_campaign_row(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    data = dict(row)
    data["niches"] = json.loads(data["niches"])
    data["countries"] = json.loads(data["countries"])
    data["cities"] = json.loads(data["cities"])
    return data


def _ensure_email_columns(db: sqlite3.Connection):
    columns = {row["name"] for row in db.execute("PRAGMA table_info(emails)").fetchall()}
    additions = {
        "verification_method": "ALTER TABLE emails ADD COLUMN verification_method TEXT DEFAULT 'pending'",
        "mailbox_confidence": "ALTER TABLE emails ADD COLUMN mailbox_confidence TEXT DEFAULT 'unknown'",
        "domain_confidence": "ALTER TABLE emails ADD COLUMN domain_confidence TEXT DEFAULT 'unknown'",
        "is_catch_all": "ALTER TABLE emails ADD COLUMN is_catch_all INTEGER DEFAULT 0",
    }
    for name, ddl in additions.items():
        if name not in columns:
            db.execute(ddl)


def init_db():
    with _write_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            niches      TEXT NOT NULL,
            countries   TEXT NOT NULL,
            cities      TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            total_urls  INTEGER DEFAULT 0,
            total_emails INTEGER DEFAULT 0,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS urls (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
            url         TEXT NOT NULL,
            domain      TEXT NOT NULL,
            niche       TEXT,
            city        TEXT,
            country     TEXT,
            status      TEXT NOT NULL DEFAULT 'pending',
            http_status INTEGER,
            crawled_at  TEXT,
            error       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_urls_campaign ON urls(campaign_id);
        CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(status);

        CREATE TABLE IF NOT EXISTS emails (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            email           TEXT NOT NULL,
            domain          TEXT NOT NULL,
            source_url      TEXT NOT NULL,
            source_domain   TEXT NOT NULL,
            campaign_id     INTEGER REFERENCES campaigns(id) ON DELETE CASCADE,
            niche           TEXT,
            city            TEXT,
            country         TEXT,
            verification    TEXT DEFAULT 'unverified',
            verification_method TEXT DEFAULT 'pending',
            mailbox_confidence TEXT DEFAULT 'unknown',
            domain_confidence TEXT DEFAULT 'unknown',
            mx_valid        INTEGER,
            smtp_valid      INTEGER,
            is_catch_all    INTEGER DEFAULT 0,
            is_generic      INTEGER DEFAULT 0,
            extracted_at    TEXT NOT NULL DEFAULT (datetime('now')),
            verified_at     TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_emails_unique ON emails(email, campaign_id);
        CREATE INDEX IF NOT EXISTS idx_emails_campaign ON emails(campaign_id);
        CREATE INDEX IF NOT EXISTS idx_emails_domain ON emails(domain);
        CREATE INDEX IF NOT EXISTS idx_emails_verification ON emails(verification);
        CREATE INDEX IF NOT EXISTS idx_emails_niche ON emails(niche);
        CREATE INDEX IF NOT EXISTS idx_emails_country ON emails(country);
    """)
        _ensure_email_columns(db)


# ── Campaign CRUD ─────────────────────────────────────────────────────

def insert_campaign(name: str, niches: list, countries: list, cities: list) -> int:
    with _write_db() as db:
        cur = db.execute(
            "INSERT INTO campaigns (name, niches, countries, cities) VALUES (?, ?, ?, ?)",
            (name, json.dumps(niches), json.dumps(countries), json.dumps(cities)),
        )
        return cur.lastrowid


def get_campaign(campaign_id: int) -> dict | None:
    row = get_db().execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,)).fetchone()
    return _decode_campaign_row(row)


def get_campaigns(status: str | None = None) -> list[dict]:
    db = get_db()
    if status:
        rows = db.execute("SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC", (status,)).fetchall()
    else:
        rows = db.execute("SELECT * FROM campaigns ORDER BY created_at DESC").fetchall()
    return [_decode_campaign_row(row) for row in rows]


def update_campaign_status(campaign_id: int, status: str):
    with _write_db() as db:
        db.execute(
            "UPDATE campaigns SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, campaign_id),
        )


def update_campaign_counts(campaign_id: int):
    db = get_db()
    url_count = db.execute("SELECT COUNT(*) FROM urls WHERE campaign_id = ?", (campaign_id,)).fetchone()[0]
    email_count = db.execute("SELECT COUNT(*) FROM emails WHERE campaign_id = ?", (campaign_id,)).fetchone()[0]
    with _write_db() as write_db:
        write_db.execute(
            "UPDATE campaigns SET total_urls = ?, total_emails = ?, updated_at = datetime('now') WHERE id = ?",
            (url_count, email_count, campaign_id),
        )


def delete_campaign(campaign_id: int):
    with _write_db() as db:
        db.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))


# ── URL CRUD ──────────────────────────────────────────────────────────

def insert_urls(rows: list[dict]):
    if not rows:
        return
    with _write_db() as db:
        db.executemany(
            "INSERT INTO urls (campaign_id, url, domain, niche, city, country) VALUES (:campaign_id, :url, :domain, :niche, :city, :country)",
            rows,
        )


def get_urls(campaign_id: int, status: str | None = None) -> list[dict]:
    db = get_db()
    if status:
        rows = db.execute("SELECT * FROM urls WHERE campaign_id = ? AND status = ?", (campaign_id, status)).fetchall()
    else:
        rows = db.execute("SELECT * FROM urls WHERE campaign_id = ?", (campaign_id,)).fetchall()
    return [dict(r) for r in rows]


def update_url_status(url_id: int, status: str, http_status: int | None = None, error: str | None = None):
    with _write_db() as db:
        db.execute(
            "UPDATE urls SET status = ?, http_status = ?, crawled_at = datetime('now'), error = ? WHERE id = ?",
            (status, http_status, error, url_id),
        )


# ── Email CRUD ────────────────────────────────────────────────────────

def insert_email(email: str, domain: str, source_url: str, source_domain: str,
                 campaign_id: int, niche: str, city: str, country: str, is_generic: int = 0):
    with _write_db() as db:
        db.execute(
            """INSERT OR IGNORE INTO emails
               (email, domain, source_url, source_domain, campaign_id, niche, city, country, is_generic)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (email, domain, source_url, source_domain, campaign_id, niche, city, country, is_generic),
        )


def insert_emails_bulk(rows: list[dict]):
    if not rows:
        return
    with _write_db() as db:
        db.executemany(
            """INSERT OR IGNORE INTO emails
               (email, domain, source_url, source_domain, campaign_id, niche, city, country, is_generic)
               VALUES (:email, :domain, :source_url, :source_domain, :campaign_id, :niche, :city, :country, :is_generic)""",
            rows,
        )


def get_emails(campaign_id: int | None = None, niche: str | None = None,
               city: str | None = None, country: str | None = None,
               verification: str | None = None, domain: str | None = None,
               search: str | None = None,
               page: int = 1, per_page: int = 50) -> tuple[list[dict], int]:
    db = get_db()
    conditions = []
    params = []

    if campaign_id:
        conditions.append("campaign_id = ?")
        params.append(campaign_id)
    if niche:
        conditions.append("niche = ?")
        params.append(niche)
    if city:
        conditions.append("city = ?")
        params.append(city)
    if country:
        conditions.append("country = ?")
        params.append(country)
    if verification:
        conditions.append("verification = ?")
        params.append(verification)
    if domain:
        conditions.append("domain LIKE ?")
        params.append(f"%{domain}%")
    if search:
        conditions.append("email LIKE ?")
        params.append(f"%{search}%")

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    total = db.execute(f"SELECT COUNT(*) FROM emails{where}", params).fetchone()[0]

    offset = (page - 1) * per_page
    rows = db.execute(
        f"SELECT * FROM emails{where} ORDER BY extracted_at DESC LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()

    return [dict(r) for r in rows], total


def get_all_emails_filtered(campaign_id: int | None = None, niche: str | None = None,
                            city: str | None = None, country: str | None = None,
                            verification: str | None = None, domain: str | None = None) -> list[dict]:
    db = get_db()
    conditions = []
    params = []
    if campaign_id:
        conditions.append("campaign_id = ?")
        params.append(campaign_id)
    if niche:
        conditions.append("niche = ?")
        params.append(niche)
    if city:
        conditions.append("city = ?")
        params.append(city)
    if country:
        conditions.append("country = ?")
        params.append(country)
    if verification:
        conditions.append("verification = ?")
        params.append(verification)
    if domain:
        conditions.append("domain LIKE ?")
        params.append(f"%{domain}%")
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = db.execute(f"SELECT * FROM emails{where} ORDER BY extracted_at DESC", params).fetchall()
    return [dict(r) for r in rows]


def update_email_verification(email_id: int, verification: str,
                              mx_valid: int | None, smtp_valid: int | None,
                              verification_method: str,
                              mailbox_confidence: str,
                              domain_confidence: str,
                              is_catch_all: int | None = 0):
    with _write_db() as db:
        db.execute(
            """UPDATE emails
               SET verification = ?,
                   mx_valid = ?,
                   smtp_valid = ?,
                   verification_method = ?,
                   mailbox_confidence = ?,
                   domain_confidence = ?,
                   is_catch_all = ?,
                   verified_at = datetime('now')
               WHERE id = ?""",
            (
                verification,
                mx_valid,
                smtp_valid,
                verification_method,
                mailbox_confidence,
                domain_confidence,
                is_catch_all,
                email_id,
            ),
        )


def get_emails_by_ids(ids: list[int]) -> list[dict]:
    if not ids:
        return []
    db = get_db()
    placeholders = ",".join("?" for _ in ids)
    rows = db.execute(f"SELECT * FROM emails WHERE id IN ({placeholders})", ids).fetchall()
    return [dict(r) for r in rows]


def get_unverified_emails(campaign_id: int | None = None,
                          include_unknown: bool = False,
                          include_all: bool = False) -> list[dict]:
    """Get emails for verification.
    - Default: only 'unverified'
    - include_unknown: also 'unknown'
    - include_all: ALL emails regardless of status (re-verify everything)
    """
    db = get_db()

    if include_all:
        if campaign_id:
            rows = db.execute("SELECT * FROM emails WHERE campaign_id = ?", (campaign_id,)).fetchall()
        else:
            rows = db.execute("SELECT * FROM emails").fetchall()
    else:
        statuses = ["'unverified'"]
        if include_unknown:
            statuses.append("'unknown'")
        status_clause = f"verification IN ({','.join(statuses)})"

        if campaign_id:
            rows = db.execute(
                f"SELECT * FROM emails WHERE {status_clause} AND campaign_id = ?",
                (campaign_id,),
            ).fetchall()
        else:
            rows = db.execute(
                f"SELECT * FROM emails WHERE {status_clause}"
            ).fetchall()
    return [dict(r) for r in rows]


# ── Stats ─────────────────────────────────────────────────────────────

def get_stats() -> dict:
    db = get_db()
    return {
        "total_campaigns": db.execute("SELECT COUNT(*) FROM campaigns").fetchone()[0],
        "active_campaigns": db.execute("SELECT COUNT(*) FROM campaigns WHERE status IN ('generating','crawling')").fetchone()[0],
        "total_emails": db.execute("SELECT COUNT(*) FROM emails").fetchone()[0],
        "verified_emails": db.execute("SELECT COUNT(*) FROM emails WHERE verification = 'valid'").fetchone()[0],
        "invalid_emails": db.execute("SELECT COUNT(*) FROM emails WHERE verification = 'invalid'").fetchone()[0],
        "risky_emails": db.execute("SELECT COUNT(*) FROM emails WHERE verification = 'risky'").fetchone()[0],
        "spam_trap_emails": db.execute("SELECT COUNT(*) FROM emails WHERE verification = 'spam_trap'").fetchone()[0],
        "unverified_emails": db.execute("SELECT COUNT(*) FROM emails WHERE verification = 'unverified'").fetchone()[0],
        "total_urls": db.execute("SELECT COUNT(*) FROM urls").fetchone()[0],
    }


def get_distinct_values(column: str) -> list[str]:
    db = get_db()
    allowed = {"niche", "city", "country", "domain", "verification"}
    if column not in allowed:
        return []
    rows = db.execute(f"SELECT DISTINCT {column} FROM emails WHERE {column} IS NOT NULL ORDER BY {column}").fetchall()
    return [r[0] for r in rows]
