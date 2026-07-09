"""
Growth Pulse Automation Engine — Autonomous Prospecting Edition
-------------------------------------------------------------------
Full pipeline: hunt for leads via the official Google Places API,
verify each candidate's website for Meta Pixel / Google Ads Tag markers,
extract contact details from the business's own site, and dispatch
outreach email — all streamed live to a Gradio dashboard.

Architecture:
  A. Hunt layer — Google Places API (New) Text Search, a single POST call
     per query (official, ToS-compliant, no separate Details round trip,
     does not scrape Google Maps directly)
  B. Scraper + pixel verifier — unchanged core logic, now also extracts
     contact email from the business's own website
  C. Persistent deduplication — processed_companies.txt, checked before
     every hunt and every scrape so no domain is ever processed twice
  D. SQLite-backed storage with optional external DB hook
  E. SMTP dispatch engine with exponential backoff retry
  F. Gradio dashboard with a "Launch Autonomous Hunt" panel that streams
     results into a live table as each lead is processed
  G. Background daemon thread for the existing queue-based automation,
     decoupled from the interactive hunt flow, both sharing one storage
     and dispatch layer
"""

import os
import re
import ssl
import csv
import time
import sqlite3
import logging
import smtplib
import threading
from datetime import datetime, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

import requests
import gradio as gr
import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger("growth_pulse")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    email_user: Optional[str] = field(default_factory=lambda: os.environ.get("EMAIL_USER"))
    email_pass: Optional[str] = field(default_factory=lambda: os.environ.get("EMAIL_PASS"))
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 465
    sender_display_name: str = field(default_factory=lambda: os.environ.get("SENDER_DISPLAY_NAME", "Growth Pulse"))

    google_places_api_key: Optional[str] = field(default_factory=lambda: os.environ.get("GOOGLE_PLACES_API_KEY"))

    database_url: Optional[str] = field(default_factory=lambda: os.environ.get("DATABASE_URL"))

    data_dir: str = field(default_factory=lambda: os.environ.get("DATA_DIR", "."))
    sqlite_filename: str = "growth_pulse.db"
    sent_leads_filename: str = "sent_leads.txt"
    processed_companies_filename: str = "processed_companies.txt"

    leads_queue_path: str = field(default_factory=lambda: os.environ.get("LEADS_QUEUE_PATH", "leads_queue.csv"))

    scrape_timeout_seconds: int = 8
    scrape_max_workers: int = 6
    loop_interval_seconds: int = int(os.environ.get("LOOP_INTERVAL_SECONDS", "300"))
    max_retries: int = 3
    max_hunt_results: int = int(os.environ.get("MAX_HUNT_RESULTS", "20"))

    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    # Directory / aggregator domains that are not the contractor's own
    # website. Places API sometimes returns these as the "website" field
    # when a business hasn't registered its own site with Google.
    aggregator_domains: tuple = (
        "yelp.com", "yellowpages.com", "angi.com", "angieslist.com",
        "thumbtack.com", "houzz.com", "bbb.org", "facebook.com",
        "instagram.com", "linkedin.com", "nextdoor.com", "mapquest.com",
        "manta.com", "foursquare.com", "chamberofcommerce.com",
        "superpages.com", "citysearch.com", "homeadvisor.com",
        "porch.com", "buildzoom.com", "google.com",
    )

    @property
    def sqlite_path(self) -> str:
        return os.path.join(self.data_dir, self.sqlite_filename)

    @property
    def sent_leads_path(self) -> str:
        return os.path.join(self.data_dir, self.sent_leads_filename)

    @property
    def processed_companies_path(self) -> str:
        return os.path.join(self.data_dir, self.processed_companies_filename)


CONFIG = Config()

# ---------------------------------------------------------------------------
# Shared runtime state — powers the Gradio dashboard
# ---------------------------------------------------------------------------

class EngineState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.status: str = "Idle"
        self.emails_dispatched_today: int = 0
        self.leads_scraped_total: int = 0
        self.leads_with_pixel_total: int = 0
        self.errors_total: int = 0
        self.last_cycle_started_at: Optional[str] = None
        self.last_cycle_completed_at: Optional[str] = None
        self.last_error: Optional[str] = None
        self._counter_date: date = date.today()

    def _roll_daily_counter_if_needed(self) -> None:
        today = date.today()
        if today != self._counter_date:
            self.emails_dispatched_today = 0
            self._counter_date = today

    def set_status(self, status: str) -> None:
        with self._lock:
            self.status = status

    def mark_cycle_start(self) -> None:
        with self._lock:
            self.last_cycle_started_at = datetime.now().isoformat(timespec="seconds")

    def mark_cycle_complete(self) -> None:
        with self._lock:
            self.last_cycle_completed_at = datetime.now().isoformat(timespec="seconds")

    def record_lead_scraped(self, has_pixel: bool) -> None:
        with self._lock:
            self.leads_scraped_total += 1
            if has_pixel:
                self.leads_with_pixel_total += 1

    def record_email_sent(self) -> None:
        with self._lock:
            self._roll_daily_counter_if_needed()
            self.emails_dispatched_today += 1

    def record_error(self, message: str) -> None:
        with self._lock:
            self.errors_total += 1
            self.last_error = message

    def snapshot(self) -> dict:
        with self._lock:
            self._roll_daily_counter_if_needed()
            return {
                "status": self.status,
                "emails_dispatched_today": self.emails_dispatched_today,
                "leads_scraped_total": self.leads_scraped_total,
                "leads_with_pixel_total": self.leads_with_pixel_total,
                "errors_total": self.errors_total,
                "last_cycle_started_at": self.last_cycle_started_at,
                "last_cycle_completed_at": self.last_cycle_completed_at,
                "last_error": self.last_error,
            }


STATE = EngineState()

# ---------------------------------------------------------------------------
# Persistent deduplication log
# ---------------------------------------------------------------------------

class DeduplicationTracker:
    """
    Maintains processed_companies.txt — a flat, append-only log of every
    domain the engine has ever hunted, scraped, or emailed. Checked before
    any new hunt or scrape so the same contractor is never processed twice
    across separate runs of the hunt panel or the background queue loop.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._cache = set()
        os.makedirs(self.config.data_dir, exist_ok=True)
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.config.processed_companies_path):
            open(self.config.processed_companies_path, "a", encoding="utf-8").close()
            return
        with open(self.config.processed_companies_path, "r", encoding="utf-8") as handle:
            for line in handle:
                domain = line.strip()
                if domain:
                    self._cache.add(domain)

    def is_processed(self, domain: str) -> bool:
        with self._lock:
            return domain.strip().lower() in self._cache

    def mark_processed(self, domain: str) -> None:
        normalized = domain.strip().lower()
        with self._lock:
            if normalized in self._cache:
                return
            self._cache.add(normalized)
            with open(self.config.processed_companies_path, "a", encoding="utf-8") as handle:
                handle.write(f"{normalized}\n")


DEDUP = DeduplicationTracker(CONFIG)

# ---------------------------------------------------------------------------
# Storage layer (SQLite primary, optional external DB hook)
# ---------------------------------------------------------------------------

class StorageBackend:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._local_lock = threading.Lock()
        self._external_adapter = None
        os.makedirs(self.config.data_dir, exist_ok=True)
        self._init_sqlite()
        if self.config.database_url:
            self._init_external()

    def _init_sqlite(self) -> None:
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain TEXT NOT NULL,
                    business_name TEXT,
                    contact_email TEXT,
                    phone_number TEXT,
                    has_meta_pixel INTEGER NOT NULL DEFAULT 0,
                    has_google_ads_tag INTEGER NOT NULL DEFAULT 0,
                    scrape_status TEXT NOT NULL,
                    email_status TEXT NOT NULL DEFAULT 'not_sent',
                    source TEXT NOT NULL DEFAULT 'queue',
                    scraped_at TEXT NOT NULL,
                    UNIQUE(domain)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dispatch_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain TEXT NOT NULL,
                    contact_email TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    outcome TEXT NOT NULL
                )
                """
            )
            connection.commit()
            connection.close()
        logger.info("SQLite storage initialized at %s", self.config.sqlite_path)

    def _init_external(self) -> None:
        try:
            import psycopg2
            self._external_adapter = psycopg2
            connection = psycopg2.connect(self.config.database_url)
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    id SERIAL PRIMARY KEY,
                    domain TEXT UNIQUE NOT NULL,
                    business_name TEXT,
                    contact_email TEXT,
                    phone_number TEXT,
                    has_meta_pixel BOOLEAN NOT NULL DEFAULT FALSE,
                    has_google_ads_tag BOOLEAN NOT NULL DEFAULT FALSE,
                    scrape_status TEXT NOT NULL,
                    email_status TEXT NOT NULL DEFAULT 'not_sent',
                    source TEXT NOT NULL DEFAULT 'queue',
                    scraped_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            connection.commit()
            cursor.close()
            connection.close()
            logger.info("External database hook verified via DATABASE_URL.")
        except ModuleNotFoundError:
            logger.warning("DATABASE_URL is set but psycopg2 is not installed. Using SQLite only.")
            self._external_adapter = None
        except Exception as exc:
            logger.error("External database initialization failed: %s", exc)
            self._external_adapter = None

    def save_lead(self, domain: str, business_name: Optional[str], contact_email: Optional[str],
                  phone_number: Optional[str], has_meta_pixel: bool, has_google_ads_tag: bool,
                  scrape_status: str, source: str) -> None:
        scraped_at = datetime.now().isoformat(timespec="seconds")
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO leads (domain, business_name, contact_email, phone_number, has_meta_pixel,
                                    has_google_ads_tag, scrape_status, source, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    business_name=excluded.business_name,
                    contact_email=excluded.contact_email,
                    phone_number=excluded.phone_number,
                    has_meta_pixel=excluded.has_meta_pixel,
                    has_google_ads_tag=excluded.has_google_ads_tag,
                    scrape_status=excluded.scrape_status,
                    source=excluded.source,
                    scraped_at=excluded.scraped_at
                """,
                (domain, business_name, contact_email, phone_number, int(has_meta_pixel),
                 int(has_google_ads_tag), scrape_status, source, scraped_at),
            )
            connection.commit()
            connection.close()

        if self._external_adapter is not None:
            try:
                connection = self._external_adapter.connect(self.config.database_url)
                cursor = connection.cursor()
                cursor.execute(
                    """
                    INSERT INTO leads (domain, business_name, contact_email, phone_number, has_meta_pixel,
                                        has_google_ads_tag, scrape_status, source, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (domain) DO UPDATE SET
                        business_name=EXCLUDED.business_name,
                        contact_email=EXCLUDED.contact_email,
                        phone_number=EXCLUDED.phone_number,
                        has_meta_pixel=EXCLUDED.has_meta_pixel,
                        has_google_ads_tag=EXCLUDED.has_google_ads_tag,
                        scrape_status=EXCLUDED.scrape_status,
                        source=EXCLUDED.source,
                        scraped_at=EXCLUDED.scraped_at
                    """,
                    (domain, business_name, contact_email, phone_number, has_meta_pixel,
                     has_google_ads_tag, scrape_status, source, scraped_at),
                )
                connection.commit()
                cursor.close()
                connection.close()
            except Exception as exc:
                logger.error("External DB write failed for %s: %s", domain, exc)

    def mark_email_sent(self, domain: str, contact_email: str, outcome: str) -> None:
        sent_at = datetime.now().isoformat(timespec="seconds")
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE leads SET email_status = ? WHERE domain = ?",
                ("sent" if outcome == "success" else "failed", domain),
            )
            cursor.execute(
                "INSERT INTO dispatch_log (domain, contact_email, sent_at, outcome) VALUES (?, ?, ?, ?)",
                (domain, contact_email, sent_at, outcome),
            )
            connection.commit()
            connection.close()

        with open(self.config.sent_leads_path, "a", encoding="utf-8") as handle:
            handle.write(f"{sent_at}\t{domain}\t{contact_email}\t{outcome}\n")

    def already_contacted(self, domain: str) -> bool:
        if not os.path.exists(self.config.sent_leads_path):
            return False
        with open(self.config.sent_leads_path, "r", encoding="utf-8") as handle:
            for line in handle:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 4 and parts[1] == domain and parts[3] == "success":
                    return True
        return False


STORAGE = StorageBackend(CONFIG)

# ---------------------------------------------------------------------------
# Component A: Hunt layer — official Google Places API (New Places API v1)
# ---------------------------------------------------------------------------

PLACES_SEARCH_TEXT_URL = "https://places.googleapis.com/v1/places:searchText"

PLACES_FIELD_MASK = (
    "places.displayName,"
    "places.websiteUri,"
    "places.formattedAddress,"
    "places.nationalPhoneNumber,"
    "places.internationalPhoneNumber"
)


def is_aggregator_domain(domain: str) -> bool:
    lowered = domain.lower()
    return any(aggregator in lowered for aggregator in CONFIG.aggregator_domains)


def extract_domain_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    cleaned = re.sub(r"^https?://", "", url.strip())
    cleaned = cleaned.split("/")[0]
    cleaned = re.sub(r"^www\.", "", cleaned)
    return cleaned.lower() if cleaned else None


def hunt_leads_via_places_api(niche: str, location: str) -> list:
    """
    Queries the official Google Places API (New) Text Search endpoint
    (POST https://places.googleapis.com/v1/places:searchText) for
    businesses matching the given niche and location. Requests
    displayName, websiteUri, formattedAddress, and phone number fields
    directly in a single call via the X-Goog-FieldMask header, with no
    separate Place Details round trip required. Filters out results with
    no independent website and results whose website belongs to a known
    aggregator/directory domain. Returns a list of dicts:
    {business_name, domain, formatted_address, phone_number_from_places}.
    """
    if not CONFIG.google_places_api_key:
        logger.error("GOOGLE_PLACES_API_KEY is not set. Cannot run the hunt layer.")
        STATE.record_error("Missing GOOGLE_PLACES_API_KEY environment secret.")
        return []

    query_text = f"{niche} in {location}"

    request_headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": CONFIG.google_places_api_key,
        "X-Goog-FieldMask": PLACES_FIELD_MASK,
    }
    request_body = {
        "textQuery": query_text,
        "maxResultCount": min(CONFIG.max_hunt_results, 20),
    }

    try:
        response = requests.post(
            PLACES_SEARCH_TEXT_URL,
            headers=request_headers,
            json=request_body,
            timeout=CONFIG.scrape_timeout_seconds,
        )
    except requests.exceptions.RequestException as exc:
        logger.error("Places Text Search request failed: %s", exc)
        STATE.record_error(f"Places Text Search request failed: {exc}")
        return []

    if response.status_code != 200:
        try:
            error_payload = response.json()
            error_message = error_payload.get("error", {}).get("message", response.text)
        except ValueError:
            error_message = response.text
        logger.error("Places Text Search API error (HTTP %d): %s", response.status_code, error_message)
        STATE.record_error(f"Places Text Search API error (HTTP {response.status_code}): {error_message}")
        return []

    try:
        payload = response.json()
    except ValueError as exc:
        logger.error("Places Text Search returned invalid JSON: %s", exc)
        STATE.record_error(f"Places Text Search invalid JSON: {exc}")
        return []

    places = payload.get("places", [])
    logger.info("Places Text Search (New API) returned %d candidate(s) for '%s'.", len(places), query_text)

    candidates = []
    for place in places:
        display_name_block = place.get("displayName", {})
        business_name = display_name_block.get("text", "").strip()
        website_uri = place.get("websiteUri")
        formatted_address = place.get("formattedAddress", "")
        phone_from_places = place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber")

        if not business_name or not website_uri:
            continue

        domain = extract_domain_from_url(website_uri)
        if not domain:
            continue
        if is_aggregator_domain(domain):
            continue
        if DEDUP.is_processed(domain):
            continue

        candidates.append({
            "business_name": business_name,
            "domain": domain,
            "formatted_address": formatted_address,
            "phone_number_from_places": phone_from_places,
        })

    logger.info("Hunt layer yielded %d new, non-aggregator candidate(s) with independent websites.", len(candidates))
    return candidates

# ---------------------------------------------------------------------------
# Component B: Scraper + pixel verifier + contact extractor
# ---------------------------------------------------------------------------

META_PIXEL_MARKER = "connect.facebook.net/en_US/fbevents.js"
GOOGLE_ADS_TAG_MARKER = "googletagmanager.com/gtag/js"

PHONE_REGEX = re.compile(
    r"(\+?\d{1,3}[\s.-]?)?(\(?\d{2,4}\)?[\s.-]?)\d{3,4}[\s.-]?\d{3,4}"
)

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

MAILTO_REGEX = re.compile(r'mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})', re.IGNORECASE)

# Generic inbox addresses that image hosts, CDNs, or template boilerplate
# commonly leave behind. These are filtered out since they rarely reach a
# real decision-maker at the business.
EMAIL_JUNK_PATTERNS = (
    "sentry.io", "wixpress.com", "godaddy.com", "example.com",
    "yourcompany.com", "domain.com", "email.com", "schema.org",
)


def normalize_domain(raw_domain: str) -> str:
    domain = raw_domain.strip()
    if not domain.startswith("http://") and not domain.startswith("https://"):
        domain = "https://" + domain
    return domain


def extract_contact_email(html_text: str) -> Optional[str]:
    mailto_match = MAILTO_REGEX.search(html_text)
    if mailto_match:
        candidate = mailto_match.group(1).strip().lower()
        if not any(junk in candidate for junk in EMAIL_JUNK_PATTERNS):
            return candidate

    for match in EMAIL_REGEX.finditer(html_text):
        candidate = match.group(0).strip().lower()
        if any(junk in candidate for junk in EMAIL_JUNK_PATTERNS):
            continue
        if candidate.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
            continue
        return candidate

    return None


def scrape_domain(raw_domain: str) -> dict:
    url = normalize_domain(raw_domain)
    headers = {"User-Agent": CONFIG.user_agent}
    result = {
        "domain": raw_domain.strip(),
        "has_meta_pixel": False,
        "has_google_ads_tag": False,
        "phone_number": None,
        "contact_email": None,
        "status": "unknown",
    }

    try:
        response = requests.get(url, headers=headers, timeout=CONFIG.scrape_timeout_seconds)
        html_text = response.text
        result["has_meta_pixel"] = META_PIXEL_MARKER in html_text
        result["has_google_ads_tag"] = GOOGLE_ADS_TAG_MARKER in html_text

        phone_match = PHONE_REGEX.search(html_text)
        if phone_match:
            candidate = re.sub(r"\s+", " ", phone_match.group(0)).strip()
            digit_count = len(re.sub(r"\D", "", candidate))
            if 7 <= digit_count <= 15:
                result["phone_number"] = candidate

        result["contact_email"] = extract_contact_email(html_text)
        result["status"] = "success" if response.status_code == 200 else f"http_{response.status_code}"
    except requests.exceptions.Timeout:
        result["status"] = "timeout"
    except requests.exceptions.ConnectionError:
        result["status"] = "connection_error"
    except requests.exceptions.RequestException as exc:
        result["status"] = f"request_error:{exc}"
    except Exception as exc:
        result["status"] = f"unexpected_error:{exc}"

    return result


def scrape_domains_parallel(domains: list) -> list:
    results = []
    if not domains:
        return results

    with ThreadPoolExecutor(max_workers=CONFIG.scrape_max_workers, thread_name_prefix="scraper") as executor:
        future_to_domain = {executor.submit(scrape_domain, domain): domain for domain in domains}
        for future in as_completed(future_to_domain):
            domain = future_to_domain[future]
            try:
                results.append(future.result())
            except Exception as exc:
                logger.error("Scrape task for %s raised an exception: %s", domain, exc)
                results.append({
                    "domain": domain,
                    "has_meta_pixel": False,
                    "has_google_ads_tag": False,
                    "phone_number": None,
                    "contact_email": None,
                    "status": f"task_exception:{exc}",
                })
    return results

# ---------------------------------------------------------------------------
# Component E: SMTP dispatch engine with exponential backoff
# ---------------------------------------------------------------------------

def build_outreach_message(sender_email: str, recipient_email: str, domain: str,
                            business_name: str, has_meta_pixel: bool, has_google_ads_tag: bool) -> MIMEMultipart:
    message = MIMEMultipart("alternative")

    if not has_meta_pixel and not has_google_ads_tag:
        subject = f"{business_name} — your site has zero ad tracking installed"
        pain_point = (
            "I checked your website and found neither Meta Pixel nor Google Ads "
            "conversion tracking installed anywhere on the site. If you're running "
            "any paid ads at all right now, you have no way to know which ones are "
            "actually generating calls or jobs."
        )
    elif not has_meta_pixel:
        subject = f"{business_name} — a gap in your ad tracking setup"
        pain_point = (
            "I checked your website and found Google Ads tracking in place, but no "
            "Meta Pixel. That means every visitor who comes from Facebook or "
            "Instagram and doesn't convert immediately becomes invisible to you "
            "afterward — no retargeting, no attribution."
        )
    else:
        subject = f"{business_name} — a gap in your ad tracking setup"
        pain_point = (
            "I checked your website and found Meta Pixel installed, but no Google "
            "Ads conversion tracking. That means you can't tell which search terms "
            "or campaigns are actually worth the spend."
        )

    message["Subject"] = subject
    message["From"] = f"{CONFIG.sender_display_name} <{sender_email}>"
    message["To"] = recipient_email

    text_body = (
        f"Hello,\n\n"
        f"I took a look at {domain} and wanted to reach out directly.\n\n"
        f"{pain_point}\n\n"
        f"We help local service businesses close exactly this kind of gap so the "
        f"traffic you already have actually turns into booked jobs.\n\n"
        f"Happy to share a short breakdown of what we found if that's useful.\n\n"
        f"Best,\n{CONFIG.sender_display_name}"
    )
    message.attach(MIMEText(text_body, "plain"))
    return message


def send_email_with_retry(recipient_email: str, domain: str, business_name: str,
                           has_meta_pixel: bool, has_google_ads_tag: bool) -> str:
    if not CONFIG.email_user or not CONFIG.email_pass:
        logger.error("EMAIL_USER / EMAIL_PASS are not set in the environment. Cannot dispatch email.")
        STATE.record_error("Missing EMAIL_USER/EMAIL_PASS environment secrets.")
        return "failed"

    message = build_outreach_message(
        CONFIG.email_user, recipient_email, domain, business_name, has_meta_pixel, has_google_ads_tag
    )
    ssl_context = ssl.create_default_context()

    for retry_count in range(CONFIG.max_retries):
        try:
            with smtplib.SMTP_SSL(CONFIG.smtp_host, CONFIG.smtp_port, context=ssl_context, timeout=15) as server:
                server.login(CONFIG.email_user, CONFIG.email_pass)
                server.sendmail(CONFIG.email_user, recipient_email, message.as_string())
            logger.info("Email sent to %s for domain %s on attempt %d", recipient_email, domain, retry_count + 1)
            STATE.record_email_sent()
            return "success"
        except smtplib.SMTPAuthenticationError as exc:
            logger.error("SMTP authentication failed: %s", exc)
            STATE.record_error(f"SMTP auth failure: {exc}")
            return "failed"
        except (smtplib.SMTPException, OSError) as exc:
            wait_seconds = 2 ** retry_count
            logger.warning("SMTP send attempt %d/%d for %s failed (%s). Retrying in %d seconds.", retry_count + 1, CONFIG.max_retries, recipient_email, exc, wait_seconds)
            STATE.record_error(f"SMTP transient failure: {exc}")
            time.sleep(wait_seconds)

    logger.error("All %d SMTP attempts failed for %s.", CONFIG.max_retries, recipient_email)
    return "failed"

# ---------------------------------------------------------------------------
# Lead queue reader (existing passive background loop)
# ---------------------------------------------------------------------------

def read_leads_queue() -> list:
    queue_path = CONFIG.leads_queue_path
    if not os.path.exists(queue_path):
        return []

    rows = []
    with open(queue_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for record in reader:
            domain = (record.get("domain") or "").strip()
            contact_email = (record.get("contact_email") or "").strip()
            if not domain:
                continue
            if STORAGE.already_contacted(domain):
                continue
            if DEDUP.is_processed(domain):
                continue
            rows.append({"domain": domain, "contact_email": contact_email})
    return rows


def run_automation_cycle() -> None:
    STATE.mark_cycle_start()
    STATE.set_status("Scraping (queue)")

    queued_leads = read_leads_queue()
    if not queued_leads:
        STATE.set_status("Idle (queue empty)")
        STATE.mark_cycle_complete()
        return

    domains = [lead["domain"] for lead in queued_leads]
    email_by_domain = {lead["domain"]: lead["contact_email"] for lead in queued_leads}

    scrape_results = scrape_domains_parallel(domains)

    STATE.set_status("Dispatching (queue)")
    for result in scrape_results:
        domain = result["domain"]
        has_pixel = bool(result["has_meta_pixel"] or result["has_google_ads_tag"])
        STATE.record_lead_scraped(has_pixel)

        contact_email = email_by_domain.get(domain) or result.get("contact_email")

        STORAGE.save_lead(
            domain=domain,
            business_name=None,
            contact_email=contact_email,
            phone_number=result["phone_number"],
            has_meta_pixel=result["has_meta_pixel"],
            has_google_ads_tag=result["has_google_ads_tag"],
            scrape_status=result["status"],
            source="queue",
        )
        DEDUP.mark_processed(domain)

        if contact_email and result["status"] == "success":
            outcome = send_email_with_retry(
                contact_email, domain, domain, result["has_meta_pixel"], result["has_google_ads_tag"]
            )
            STORAGE.mark_email_sent(domain, contact_email, outcome)

    STATE.set_status("Idle")
    STATE.mark_cycle_complete()


def automation_loop() -> None:
    logger.info("Growth Pulse background queue loop starting.")
    while True:
        try:
            run_automation_cycle()
        except Exception as exc:
            logger.exception("Unhandled exception in automation cycle: %s", exc)
            STATE.record_error(f"Unhandled cycle exception: {exc}")
            STATE.set_status("Error - retrying next cycle")
        time.sleep(CONFIG.loop_interval_seconds)

# ---------------------------------------------------------------------------
# Component F: Autonomous hunt pipeline (interactive, streamed to the UI)
# ---------------------------------------------------------------------------

HUNT_TABLE_COLUMNS = [
    "Business Name", "Domain", "Has Meta Pixel", "Has Google Ads Tag",
    "Contact Email", "Phone", "Email Status", "Notes",
]


def run_autonomous_hunt(niche: str, location: str):
    """
    Generator that drives the full hunt -> scrape -> verify -> email
    pipeline for a given niche and location, yielding an updated DataFrame
    and a status string after every lead so the Gradio UI updates live.
    """
    niche = (niche or "").strip()
    location = (location or "").strip()

    rows = []

    if not niche or not location:
        yield pd.DataFrame(columns=HUNT_TABLE_COLUMNS), "Please enter both a target industry/niche and a target city/location."
        return

    STATE.mark_cycle_start()
    STATE.set_status(f"Hunting: {niche} in {location}")
    yield pd.DataFrame(columns=HUNT_TABLE_COLUMNS), f"Searching Google Places for '{niche} in {location}'..."

    candidates = hunt_leads_via_places_api(niche, location)

    if not candidates:
        STATE.set_status("Idle")
        STATE.mark_cycle_complete()
        yield pd.DataFrame(columns=HUNT_TABLE_COLUMNS), (
            "No new candidates found. Either GOOGLE_PLACES_API_KEY is missing/invalid, "
            "there were zero results for this search, or every result was already processed."
        )
        return

    STATE.set_status(f"Processing {len(candidates)} candidate(s)")

    for candidate in candidates:
        domain = candidate["domain"]
        business_name = candidate["business_name"]

        if DEDUP.is_processed(domain):
            continue

        scrape_result = scrape_domain(domain)
        has_pixel = bool(scrape_result["has_meta_pixel"] or scrape_result["has_google_ads_tag"])
        STATE.record_lead_scraped(has_pixel)

        contact_email = scrape_result.get("contact_email")
        phone_number = scrape_result.get("phone_number") or candidate.get("phone_number_from_places")

        STORAGE.save_lead(
            domain=domain,
            business_name=business_name,
            contact_email=contact_email,
            phone_number=phone_number,
            has_meta_pixel=scrape_result["has_meta_pixel"],
            has_google_ads_tag=scrape_result["has_google_ads_tag"],
            scrape_status=scrape_result["status"],
            source="hunt",
        )
        DEDUP.mark_processed(domain)

        email_status = "no email found"
        notes = scrape_result["status"]

        if scrape_result["status"] == "success" and contact_email:
            outcome = send_email_with_retry(
                contact_email, domain, business_name,
                scrape_result["has_meta_pixel"], scrape_result["has_google_ads_tag"],
            )
            STORAGE.mark_email_sent(domain, contact_email, outcome)
            email_status = "sent" if outcome == "success" else "failed"
        elif scrape_result["status"] != "success":
            email_status = "skipped (scrape failed)"
            notes = f"Scrape status: {scrape_result['status']}"

        rows.append({
            "Business Name": business_name,
            "Domain": domain,
            "Has Meta Pixel": "Yes" if scrape_result["has_meta_pixel"] else "No",
            "Has Google Ads Tag": "Yes" if scrape_result["has_google_ads_tag"] else "No",
            "Contact Email": contact_email or "—",
            "Phone": phone_number or "—",
            "Email Status": email_status,
            "Notes": notes,
        })

        progress_text = f"Processed {len(rows)}/{len(candidates)} candidate(s) for '{niche} in {location}'."
        yield pd.DataFrame(rows, columns=HUNT_TABLE_COLUMNS), progress_text

    STATE.set_status("Idle")
    STATE.mark_cycle_complete()
    final_text = f"Hunt complete. {len(rows)} lead(s) processed for '{niche} in {location}'."
    yield pd.DataFrame(rows, columns=HUNT_TABLE_COLUMNS), final_text

# ---------------------------------------------------------------------------
# Gradio dashboard
# ---------------------------------------------------------------------------

def get_dashboard_metrics():
    snapshot = STATE.snapshot()
    return (
        snapshot["status"],
        snapshot["emails_dispatched_today"],
        snapshot["leads_scraped_total"],
        snapshot["leads_with_pixel_total"],
        snapshot["errors_total"],
        snapshot["last_cycle_started_at"] or "Not yet run",
        snapshot["last_cycle_completed_at"] or "Not yet run",
        snapshot["last_error"] or "None",
    )


def build_dashboard() -> gr.Blocks:
    with gr.Blocks(title="Growth Pulse — Autonomous Prospecting Engine") as dashboard:
        gr.Markdown("# Growth Pulse — Autonomous Prospecting Engine")

        with gr.Tab("Autonomous Hunt"):
            gr.Markdown(
                "Enter a target industry and location. The engine will search Google "
                "Places for matching businesses, check each one's website for missing "
                "ad tracking, and send outreach email to qualifying leads — live, below."
            )
            with gr.Row():
                niche_input = gr.Textbox(label="Target Industry/Niche", placeholder="Roofing Contractors")
                location_input = gr.Textbox(label="Target City/Location", placeholder="Miami, FL")
            launch_button = gr.Button("Launch Autonomous Hunt", variant="primary", size="lg")
            hunt_status = gr.Textbox(label="Hunt Status", interactive=False)
            hunt_table = gr.Dataframe(headers=HUNT_TABLE_COLUMNS, label="Live Results", wrap=True)

            launch_button.click(
                fn=run_autonomous_hunt,
                inputs=[niche_input, location_input],
                outputs=[hunt_table, hunt_status],
            )

        with gr.Tab("Engine Status"):
            gr.Markdown(
                "Background metrics covering both the autonomous hunt panel and the "
                "passive queue-based loop (leads_queue.csv), if in use."
            )
            with gr.Row():
                status_box = gr.Textbox(label="Engine Status", interactive=False)
                errors_box = gr.Number(label="Errors Total", interactive=False)

            with gr.Row():
                emails_box = gr.Number(label="Emails Dispatched Today", interactive=False)
                leads_box = gr.Number(label="Leads Scraped Total", interactive=False)
                pixel_box = gr.Number(label="Leads With Ad Tracking Gap Detected", interactive=False)

            with gr.Row():
                started_box = gr.Textbox(label="Last Cycle Started", interactive=False)
                completed_box = gr.Textbox(label="Last Cycle Completed", interactive=False)

            last_error_box = gr.Textbox(label="Last Error", interactive=False)
            refresh_button = gr.Button("Refresh Metrics")

            metric_outputs = [
                status_box, emails_box, leads_box, pixel_box,
                errors_box, started_box, completed_box, last_error_box,
            ]

            refresh_button.click(fn=get_dashboard_metrics, inputs=None, outputs=metric_outputs)
            dashboard.load(fn=get_dashboard_metrics, inputs=None, outputs=metric_outputs)

    return dashboard


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

STATE.set_status("Starting")
_background_thread = threading.Thread(target=automation_loop, name="automation_loop", daemon=True)
_background_thread.start()

demo = build_dashboard()
demo.queue()

if __name__ == "__main__":
    demo.launch(
        server_name="0.0.0.0",
        server_port=int(os.environ.get("PORT", "7860")),
        ssr_mode=False,
    )
if __name__ == "__main__":
    # --- AUTOPILOT TARGET HUNT BOOTSTRAPPER ---
    target_niches = os.environ.get("AUTOMATED_NICHES", "")
    target_cities = os.environ.get("AUTOMATED_CITIES", "")
    
    if target_niches and target_cities:
        logger.info("Autopilot triggered. Generating automated global hunting lists...")
        niches = [n.strip() for n in target_niches.split(",") if n.strip()]
        cities = [c.strip() for c in target_cities.split(",") if c.strip()]
        
        # Build a fresh leads queue by hunting these parameters automatically via Places API
        with open(CONFIG.leads_queue_path, "w", encoding="utf-8", newline="") as h:
            writer = csv.writer(h)
            writer.writerow(["domain", "contact_email"]) # Headers required by your loop
            
            for niche in niches:
                for city in cities:
                    logger.info(f"Background scouting: Harvesting {niche} in {city}...")
                    found = hunt_leads_via_places_api(niche, city)
                    for lead in found:
                        # Pre-populate the queue for your background worker thread loop
                        writer.writerow([lead["domain"], ""]) 
        logger.info("Automated queue generation complete. Background thread engine engaging.")
    # ------------------------------------------

    demo = build_dashboard()
    demo.queue()
    demo.launch(
        server_name="0.0.0.0",
        server_port=int(os.environ.get("PORT", "7860")),
        ssr_mode=False,
    )
  
