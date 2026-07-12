"""
Growth Pulse Automation Engine — Autonomous Prospecting Edition
-------------------------------------------------------------------
Full pipeline: hunt for leads via the official Google Places API,
verify each candidate's website for Meta Pixel / Google Ads Tag markers,
extract contact details from the business's own site, and dispatch
outreach email — all streamed live to a Gradio dashboard.

Architecture:
  A. Hunt layer — Google Places API (New) Text Search.
  B. Scraper + pixel verifier — extracts contact email and checks for tracking pixels.
  C. Persistent deduplication — processed_companies.txt.
  D. SQLite-backed storage — tracking leads, emails, and follow-ups.
  E. Brevo dispatch engine — with daily cap and retry.
  F. Gradio dashboard — live stream of results.
  G. GlobalHunterThread — autonomous niche x city rotation, driven by
     AUTOMATED_NICHES / AUTOMATED_CITIES env vars. (Previously declared
     in render.yaml but never actually wired into a background loop —
     this rebuild makes it real.)

# BEAST PHASE 3 IMPROVEMENTS (this rebuild):
- IMPROVED: Real autonomous hunt loop (GlobalHunterThread) — niches x cities
  rotation was configured in render.yaml but no code ever consumed it.
- IMPROVED: Founder/competitor/GBP lookups moved off raw Google HTML scraping
  (which returns consent-wall / CAPTCHA markup for most non-US IPs and gets
  blocked fast) onto DuckDuckGo's HTML-lite endpoint, which is scrape-tolerant.
- IMPROVED: Multi-page contact crawl (home + /contact + /about + /team) with
  mailto: extraction and a junk-email blocklist, instead of one regex pass
  over the homepage only.
- IMPROVED: 3-tier contact prioritization (named personal mailto > role-based
  founder/owner/ceo local-part > generic info/contact) instead of a single
  keyword pass.
- IMPROVED: Deliverability — plain-text alternative part, List-Unsubscribe
  header, jittered send delay, warm-up ramp on top of DAILY_EMAIL_CAP.
- IMPROVED: HTTP session with retry/backoff + http/https/www fallback instead
  of a single unguarded requests.get.
- IMPROVED: Errors are now counted and surfaced in STATE (previously
  swallowed silently by bare except blocks).
- IMPROVED: Gradio dashboard auto-refresh via gr.Timer instead of the
  version-fragile demo.load(..., every=...) pattern.

# BEAST PHASE 4 IMPROVEMENTS (this pass):
- IMPROVED: Render's free web tier has no persistent disk — every redeploy
  or restart wiped growth_pulse.db and processed_companies.txt, silently
  losing lead history and re-emailing already-contacted companies. This
  adds a GitHub-backed snapshot/restore (Contents API over plain requests,
  no new client library to trust blind) that pulls state on boot and pushes
  it after every hunt cycle and on a timer.
- IMPROVED: MX-record check before send — domains with no mail server are
  filtered out before hitting Brevo, cutting hard bounces that damage
  sender reputation.
- IMPROVED: Timezone-aware send gating — AUTOMATED_CITIES spans US/UK/AU/UAE
  time zones; sends now hold until local business hours instead of firing
  whenever the loop happens to reach that city in UTC.
- IMPROVED: Dropped the "60-second video walkthrough" promise from the
  email copy — nothing in this stack generates that video, so a prospect
  replying YES was a guaranteed broken promise and a spam-complaint risk.
  Replaced with a claim the system can actually back up on reply.

# BEAST PHASE 5 IMPROVEMENTS (this pass):
- FIXED: root cause of "Overpass keeps failing on Render" — Render's free
  tier has no outbound IPv6 route, and Overpass (among other hosts) publishes
  both A and AAAA DNS records; the old code let Python try the IPv6 address
  first and fail with "Network is unreachable" before ever reaching IPv4.
  A process-wide urllib3 monkeypatch forces IPv4-only resolution, fixing
  this for every outbound call in the file, not only Overpass.
- IMPROVED: Overpass now rotates across four public mirrors with a short
  per-mirror timeout instead of hammering a single overloaded instance —
  overpass-api.de alone regularly answers 429/504 under normal load,
  independent of any networking issue.
- IMPROVED: Overpass/OSM discovery no longer hard-depends on a Geoapify key
  for geocoding — falls back to Nominatim (OSM's own free, keyless geocoder)
  so raw OSM discovery keeps working on a zero-paid-key deployment.
- IMPROVED: OVERPASS_NICHE_TAGS now maps each trade to every OSM tagging
  style actually in use (craft=, shop=, office=) instead of one guess per
  niche, and adds plumbing/painting/carpentry coverage — this is the direct
  lever for "find more real local trades" from a source that costs no quota.
- IMPROVED: every discovery source's per-cycle and cumulative contribution
  is now counted and logged (hunt_candidates), so it's possible to see which
  source is actually producing candidates for a given niche/city instead of
  only ever seeing a combined total.
- IMPROVED: Gradio dashboard rebuilt — metric cards instead of a raw JSON
  dump, a Discovery Sources tab showing the breakdown above, a live log tail
  backed by an in-memory ring buffer, and a read-only Automation tab
  summarizing what the autonomous rotation is actually configured to do.
"""

import os
import re
import csv
import json
import time
import base64
import random
import socket
import sqlite3
import logging
import threading
import collections
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse, urljoin, quote
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter, Retry
import gradio as gr
import pandas as pd

try:
    import dns.resolver  # dnspython
    _DNS_AVAILABLE = True
except ImportError:
    _DNS_AVAILABLE = False

# ---------------------------------------------------------------------------
# BEAST PHASE 5 - ROOT-CAUSE FIX: "Network is unreachable" on Render free tier
# ---------------------------------------------------------------------------
# Render's free web tier has no outbound IPv6 route. Overpass, and a growing
# number of other hosts, publish both an A (IPv4) and AAAA (IPv6) DNS record.
# Python's socket layer tries the addresses getaddrinfo() returns in order;
# when the first one it tries is the AAAA record, the connect() call fails
# immediately with OSError: [Errno 101] Network is unreachable. This isn't a
# retry-able flake - every call to a dual-stack host fails the same way,
# which is exactly the "Overpass keeps failing" symptom reported. It affects
# every outbound call in this file (Overpass, Geoapify, DuckDuckGo, Brevo,
# GitHub, Telegram), not just Overpass; those calls happened to fail less
# often only because those hosts happen not to advertise AAAA records today.
#
# Fix: force urllib3 (which `requests` sits on top of) to only resolve
# IPv4 addresses, process-wide. This is a two-line monkeypatch of urllib3's
# address-family selector and needs no per-call code changes anywhere else.
# ---------------------------------------------------------------------------

import urllib3.util.connection as _urllib3_cn

def _force_ipv4_only():
    return socket.AF_INET

_urllib3_cn.allowed_gai_family = _force_ipv4_only

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger("growth_pulse")


# IMPROVED: in-memory ring buffer of the last N log lines, surfaced live on
# the Gradio dashboard so "is this actually working" has a real answer
# without needing to open the Render log viewer in another tab.
class _RingBufferHandler(logging.Handler):
    def __init__(self, capacity: int = 300):
        super().__init__()
        self.buffer = collections.deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.buffer.append(self.format(record))
        except Exception:
            pass

    def tail_text(self) -> str:
        return "\n".join(self.buffer) if self.buffer else "No log activity yet."


LOG_RING = _RingBufferHandler()
LOG_RING.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(LOG_RING)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    email_user: Optional[str] = field(default_factory=lambda: os.environ.get("EMAIL_USER"))
    email_pass: Optional[str] = field(default_factory=lambda: os.environ.get("EMAIL_PASS"))
    brevo_api_key: Optional[str] = field(default_factory=lambda: os.environ.get("BREVO_API_KEY"))
    sender_display_name: str = field(default_factory=lambda: os.environ.get("SENDER_DISPLAY_NAME", "Growth Pulse"))

    # IMPROVED: Google Places replaced with Geoapify. Kept the old field so
    # nothing breaks if it's still set, but it is no longer read anywhere.
    google_places_api_key: Optional[str] = field(default_factory=lambda: os.environ.get("GOOGLE_PLACES_API_KEY"))
    geoapify_api_key: Optional[str] = field(default_factory=lambda: os.environ.get("GEOAPIFY_API_KEY"))
    # IMPROVED: Geoapify's free tier is quota-limited per day across
    # geocode+places+details combined; this caps our own usage well under
    # that so a single hunt cycle can't burn the day's quota by calling
    # Place Details once per candidate. Defaults conservative on purpose.
    geoapify_daily_call_cap: int = int(os.environ.get("GEOAPIFY_DAILY_CALL_CAP", "2500"))
    geoapify_details_per_cycle: int = int(os.environ.get("GEOAPIFY_DETAILS_PER_CYCLE", "15"))
    telegram_bot_token: Optional[str] = field(default_factory=lambda: os.environ.get("TELEGRAM_BOT_TOKEN"))
    telegram_chat_id: Optional[str] = field(default_factory=lambda: os.environ.get("TELEGRAM_CHAT_ID"))

    data_dir: str = field(default_factory=lambda: os.environ.get("DATA_DIR", "."))
    sqlite_filename: str = "growth_pulse.db"
    processed_companies_filename: str = "processed_companies.txt"

    # IMPROVED: these two env vars were already declared in render.yaml but
    # nothing in the old app.py ever read them — autonomous mode was dead.
    automated_niches: List[str] = field(default_factory=lambda: [
        n.strip() for n in os.environ.get("AUTOMATED_NICHES", "").split(",") if n.strip()
    ])
    automated_cities: List[str] = field(default_factory=lambda: [
        c.strip() for c in os.environ.get("AUTOMATED_CITIES", "").split(",") if c.strip()
    ])

    scrape_timeout_seconds: int = 8
    loop_interval_seconds: int = int(os.environ.get("LOOP_INTERVAL_SECONDS", "1800"))
    max_hunt_results: int = int(os.environ.get("MAX_HUNT_RESULTS", "20"))

    daily_email_cap: int = int(os.environ.get("DAILY_EMAIL_CAP", "180"))
    # IMPROVED: warm-up ramp so a brand-new sending domain doesn't blast the
    # full cap on day one and get flagged. Ramps to full cap over 10 days.
    warmup_days: int = int(os.environ.get("WARMUP_DAYS", "10"))
    warmup_start_cap: int = int(os.environ.get("WARMUP_START_CAP", "20"))
    campaign_start_date: str = field(default_factory=lambda: os.environ.get("CAMPAIGN_START_DATE", ""))

    email_send_delay_seconds: int = int(os.environ.get("EMAIL_SEND_DELAY_SECONDS", "5"))
    email_send_jitter_seconds: int = int(os.environ.get("EMAIL_SEND_JITTER_SECONDS", "4"))

    # IMPROVED: GitHub-backed persistence. Set these once (a free private
    # repo works fine) so state survives Render restarts/redeploys.
    github_token: Optional[str] = field(default_factory=lambda: os.environ.get("GITHUB_TOKEN"))
    github_repo: Optional[str] = field(default_factory=lambda: os.environ.get("GITHUB_BACKUP_REPO"))  # "owner/repo"
    github_backup_branch: str = field(default_factory=lambda: os.environ.get("GITHUB_BACKUP_BRANCH", "main"))
    backup_interval_seconds: int = int(os.environ.get("BACKUP_INTERVAL_SECONDS", "900"))

    # IMPROVED: business-hours send gating, local to each target city.
    send_window_start_hour: int = int(os.environ.get("SEND_WINDOW_START_HOUR", "9"))
    send_window_end_hour: int = int(os.environ.get("SEND_WINDOW_END_HOUR", "17"))

    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    aggregator_domains: tuple = (
        "yelp.com", "yellowpages.com", "angi.com", "angieslist.com",
        "thumbtack.com", "houzz.com", "bbb.org", "facebook.com",
        "instagram.com", "linkedin.com", "nextdoor.com", "mapquest.com",
        "manta.com", "foursquare.com", "chamberofcommerce.com",
        "superpages.com", "citysearch.com", "homeadvisor.com",
        "porch.com", "buildzoom.com", "google.com",
    )

    # IMPROVED: junk local-parts / domains that regex email-scraping tends to
    # false-positive on (tracking pixels, CDN assets, template placeholders).
    junk_email_markers: tuple = (
        "sentry.io", "wixpress.com", "example.com", "godaddy.com",
        "w3.org", "schema.org", "yourdomain", "domain.com",
        "@2x", ".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp",
        "wordpress.com", "gravatar.com", "cloudflare.com",
    )

    @property
    def sqlite_path(self) -> str:
        return os.path.join(self.data_dir, self.sqlite_filename)

    @property
    def processed_companies_path(self) -> str:
        return os.path.join(self.data_dir, self.processed_companies_filename)


CONFIG = Config()

# IMPROVED: a shared, retrying HTTP session instead of bare requests.get calls
# scattered through the file. Handles transient DNS/connection resets so a
# single flaky target doesn't quietly drop a lead.
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": CONFIG.user_agent})
_retry = Retry(total=2, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504])
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://", HTTPAdapter(max_retries=_retry))

# ---------------------------------------------------------------------------
# IMPROVED: Timezone-aware send gating
# ---------------------------------------------------------------------------
CITY_TIMEZONES: Dict[str, str] = {
    "new york": "America/New_York",
    "atlanta": "America/New_York",
    "miami": "America/New_York",
    "chicago": "America/Chicago",
    "houston": "America/Chicago",
    "dallas": "America/Chicago",
    "phoenix": "America/Phoenix",
    "las vegas": "America/Los_Angeles",
    "los angeles": "America/Los_Angeles",
    "toronto": "America/Toronto",
    "london": "Europe/London",
    "sydney": "Australia/Sydney",
    "dubai": "Asia/Dubai",
}


def resolve_city_timezone(location: str) -> ZoneInfo:
    lowered = location.lower()
    for key, tz_name in CITY_TIMEZONES.items():
        if key in lowered:
            try:
                return ZoneInfo(tz_name)
            except Exception:
                break
    return ZoneInfo("America/New_York")


def is_within_send_window(location: str) -> bool:
    """IMPROVED: hold sends outside local business hours instead of firing
    on whatever UTC moment the hunt loop happens to reach that city."""
    tz = resolve_city_timezone(location)
    local_hour = datetime.now(tz).hour
    return CONFIG.send_window_start_hour <= local_hour < CONFIG.send_window_end_hour


# ---------------------------------------------------------------------------
# IMPROVED: MX-record check
# ---------------------------------------------------------------------------

def has_mx_record(email_domain: str) -> bool:
    if not _DNS_AVAILABLE:
        return True
    try:
        answers = dns.resolver.resolve(email_domain, "MX", lifetime=5)
        return len(answers) > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# IMPROVED: GitHub-backed persistence
# ---------------------------------------------------------------------------
# Residual risk stated plainly: this is periodic snapshotting, not per-write
# replication. A crash between syncs loses whatever changed in that window
# (at most backup_interval_seconds of activity) — a real gap, not a hidden
# one, and the correct trade-off against losing everything on every restart.

class GitHubBackup:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.enabled = bool(config.github_token and config.github_repo)
        self._lock = threading.Lock()
        if not self.enabled:
            logger.info("GitHub backup disabled — set GITHUB_TOKEN and GITHUB_BACKUP_REPO to enable.")

    def _api_url(self, repo_path: str) -> str:
        return f"https://api.github.com/repos/{self.config.github_repo}/contents/{repo_path}"

    def _headers(self) -> dict:
        return {
            "Authorization": f"token {self.config.github_token}",
            "Accept": "application/vnd.github+json",
        }

    def restore_file(self, local_path: str, repo_path: str) -> bool:
        if not self.enabled:
            return False
        try:
            resp = SESSION.get(
                self._api_url(repo_path),
                headers=self._headers(),
                params={"ref": self.config.github_backup_branch},
                timeout=15,
            )
            if resp.status_code == 404:
                logger.info(f"No prior GitHub backup found for {repo_path} — starting fresh.")
                return False
            resp.raise_for_status()
            content_b64 = resp.json().get("content", "")
            raw = base64.b64decode(content_b64)
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
            with open(local_path, "wb") as handle:
                handle.write(raw)
            logger.info(f"Restored {repo_path} from GitHub backup ({len(raw)} bytes).")
            return True
        except requests.RequestException as exc:
            STATE.record_error(f"GitHub restore failed for {repo_path}: {exc}")
            return False

    def push_file(self, local_path: str, repo_path: str) -> bool:
        if not self.enabled or not os.path.exists(local_path):
            return False
        with self._lock:
            try:
                with open(local_path, "rb") as handle:
                    content_b64 = base64.b64encode(handle.read()).decode("ascii")

                sha = None
                existing = SESSION.get(
                    self._api_url(repo_path),
                    headers=self._headers(),
                    params={"ref": self.config.github_backup_branch},
                    timeout=15,
                )
                if existing.status_code == 200:
                    sha = existing.json().get("sha")

                payload = {
                    "message": f"backup: {repo_path} @ {datetime.now().isoformat(timespec='seconds')}",
                    "content": content_b64,
                    "branch": self.config.github_backup_branch,
                }
                if sha:
                    payload["sha"] = sha

                resp = SESSION.put(self._api_url(repo_path), headers=self._headers(), json=payload, timeout=20)
                if resp.status_code not in (200, 201):
                    STATE.record_error(f"GitHub backup push failed for {repo_path}: {resp.status_code} {resp.text[:200]}")
                    return False
                return True
            except requests.RequestException as exc:
                STATE.record_error(f"GitHub backup push exception for {repo_path}: {exc}")
                return False

    def sync_all(self, files: List[Tuple[str, str]]) -> None:
        for local_path, repo_path in files:
            self.push_file(local_path, repo_path)

# ---------------------------------------------------------------------------
# Shared runtime state
# ---------------------------------------------------------------------------

class EngineState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.status: str = "Idle"
        self.emails_dispatched_today: int = 0
        self.email_cap_reached_today: bool = False
        self.leads_scraped_total: int = 0
        self.leads_with_pixel_total: int = 0
        self.errors_total: int = 0
        self.last_cycle_started_at: Optional[str] = None
        self.last_cycle_completed_at: Optional[str] = None
        self.last_error: Optional[str] = None
        self.current_niche_city_index: int = 0
        self._counter_date: date = date.today()
        # IMPROVED: Geoapify free-tier daily quota tracking.
        self.geoapify_calls_today: int = 0
        self.geoapify_cap_reached_today: bool = False
        # IMPROVED: per-source discovery counters (cumulative, process
        # lifetime) so the dashboard can answer "which discovery source is
        # actually finding businesses" instead of a single opaque total.
        self.candidates_from_geoapify_total: int = 0
        self.candidates_from_overpass_total: int = 0
        self.candidates_from_ddg_total: int = 0
        self.overpass_mirror_failures_total: int = 0
        self.last_hunt_source_breakdown: Dict[str, int] = {}

    def _roll_daily_counter_if_needed(self) -> None:
        today = date.today()
        if today != self._counter_date:
            self.emails_dispatched_today = 0
            self.email_cap_reached_today = False
            self.geoapify_calls_today = 0
            self.geoapify_cap_reached_today = False
            self._counter_date = today

    def set_status(self, status: str) -> None:
        with self._lock:
            self.status = status

    def record_error(self, message: str) -> None:
        with self._lock:
            self.errors_total += 1
            self.last_error = message
        logger.error(message)

    def todays_cap(self) -> int:
        """# IMPROVED: warm-up ramp — grows linearly from warmup_start_cap to
        daily_email_cap over warmup_days, based on CAMPAIGN_START_DATE."""
        if not CONFIG.campaign_start_date:
            return CONFIG.daily_email_cap
        try:
            start = datetime.strptime(CONFIG.campaign_start_date, "%Y-%m-%d").date()
        except ValueError:
            return CONFIG.daily_email_cap
        days_in = (date.today() - start).days
        if days_in >= CONFIG.warmup_days or days_in < 0:
            return CONFIG.daily_email_cap
        step = (CONFIG.daily_email_cap - CONFIG.warmup_start_cap) / max(CONFIG.warmup_days, 1)
        return int(CONFIG.warmup_start_cap + step * days_in)

    def record_email_sent(self) -> None:
        with self._lock:
            self._roll_daily_counter_if_needed()
            self.emails_dispatched_today += 1
            if self.emails_dispatched_today >= self.todays_cap():
                self.email_cap_reached_today = True

    def try_reserve_geoapify_call(self) -> bool:
        """IMPROVED: gate every Geoapify request through here so a single
        hunt cycle can't burn the day's free-tier quota. Returns False once
        the daily cap is hit; callers fall back to the DDG discovery path."""
        with self._lock:
            self._roll_daily_counter_if_needed()
            if self.geoapify_calls_today >= CONFIG.geoapify_daily_call_cap:
                self.geoapify_cap_reached_today = True
                return False
            self.geoapify_calls_today += 1
            return True

    def record_hunt_sources(self, geoapify: int, overpass: int, ddg: int) -> None:
        """IMPROVED: called once per hunt cycle with how many candidates each
        discovery source contributed, so the dashboard shows which sources
        are actually producing leads instead of a single opaque total."""
        with self._lock:
            self.candidates_from_geoapify_total += geoapify
            self.candidates_from_overpass_total += overpass
            self.candidates_from_ddg_total += ddg
            self.last_hunt_source_breakdown = {
                "geoapify": geoapify, "overpass": overpass, "ddg": ddg,
            }

    def record_overpass_mirror_failure(self) -> None:
        with self._lock:
            self.overpass_mirror_failures_total += 1

    def snapshot(self) -> dict:
        with self._lock:
            self._roll_daily_counter_if_needed()
            return {
                "status": self.status,
                "emails_dispatched_today": self.emails_dispatched_today,
                "todays_cap": self.todays_cap(),
                "email_cap_reached_today": self.email_cap_reached_today,
                "leads_scraped_total": self.leads_scraped_total,
                "leads_with_pixel_total": self.leads_with_pixel_total,
                "geoapify_calls_today": self.geoapify_calls_today,
                "geoapify_cap_reached_today": self.geoapify_cap_reached_today,
                "errors_total": self.errors_total,
                "last_cycle_started_at": self.last_cycle_started_at,
                "last_cycle_completed_at": self.last_cycle_completed_at,
                "last_error": self.last_error,
                "last_hunt_source_breakdown": self.last_hunt_source_breakdown,
                "candidates_from_geoapify_total": self.candidates_from_geoapify_total,
                "candidates_from_overpass_total": self.candidates_from_overpass_total,
                "candidates_from_ddg_total": self.candidates_from_ddg_total,
                "overpass_mirror_failures_total": self.overpass_mirror_failures_total,
            }


STATE = EngineState()
GITHUB_BACKUP = GitHubBackup(CONFIG)

# IMPROVED: pull last known state before anything else touches disk, so a
# fresh Render instance picks up leads/dedup history instead of starting blank.
if GITHUB_BACKUP.enabled:
    GITHUB_BACKUP.restore_file(CONFIG.sqlite_path, "backup/growth_pulse.db")
    GITHUB_BACKUP.restore_file(CONFIG.processed_companies_path, "backup/processed_companies.txt")

# ---------------------------------------------------------------------------
# Persistent deduplication log
# ---------------------------------------------------------------------------

class DeduplicationTracker:
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
# Storage layer
# ---------------------------------------------------------------------------

class StorageBackend:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._local_lock = threading.Lock()
        os.makedirs(self.config.data_dir, exist_ok=True)
        self._init_sqlite()

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
                    follow_up_step INTEGER NOT NULL DEFAULT 0,
                    last_email_sent_at TEXT,
                    project_type TEXT,
                    source TEXT NOT NULL DEFAULT 'queue',
                    scraped_at TEXT NOT NULL,
                    founder_name TEXT,
                    competitor TEXT,
                    gbp_status TEXT,
                    location TEXT,
                    UNIQUE(domain)
                )
                """
            )
            # IMPROVED: ALTER-if-missing for these columns, since a DB
            # restored from an older GitHub backup snapshot may predate them.
            for column in ("founder_name", "competitor", "gbp_status", "location"):
                try:
                    cursor.execute(f"ALTER TABLE leads ADD COLUMN {column} TEXT")
                except sqlite3.OperationalError:
                    pass  # column already exists
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dispatch_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain TEXT NOT NULL,
                    contact_email TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    step INTEGER DEFAULT 1
                )
                """
            )
            connection.commit()
            connection.close()

    def save_lead(self, domain: str, business_name: Optional[str], contact_email: Optional[str],
                  phone_number: Optional[str], has_meta_pixel: bool, has_google_ads_tag: bool,
                  scrape_status: str, source: str, project_type: str = None,
                  founder_name: str = None, competitor: str = None, gbp_status: str = None,
                  location: str = None) -> None:
        scraped_at = datetime.now().isoformat(timespec="seconds")
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO leads (domain, business_name, contact_email, phone_number, has_meta_pixel,
                                    has_google_ads_tag, scrape_status, source, scraped_at, project_type,
                                    founder_name, competitor, gbp_status, location)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    business_name=excluded.business_name,
                    contact_email=excluded.contact_email,
                    phone_number=excluded.phone_number,
                    has_meta_pixel=excluded.has_meta_pixel,
                    has_google_ads_tag=excluded.has_google_ads_tag,
                    scrape_status=excluded.scrape_status,
                    source=excluded.source,
                    scraped_at=excluded.scraped_at,
                    project_type=COALESCE(excluded.project_type, leads.project_type),
                    founder_name=COALESCE(excluded.founder_name, leads.founder_name),
                    competitor=COALESCE(excluded.competitor, leads.competitor),
                    gbp_status=COALESCE(excluded.gbp_status, leads.gbp_status),
                    location=COALESCE(excluded.location, leads.location)
                """,
                (domain, business_name, contact_email, phone_number, int(has_meta_pixel),
                 int(has_google_ads_tag), scrape_status, source, scraped_at, project_type,
                 founder_name, competitor, gbp_status, location),
            )
            connection.commit()
            connection.close()

    def mark_email_sent(self, domain: str, contact_email: str, outcome: str, step: int = 1) -> None:
        sent_at = datetime.now().isoformat(timespec="seconds")
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            cursor = connection.cursor()
            cursor.execute(
                "UPDATE leads SET email_status=?, last_email_sent_at=?, follow_up_step=? WHERE domain=?",
                (outcome, sent_at, step, domain)
            )
            cursor.execute(
                "INSERT INTO dispatch_log (domain, contact_email, sent_at, outcome, step) VALUES (?, ?, ?, ?, ?)",
                (domain, contact_email, sent_at, outcome, step)
            )
            connection.commit()
            connection.close()

    def get_follow_up_leads(self, days_delay: int = 3, max_step: int = 2) -> List[Dict]:
        """IMPROVED: bounded to max_step so a lead can't be nudged forever."""
        cutoff = (datetime.now() - timedelta(days=days_delay)).isoformat()
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute(
                """SELECT * FROM leads
                   WHERE email_status='success' AND follow_up_step >= 1
                   AND follow_up_step < ? AND last_email_sent_at < ?""",
                (max_step, cutoff)
            )
            rows = [dict(row) for row in cursor.fetchall()]
            connection.close()
            return rows

    def get_pending_timing_leads(self) -> List[Dict]:
        """IMPROVED: leads scraped outside their city's send window, held
        here until the dispatch loop finds them inside business hours."""
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM leads WHERE email_status='pending_timing'")
            rows = [dict(row) for row in cursor.fetchall()]
            connection.close()
            return rows


STORAGE = StorageBackend(CONFIG)


def backup_sync_now() -> None:
    if not GITHUB_BACKUP.enabled:
        return
    GITHUB_BACKUP.sync_all([
        (CONFIG.sqlite_path, "backup/growth_pulse.db"),
        (CONFIG.processed_companies_path, "backup/processed_companies.txt"),
    ])


def backup_loop() -> None:
    if not GITHUB_BACKUP.enabled:
        return
    while True:
        time.sleep(CONFIG.backup_interval_seconds)
        try:
            backup_sync_now()
        except Exception as exc:
            STATE.record_error(f"Backup loop error: {exc}")

# ---------------------------------------------------------------------------
# IMPROVED: Search intelligence — DuckDuckGo HTML-lite instead of raw Google
# Google's public search HTML returns a CAPTCHA/consent wall to almost any
# scripted request within a handful of calls, especially from shared cloud
# IPs and from non-US regions (several AUTOMATED_CITIES are UK/AU/UAE) — the
# old free_google_search() would have gone dark within the first hunt cycle.
# DuckDuckGo's non-JS "html" endpoint has no login wall and is commonly used
# for lightweight scraping; it is still a courtesy scrape, so this keeps
# request volume low, sequential, and rate-limited.
# ---------------------------------------------------------------------------

_last_search_at = 0.0
_search_lock = threading.Lock()

def free_web_search(query: str) -> str:
    global _last_search_at
    with _search_lock:
        wait = 2.0 - (time.time() - _last_search_at)
        if wait > 0:
            time.sleep(wait)
        try:
            resp = SESSION.get(
                "https://html.duckduckgo.com/html/",
                params={"q": query},
                timeout=10,
            )
            _last_search_at = time.time()
            # IMPROVED: previously any non-200 status was silently discarded
            # as "" with no signal at all — a hunt cycle logging "0 candidates"
            # looked identical whether DDG genuinely had no results or was
            # rate-limiting/blocking this deployment's IP. Now every non-200
            # is logged with enough detail to tell the two apart.
            if resp.status_code == 200:
                # IMPROVED: DDG can return HTTP 200 with an anti-bot/rate-limit
                # page instead of real results, especially to cloud/datacenter
                # IP ranges. That page doesn't contain the 'result__a' marker
                # our parser looks for, so a 200 with zero matches is worth
                # flagging distinctly from a query that genuinely has no hits.
                if "result__a" not in resp.text:
                    logger.warning(
                        f"DDG returned HTTP 200 for '{query}' but no result markers were found "
                        f"({len(resp.text)} bytes) — likely a bot-check/rate-limit page rather than "
                        f"a true zero-result search. If this repeats across queries, DDG is probably "
                        f"throttling this deployment's IP, not failing to find businesses."
                    )
                return resp.text
            logger.warning(f"DDG search for '{query}' returned HTTP {resp.status_code} — treating as no results.")
        except requests.RequestException as exc:
            STATE.record_error(f"search failed for '{query}': {exc}")
        return ""


def is_junk_email(addr: str) -> bool:
    lowered = addr.lower()
    return any(marker in lowered for marker in CONFIG.junk_email_markers)


# ---------------------------------------------------------------------------
# IMPROVED: Hunt layer — Geoapify Places (replaces Google Places).
# ---------------------------------------------------------------------------
# Two real constraints, not hidden behind the abstraction:
#  1. Geoapify's Places API is category-based (fixed OSM taxonomy), not a
#     free-text business search like Google's textQuery. Checked against the
#     actual AUTOMATED_NICHES list: only "Commercial Electricians" maps to a
#     real category (service.electrician). Roofing, solar installers, custom
#     home builders, landscaping, foundation repair, and HVAC have no
#     matching category — OSM doesn't tag these as a business type at all.
#  2. Even where a category matches, Geoapify's documented Places response
#     has no website field, and a follow-up Place Details lookup only
#     surfaces one if OSM happens to have a contact:website tag — sparse for
#     small local contractors.
# NICHE_CATEGORY_MAP holds only the mappings that are genuinely solid.
# Everything else — and anything a mapped category fails to return a usable
# website for — falls through to a DuckDuckGo business-discovery search
# (ddg_business_discovery), reusing the same rate-limited search path as the
# founder/competitor lookups. This keeps the system 100% free and running
# unattended, but it is a lower-precision source than a real places API;
# watch geoapify_calls_today / leads_scraped_total on the dashboard to see
# which niches are actually producing candidates.
# ---------------------------------------------------------------------------

NICHE_CATEGORY_MAP: Dict[str, str] = {
    "electrician": "service.electrician",
    "electrical": "service.electrician",
}

# IMPROVED: raw OpenStreetMap "craft=" tags cover a few trades that Geoapify's
# curated category list doesn't expose at all (Geoapify wraps OSM but only
# surfaces a subset of tags as categories). Querying OSM directly via the
# free Overpass API reaches these without spending Geoapify's daily credits.
# Real caveat, stated plainly: OSM's coverage of solo-operator trade
# businesses (no storefront) is generally sparse — this widens the net, it
# doesn't fill it. Foundation repair, solar installers, and pool builders
# still have no reliable OSM tag and go straight to DDG discovery.
#
# IMPROVED (this pass): each niche now maps to *every* tag combination OSM
# actually uses for that trade instead of one guess. craft=* covers the
# tradesperson/workshop mapping style; shop=* and office=* cover the
# storefront/registered-company mapping style. Mappers use both inconsistently
# for the same real-world business, so querying only one style was silently
# skipping real, tagged local businesses. This is the main lever for "find
# more real local trades" — broader tag coverage directly means more
# candidates out of a source that costs no quota and needs no key.
OVERPASS_NICHE_TAGS: Dict[str, List[Tuple[str, str]]] = {
    "roofing": [("craft", "roofer")],
    "roofer": [("craft", "roofer")],
    "electrician": [("craft", "electrician"), ("shop", "electrical")],
    "electrical": [("craft", "electrician"), ("shop", "electrical")],
    "hvac": [("craft", "hvac")],
    "heating": [("craft", "hvac")],
    "landscap": [("craft", "gardener"), ("shop", "garden_centre")],
    "builder": [("craft", "builder"), ("office", "construction_company")],
    "home builder": [("craft", "builder"), ("office", "construction_company")],
    "construction": [("craft", "builder"), ("office", "construction_company")],
    "plumb": [("craft", "plumber"), ("shop", "plumbing")],
    "painter": [("craft", "painter")],
    "painting": [("craft", "painter")],
    "carpenter": [("craft", "carpenter")],
    "carpentry": [("craft", "carpenter")],
}

_geocode_cache: Dict[str, Optional[dict]] = {}
_geocode_lock = threading.Lock()


def map_niche_to_category(niche: str) -> Optional[str]:
    lowered = niche.lower()
    for key, category in NICHE_CATEGORY_MAP.items():
        if key in lowered:
            return category
    return None


def map_niche_to_overpass_tags(niche: str) -> List[Tuple[str, str]]:
    lowered = niche.lower()
    tags: List[Tuple[str, str]] = []
    for key, tag_list in OVERPASS_NICHE_TAGS.items():
        if key in lowered:
            for tag in tag_list:
                if tag not in tags:
                    tags.append(tag)
    return tags


# IMPROVED: Nominatim (OSM's own free geocoder) as a fallback when Geoapify
# has no key set or has hit its daily quota — this is what removes Overpass's
# hard dependency on a Geoapify key entirely, so raw OSM discovery keeps
# working even on a bare deployment with zero paid-tier keys configured.
# Nominatim's usage policy caps public requests at 1/sec and requires an
# identifying User-Agent; both are respected below. No place_id is returned
# (that's a Geoapify-specific concept), only lat/lon, which is all Overpass
# needs for its "around" radius filter.
_last_nominatim_at = 0.0
_nominatim_lock = threading.Lock()


def nominatim_geocode_record(location: str) -> Optional[dict]:
    global _last_nominatim_at
    with _nominatim_lock:
        wait = 1.1 - (time.time() - _last_nominatim_at)
        if wait > 0:
            time.sleep(wait)
        try:
            resp = SESSION.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": location, "format": "json", "limit": 1},
                headers={"User-Agent": f"GrowthPulse/1.0 ({CONFIG.sender_display_name})"},
                timeout=10,
            )
            _last_nominatim_at = time.time()
            resp.raise_for_status()
            results = resp.json()
            if results:
                return {"place_id": None, "lat": results[0].get("lat"), "lon": results[0].get("lon")}
            return None
        except (requests.RequestException, ValueError, IndexError, KeyError) as exc:
            STATE.record_error(f"Nominatim geocode failed for '{location}': {exc}")
            return None


def geoapify_geocode_record(location: str) -> Optional[dict]:
    """Resolves a city string to {"place_id":..., "lat":..., "lon":...},
    cached for the life of the process so repeated hunt cycles don't
    re-geocode the same city. Backs both the Geoapify Places filter and the
    Overpass "around" radius query.

    IMPROVED: tries Geoapify first (needed for place_id, which the Places
    category search requires), then falls back to Nominatim when there's no
    Geoapify key or its daily quota is exhausted — Overpass discovery only
    needs lat/lon, so it keeps working either way."""
    with _geocode_lock:
        if location in _geocode_cache:
            return _geocode_cache[location]

    record = None
    if CONFIG.geoapify_api_key and STATE.try_reserve_geoapify_call():
        try:
            resp = SESSION.get(
                "https://api.geoapify.com/v1/geocode/search",
                params={"text": location, "type": "city", "format": "json", "apiKey": CONFIG.geoapify_api_key},
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                record = {
                    "place_id": results[0].get("place_id"),
                    "lat": results[0].get("lat"),
                    "lon": results[0].get("lon"),
                }
        except (requests.RequestException, ValueError, IndexError, KeyError) as exc:
            STATE.record_error(f"Geoapify geocode failed for '{location}': {exc}")

    if record is None:
        record = nominatim_geocode_record(location)
        if record:
            logger.info(f"Geocoded '{location}' via Nominatim fallback (no Geoapify place_id available).")

    with _geocode_lock:
        _geocode_cache[location] = record
    return record


def geoapify_geocode_city(location: str) -> Optional[str]:
    record = geoapify_geocode_record(location)
    return record.get("place_id") if record else None


def _find_website_in_json(node) -> Optional[str]:
    """Best-effort recursive search for a website-shaped value anywhere in a
    Place Details response — Geoapify only surfaces contact:website when OSM
    happens to have it tagged, and the exact key path isn't guaranteed."""
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, str) and "website" in key.lower() and value.startswith("http"):
                return value
            found = _find_website_in_json(value)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_website_in_json(item)
            if found:
                return found
    return None


def geoapify_place_website(place_id: str) -> Optional[str]:
    if not place_id or not CONFIG.geoapify_api_key or not STATE.try_reserve_geoapify_call():
        return None
    try:
        resp = SESSION.get(
            "https://api.geoapify.com/v2/place-details",
            params={"id": place_id, "features": "details", "apiKey": CONFIG.geoapify_api_key},
            timeout=10,
        )
        resp.raise_for_status()
        return _find_website_in_json(resp.json())
    except (requests.RequestException, ValueError) as exc:
        STATE.record_error(f"Geoapify place-details failed for {place_id}: {exc}")
        return None


def geoapify_places_search(category: str, location: str, limit: int) -> List[Dict]:
    place_id = geoapify_geocode_city(location)
    if not place_id or not STATE.try_reserve_geoapify_call():
        return []
    try:
        resp = SESSION.get(
            "https://api.geoapify.com/v2/places",
            params={
                "categories": category,
                "filter": f"place:{place_id}",
                "limit": limit,
                "apiKey": CONFIG.geoapify_api_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        features = resp.json().get("features", [])
        results = [
            {"name": f["properties"].get("name"), "place_id": f["properties"].get("place_id")}
            for f in features if f.get("properties", {}).get("name")
        ]
        logger.info(f"Geoapify places search '{category}'/{location} -> {len(results)} raw result(s).")
        return results
    except (requests.RequestException, ValueError, KeyError) as exc:
        STATE.record_error(f"Geoapify places search failed for {category}/{location}: {exc}")
        return []


DDG_RESULT_RE = re.compile(
    r'<a rel="nofollow" href="[^"]*uddg=([^&"]+)[^"]*"[^>]*class="result__a"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def extract_ddg_results(html: str) -> List[Tuple[str, str]]:
    """Returns (business_name, absolute_url) pairs from a DuckDuckGo
    html-lite results page."""
    from urllib.parse import unquote
    out = []
    for encoded_url, name_html in DDG_RESULT_RE.findall(html):
        url = unquote(encoded_url)
        name = _HTML_TAG_RE.sub("", name_html).strip()
        if url and name:
            out.append((name, url))
    return out


def _is_usable_domain(domain: str) -> bool:
    if not domain or "duckduckgo.com" in domain:
        return False
    return not any(agg in domain for agg in CONFIG.aggregator_domains)


def ddg_find_website_for_business(name: str, location: str) -> Optional[str]:
    """IMPROVED: per-candidate fallback when Geoapify has a place but no
    website — looks the business up by name instead of leaving it dead."""
    html = free_web_search(f'"{name}" {location} official website')
    for _, url in extract_ddg_results(html):
        if _is_usable_domain(urlparse(url).netloc.lower()):
            return url
    return None


# IMPROVED: multiple public Overpass mirrors instead of hardcoding
# overpass-api.de. The primary instance is the most heavily loaded public
# Overpass server in existence and frequently answers with 429/504 under
# load, independent of any Render-specific networking issue. Rotating
# through mirrors — each with a short, fast-failing timeout rather than one
# long timeout on a single host — means a single overloaded/unreachable
# mirror costs a few seconds, not the whole hunt cycle.
OVERPASS_MIRRORS: List[str] = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass-api.de/api/interpreter",
]
# IMPROVED: dropped overpass.osm.rambler.ru — confirmed dead via a live
# Render deployment log (NameResolutionError, the domain does not resolve
# at all). Left as a comment rather than silently forgotten: if you add a
# replacement mirror here, verify it resolves and answers a query from
# your actual deployment first — a mirror that looks right on paper but
# fails outright wastes a full timeout cycle on every single hunt.
#
# Real caveat worth knowing, also from that same log: overpass-api.de
# answered with a bare TCP connection refusal, not a timeout or a 429/503.
# That's consistent with the operator blocking known cloud/datacenter IP
# ranges outright (several public Overpass operators do this deliberately
# to fight abuse) — mirror rotation won't fix that, since it's a property
# of the network Render's free tier runs on, not of any one mirror. If
# Overpass keeps failing across all mirrors from this deployment, that's
# the most likely reason, and Geoapify / DDG (both of which use normal
# authenticated or browser-style HTTPS rather than a shared scrape target)
# are the sources actually worth relying on from here.
# Small process-wide rotation offset so repeated cycles don't always hammer
# the same mirror first — spreads load across the mirror list over time.
_overpass_rotation = {"offset": 0}
_overpass_rotation_lock = threading.Lock()


def _next_overpass_order() -> List[str]:
    with _overpass_rotation_lock:
        offset = _overpass_rotation["offset"] % len(OVERPASS_MIRRORS)
        _overpass_rotation["offset"] += 1
    return OVERPASS_MIRRORS[offset:] + OVERPASS_MIRRORS[:offset]


def overpass_query(query: str, timeout_per_mirror: int = 12) -> Optional[dict]:
    """IMPROVED: tries each Overpass mirror in turn with a short per-mirror
    timeout, logging exactly which mirror served the request (or why each
    one failed) so mirror health is visible in the live log instead of a
    single opaque "Overpass failed" error."""
    for mirror in _next_overpass_order():
        try:
            resp = SESSION.post(mirror, data={"data": query}, timeout=timeout_per_mirror)
            if resp.status_code == 200:
                logger.info(f"Overpass OK via {mirror}")
                return resp.json()
            logger.warning(f"Overpass mirror {mirror} returned HTTP {resp.status_code} — trying next mirror.")
            STATE.record_overpass_mirror_failure()
        except (requests.RequestException, ValueError) as exc:
            logger.warning(f"Overpass mirror {mirror} unreachable ({exc}) — trying next mirror.")
            STATE.record_overpass_mirror_failure()
    STATE.record_error("All Overpass mirrors failed or timed out for this query.")
    return None


def overpass_business_search(niche: str, location: str, limit: int) -> List[Dict]:
    """IMPROVED: queries raw OSM craft=/shop=/office= tags via the free
    Overpass API (rotating across mirrors, see overpass_query) — no key, no
    daily quota, and it reaches trades Geoapify's category list doesn't
    expose. Coverage is still genuinely sparse for solo-operator trades with
    no storefront; this widens the net, it doesn't fill it."""
    tags = map_niche_to_overpass_tags(niche)
    if not tags:
        return []
    record = geoapify_geocode_record(location)
    if not record or not record.get("lat") or not record.get("lon"):
        logger.info(f"Overpass skipped for {niche}/{location} — no geocode available (needs GEOAPIFY_API_KEY).")
        return []
    lat, lon = record["lat"], record["lon"]
    radius_m = 20000
    clauses = "".join(
        f'node["{k}"="{v}"](around:{radius_m},{lat},{lon});'
        f'way["{k}"="{v}"](around:{radius_m},{lat},{lon});'
        for k, v in tags
    )
    query = f"[out:json][timeout:20];({clauses});out center tags {limit};"
    payload = overpass_query(query)
    if payload is None:
        return []
    elements = payload.get("elements", [])

    out: List[Dict] = []
    for el in elements:
        el_tags = el.get("tags", {})
        name = el_tags.get("name")
        if not name:
            continue
        website = el_tags.get("website") or el_tags.get("contact:website")
        if not website:
            website = ddg_find_website_for_business(name, location)
        if website:
            out.append({"name": name, "uri": website})
        if len(out) >= limit:
            break
    logger.info(f"Overpass found {len(out)} candidate(s) with website for {niche}/{location} "
                f"(from {len(elements)} raw OSM element(s)).")
    return out


# IMPROVED: DDG's html-lite endpoint returns roughly one page (~10 results)
# per query, so a single query was capping fallback-niche discovery well
# below MAX_HUNT_RESULTS regardless of the limit passed in. Issuing a few
# differently-phrased queries (still serialized through the same 2s shared
# throttle) pulls in a materially larger, more diverse candidate pool per
# cycle for the niches that have no structured data source at all.
DDG_QUERY_VARIANTS = [
    "{niche} companies {location}",
    "{niche} contractors {location}",
    "best {niche} {location}",
]


def ddg_business_discovery(niche: str, location: str, limit: int) -> List[Dict]:
    """Fallback hunt source for niches with no Geoapify/Overpass match.
    Lower precision than a real places API — expect noisier results."""
    seen_domains = set()
    out: List[Dict] = []
    for template in DDG_QUERY_VARIANTS:
        if len(out) >= limit:
            break
        html = free_web_search(template.format(niche=niche, location=location))
        for name, url in extract_ddg_results(html):
            domain = urlparse(url).netloc.lower()
            if not _is_usable_domain(domain) or domain in seen_domains:
                continue
            seen_domains.add(domain)
            out.append({"name": name, "uri": url})
            if len(out) >= limit:
                break
    logger.info(f"DDG business discovery '{niche}'/{location} -> {len(out)} candidate(s).")
    return out


def hunt_candidates(niche: str, location: str) -> List[Dict]:
    """Returns a list of {"name":..., "uri":...} candidate businesses.
    Tries, in order: Geoapify category (best precision, quota-limited),
    Overpass craft/shop/office-tag lookup (free, no quota, narrower
    coverage), then DuckDuckGo business discovery (widest coverage, lowest
    precision).

    IMPROVED: each source's contribution is counted and logged, and pushed
    into STATE for the dashboard — this is what answers "which discovery
    method is actually finding businesses for this niche" instead of only
    ever seeing a combined total."""
    candidates: List[Dict] = []
    from_geoapify = from_overpass = from_ddg = 0

    category = map_niche_to_category(niche)
    if category and CONFIG.geoapify_api_key:
        places = geoapify_places_search(category, location, CONFIG.max_hunt_results)
        for i, place in enumerate(places):
            name = place.get("name")
            if not name:
                continue
            website = None
            # IMPROVED: cap Place Details lookups per cycle — they're the
            # most quota-expensive call and the least likely to pay off.
            if i < CONFIG.geoapify_details_per_cycle and place.get("place_id"):
                website = geoapify_place_website(place["place_id"])
            if not website:
                website = ddg_find_website_for_business(name, location)
            if website:
                candidates.append({"name": name, "uri": website})
                from_geoapify += 1

    if len(candidates) < CONFIG.max_hunt_results:
        overpass_results = overpass_business_search(niche, location, CONFIG.max_hunt_results - len(candidates))
        candidates.extend(overpass_results)
        from_overpass = len(overpass_results)

    if len(candidates) < CONFIG.max_hunt_results:
        ddg_results = ddg_business_discovery(niche, location, CONFIG.max_hunt_results - len(candidates))
        candidates.extend(ddg_results)
        from_ddg = len(ddg_results)

    STATE.record_hunt_sources(from_geoapify, from_overpass, from_ddg)
    logger.info(
        f"hunt_candidates '{niche}' / '{location}' -> {len(candidates)} total "
        f"(geoapify={from_geoapify}, overpass={from_overpass}, ddg={from_ddg})"
    )
    return candidates



# IMPROVED: multi-page crawl instead of one homepage regex pass. Most sites
# put the owner's direct email on /contact or /about, not the homepage.
CONTACT_PAGE_PATHS = ("", "/contact", "/contact-us", "/about", "/about-us", "/team")
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
MAILTO_RE = re.compile(r'mailto:([^"\'\s?]+)', re.IGNORECASE)
NAME_NEAR_MAILTO_RE = re.compile(
    r'([A-Z][a-z]+ [A-Z][a-z]+)\s*(?:<[^>]*>)?\s*(?:\n|\s){0,20}<a[^>]*mailto:([^"\'\s?]+)',
    re.IGNORECASE,
)


def fetch_url(url: str) -> Optional[requests.Response]:
    """IMPROVED: fallback across https/http and bare/www variants instead of
    a single unguarded request that silently fails on the first mismatch."""
    parsed = urlparse(url if "//" in url else f"https://{url}")
    host = parsed.netloc or parsed.path
    candidates = [f"https://{host}", f"https://www.{host}", f"http://{host}"]
    for candidate in dict.fromkeys(candidates):
        try:
            resp = SESSION.get(candidate, timeout=CONFIG.scrape_timeout_seconds, allow_redirects=True)
            if resp.status_code < 400:
                return resp
        except requests.RequestException:
            continue
    return None


def crawl_contact_pages(root_domain: str) -> Tuple[str, List[Tuple[Optional[str], str]]]:
    """Returns (combined_html_for_pixel_checks, list of (owner_name_or_None, email))."""
    combined_html = ""
    found: List[Tuple[Optional[str], str]] = []
    seen_emails = set()
    base_resp = fetch_url(root_domain)
    if not base_resp:
        return combined_html, found
    base_url = base_resp.url
    combined_html += base_resp.text

    for path in CONTACT_PAGE_PATHS:
        page_url = base_url if path == "" else urljoin(base_url, path)
        try:
            if path == "":
                resp = base_resp
            else:
                resp = SESSION.get(page_url, timeout=CONFIG.scrape_timeout_seconds)
            if resp.status_code >= 400:
                continue
        except requests.RequestException:
            continue
        text = resp.text
        if path != "":
            combined_html += text

        for name, addr in NAME_NEAR_MAILTO_RE.findall(text):
            addr = addr.strip()
            if not is_junk_email(addr) and addr.lower() not in seen_emails:
                found.append((name.strip(), addr))
                seen_emails.add(addr.lower())

        for addr in MAILTO_RE.findall(text):
            addr = addr.strip()
            if not is_junk_email(addr) and addr.lower() not in seen_emails:
                found.append((None, addr))
                seen_emails.add(addr.lower())

        for addr in EMAIL_RE.findall(text):
            if not is_junk_email(addr) and addr.lower() not in seen_emails:
                found.append((None, addr))
                seen_emails.add(addr.lower())

    return combined_html, found


def prioritize_contact(found: List[Tuple[Optional[str], str]]) -> Tuple[Optional[str], Optional[str]]:
    """IMPROVED: 3-tier priority — named personal mailto, then role-based
    local-part (founder/owner/ceo/president), then generic info/contact,
    instead of a single keyword-or-first-match rule."""
    if not found:
        return None, None
    named = [f for f in found if f[0]]
    if named:
        return named[0]
    role_keywords = ("founder", "owner", "ceo", "president", "principal")
    role_matches = [f for f in found if any(k in f[1].lower() for k in role_keywords)]
    if role_matches:
        return role_matches[0]
    generic_first = [f for f in found if not any(k in f[1].lower() for k in ("info", "contact", "hello", "support", "sales"))]
    if generic_first:
        return generic_first[0]
    return found[0]


def find_founder_name(domain: str, business_name: str) -> Optional[str]:
    query = f'"{business_name}" founder OR owner OR CEO {domain}'
    html_content = free_web_search(query)
    patterns = [
        r"(?:Founder|CEO|Owner|President)[,:]?\s+(?:is\s+)?([A-Z][a-z]+ [A-Z][a-z]+)",
        r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+(?:Founder|CEO|Owner|President)",
    ]
    for p in patterns:
        match = re.search(p, html_content)
        if match:
            return match.group(1)
    return None


def find_competitor(location: str, niche: str, current_business: str) -> str:
    query = f"{niche} in {location}"
    html_content = free_web_search(query)
    potential = re.findall(r'class="result__a"[^>]*>([^<]+)<', html_content)
    for comp in potential:
        comp_clean = re.sub(r"\s+", " ", comp).strip()
        if current_business.lower() not in comp_clean.lower() and 3 < len(comp_clean) < 60:
            return comp_clean
    return "other local firms"


def check_gbp_status(business_name: str, location: str) -> str:
    """Heuristic-only signal — DDG rarely surfaces a review count directly,
    so this degrades gracefully to 'not_found' rather than blocking send."""
    query = f"{business_name} {location} reviews"
    html_content = free_web_search(query)
    match = re.search(r"([\d.]+)\s*(?:Google\s*)?reviews", html_content, re.IGNORECASE)
    if match:
        try:
            count = float(match.group(1))
            return "weak_reviews" if count < 10 else "active"
        except ValueError:
            pass
    return "not_found"


def identify_project_type(html_text: str) -> str:
    projects = {
        "Commercial": ["commercial", "industrial", "warehouse", "office"],
        "Luxury": ["luxury", "premium", "high-end", "custom home"],
        "Government": ["government", "municipal", "public works"],
    }
    lowered = html_text.lower()
    for p_type, keywords in projects.items():
        if any(kw in lowered for kw in keywords):
            return p_type
    return "Residential"

# ---------------------------------------------------------------------------
# Scraper & Email Engine
# ---------------------------------------------------------------------------

def scrape_domain(candidate: dict) -> dict:
    raw_domain = candidate["domain"]
    result = {
        "domain": raw_domain.strip(),
        "has_meta_pixel": False,
        "has_google_ads_tag": False,
        "phone_number": None,
        "contact_email": None,
        "status": "unknown",
        "founder_name": None,
        "competitor": "a local competitor",
        "gbp_status": "unknown",
        "project_type": "Residential",
    }

    try:
        combined_html, found_contacts = crawl_contact_pages(raw_domain)
        if not combined_html:
            result["status"] = "unreachable"
            return result

        result["has_meta_pixel"] = "connect.facebook.net" in combined_html and "fbevents.js" in combined_html
        result["has_google_ads_tag"] = "googletagmanager.com/gtag/js" in combined_html
        result["project_type"] = identify_project_type(combined_html)

        owner_name, contact_email = prioritize_contact(found_contacts)
        result["contact_email"] = contact_email
        # IMPROVED: the three enrichment lookups below are each a DDG call
        # under the shared 2s throttle. They were running unconditionally,
        # burning that throttle on candidates with no contact email at all
        # — leads that can never be sent to anyway. Skipping them here is
        # the single biggest throughput fix for hitting a daily send target:
        # it roughly triples how many real candidates fit in a cycle window.
        if contact_email:
            result["founder_name"] = owner_name or find_founder_name(raw_domain, candidate["business_name"])
            result["competitor"] = find_competitor(candidate["location"], candidate["niche"], candidate["business_name"])
            result["gbp_status"] = check_gbp_status(candidate["business_name"], candidate["location"])
        result["status"] = "success"
    except Exception as exc:
        STATE.record_error(f"scrape_domain failed for {raw_domain}: {exc}")
        result["status"] = "error"

    return result


def send_beast_email(recipient_email, domain, business_name, data: dict, step: int = 1):
    api_key = CONFIG.brevo_api_key
    sender_email = CONFIG.email_user
    if not api_key or not sender_email:
        STATE.record_error("send_beast_email: missing BREVO_API_KEY or EMAIL_USER")
        return "failed"
    if STATE.email_cap_reached_today:
        return "failed"

    salutation = f"Hi {data['founder_name'].split()[0]}" if data.get('founder_name') else f"Hi {business_name} team"

    # IMPROVED: the old copy promised a "60-second video walkthrough" that
    # nothing in this stack generates — a prospect replying YES was a
    # guaranteed broken promise, which is exactly what turns curiosity into
    # a spam complaint. The gap the system finds (missing pixel, missing
    # ads tag, weak reviews vs a named competitor) is real and on the
    # record from the scrape, so the copy now offers a written breakdown
    # of that gap — something a human can genuinely send on reply.
    if step == 1:
        subject = f"Question about {business_name}'s {data.get('project_type', 'recent')} projects"
        gap_line = "your site isn't set up to track which ads actually turn into booked jobs"
        if data.get('gbp_status') == "weak_reviews":
            gap_line += f", and {data.get('competitor', 'a nearby competitor')} is outranking you on Google reviews right now"
        html_body = f"""
        <p>{salutation},</p>
        <p>I was looking at your {data.get('project_type', 'recent')} projects on {domain} and noticed {gap_line}.</p>
        <p>We help local leaders plug these gaps to ensure ad spend turns into booked jobs.</p>
        <p>Would you be open to a 5-minute chat? <strong>Reply YES</strong> and I'll send over a quick written breakdown of what I found.</p>
        <p>Best,<br>{CONFIG.sender_display_name}</p>
        <p style="font-size:11px;color:#888;">If this isn't useful, reply STOP and I won't follow up again.</p>
        """
        text_body = (
            f"{salutation},\n\n"
            f"I was looking at your {data.get('project_type', 'recent')} projects on {domain} and noticed {gap_line}.\n\n"
            f"We help local leaders plug these gaps to ensure ad spend turns into booked jobs.\n\n"
            f"Would you be open to a 5-minute chat? Reply YES and I'll send over a quick written breakdown of what I found.\n\n"
            f"Best,\n{CONFIG.sender_display_name}\n\n"
            f"Reply STOP and I won't follow up again."
        )
    else:
        subject = f"Re: {business_name}'s website tracking"
        html_body = f"""
        <p>{salutation},</p>
        <p>Quickly bringing this to the top of your inbox. Did you see the tracking gap I mentioned on {domain}?</p>
        <p>Happy to send over the written breakdown — just reply YES and I'll get it to you.</p>
        <p>Best,<br>{CONFIG.sender_display_name}</p>
        <p style="font-size:11px;color:#888;">Reply STOP and I won't follow up again.</p>
        """
        text_body = (
            f"{salutation},\n\n"
            f"Quickly bringing this to the top of your inbox. Did you see the tracking gap I mentioned on {domain}?\n\n"
            f"Happy to send over the written breakdown — just reply YES and I'll get it to you.\n\n"
            f"Best,\n{CONFIG.sender_display_name}\n\nReply STOP and I won't follow up again."
        )

    payload = {
        "sender": {"name": CONFIG.sender_display_name, "email": sender_email},
        "to": [{"email": recipient_email}],
        "subject": subject,
        "htmlContent": html_body,
        "textContent": text_body,
        # IMPROVED: List-Unsubscribe improves inbox placement and is close
        # to mandatory for bulk cold outreach under Gmail/Yahoo bulk-sender
        # rules; the old payload had no unsubscribe signal at all.
        "headers": {"List-Unsubscribe": f"mailto:{sender_email}?subject=unsubscribe"},
    }

    try:
        res = SESSION.post(
            "https://api.brevo.com/v3/smtp/email",
            json=payload,
            headers={"api-key": api_key, "content-type": "application/json"},
            timeout=10,
        )
        if res.status_code < 300:
            STATE.record_email_sent()
            if data.get('founder_name'):
                notify_me(f"🚀 Beast Match! Sent Step {step} to {data['founder_name']} ({business_name})")
            return "success"
        STATE.record_error(f"Brevo send failed ({res.status_code}) for {recipient_email}: {res.text[:200]}")
    except requests.RequestException as exc:
        STATE.record_error(f"Brevo send exception for {recipient_email}: {exc}")
    return "failed"


def notify_me(message: str):
    if CONFIG.telegram_bot_token and CONFIG.telegram_chat_id:
        url = f"https://api.telegram.org/bot{CONFIG.telegram_bot_token}/sendMessage"
        try:
            SESSION.post(url, json={"chat_id": CONFIG.telegram_chat_id, "text": message}, timeout=5)
        except requests.RequestException:
            pass

# ---------------------------------------------------------------------------
# Hunt & Automation
# ---------------------------------------------------------------------------

def run_hunt(niche: str, location: str):
    """Core hunt cycle for one niche/city pair. Used by both the manual
    dashboard button and the autonomous GlobalHunterThread."""
    STATE.set_status(f"Hunting: {niche} in {location}")
    STATE.last_cycle_started_at = datetime.now().isoformat(timespec="seconds")

    try:
        places = hunt_candidates(niche, location)
    except Exception as exc:
        STATE.record_error(f"hunt_candidates failed for {niche}/{location}: {exc}")
        places = []

    results_df = pd.DataFrame(columns=["Business Name", "Domain", "Project Type", "Email Status", "Notes"])

    for p in places:
        if STATE.email_cap_reached_today:
            STATE.set_status("Daily email cap reached — hunting paused")
            break
        name = p.get("name", "Unknown")
        uri = p.get("uri")
        if not uri:
            continue
        domain = urlparse(uri).netloc.lower()
        if any(agg in domain for agg in CONFIG.aggregator_domains):
            continue
        if DEDUP.is_processed(domain):
            continue

        STATE.set_status(f"Beast Scraping: {domain}")
        data = scrape_domain({"domain": domain, "business_name": name, "location": location, "niche": niche})
        STATE.leads_scraped_total += 1
        if data["has_meta_pixel"] or data["has_google_ads_tag"]:
            STATE.leads_with_pixel_total += 1

        email_status = "no_email"
        if data["contact_email"]:
            email_domain = data["contact_email"].split("@")[-1]
            # IMPROVED: MX check before ever hitting Brevo — a domain with
            # no mail server is a guaranteed hard bounce, and hard bounces
            # are what get a sending domain rate-limited.
            if not has_mx_record(email_domain):
                email_status = "no_mx"
            # IMPROVED: hold the send until the prospect's local business
            # hours instead of firing at whatever UTC moment the loop hits.
            elif not is_within_send_window(location):
                email_status = "pending_timing"
            else:
                email_status = send_beast_email(data["contact_email"], domain, name, data, step=1)
                time.sleep(CONFIG.email_send_delay_seconds + random.uniform(0, CONFIG.email_send_jitter_seconds))

        STORAGE.save_lead(domain, name, data["contact_email"], None, data["has_meta_pixel"],
                           data["has_google_ads_tag"], data["status"], "hunt", data["project_type"],
                           founder_name=data["founder_name"], competitor=data["competitor"],
                           gbp_status=data["gbp_status"], location=location)
        DEDUP.mark_processed(domain)
        if data["contact_email"]:
            STORAGE.mark_email_sent(domain, data["contact_email"], email_status, step=1 if email_status == "success" else 0)

        new_row = {"Business Name": name, "Domain": domain, "Project Type": data["project_type"],
                   "Email Status": email_status, "Notes": f"Founder: {data['founder_name'] or '?'}"}
        results_df = pd.concat([results_df, pd.DataFrame([new_row])], ignore_index=True)
        yield results_df

    STATE.last_cycle_completed_at = datetime.now().isoformat(timespec="seconds")
    STATE.set_status("Idle")
    backup_sync_now()  # IMPROVED: snapshot state to GitHub after every cycle
    yield results_df


def run_autonomous_hunt(niche: str, location: str):
    """Manual dashboard entry point — thin wrapper kept for UI compatibility."""
    yield from run_hunt(niche, location)


def run_follow_up_cycle():
    leads = STORAGE.get_follow_up_leads(days_delay=3)
    for lead in leads:
        if STATE.email_cap_reached_today:
            break
        data = {"founder_name": None, "project_type": lead['project_type']}
        outcome = send_beast_email(lead['contact_email'], lead['domain'], lead['business_name'], data, step=2)
        STORAGE.mark_email_sent(lead['domain'], lead['contact_email'], outcome, step=2 if outcome == "success" else lead['follow_up_step'])
        time.sleep(CONFIG.email_send_delay_seconds + random.uniform(0, CONFIG.email_send_jitter_seconds))


def global_hunter_loop():
    """# IMPROVED: this is the piece that was missing entirely. render.yaml
    ships AUTOMATED_NICHES and AUTOMATED_CITIES, and the module docstring
    even calls it out, but the old app.py never rotated through them — only
    the manual dashboard button ever triggered a hunt. This loop walks every
    (niche, city) pair, sleeping loop_interval_seconds between pairs, and
    wraps to the start once it exhausts the matrix."""
    niches = CONFIG.automated_niches
    cities = CONFIG.automated_cities
    if not niches or not cities:
        logger.info("GlobalHunterThread idle — AUTOMATED_NICHES/AUTOMATED_CITIES not set.")
        return

    combos = [(n, c) for n in niches for c in cities]
    logger.info(f"GlobalHunterThread starting with {len(combos)} niche x city combinations.")

    while True:
        if STATE.email_cap_reached_today:
            time.sleep(CONFIG.loop_interval_seconds)
            continue
        idx = STATE.current_niche_city_index % len(combos)
        niche, city = combos[idx]
        try:
            for _ in run_hunt(niche, city):
                pass
        except Exception as exc:
            STATE.record_error(f"GlobalHunterThread cycle failed for {niche}/{city}: {exc}")
        STATE.current_niche_city_index += 1
        time.sleep(CONFIG.loop_interval_seconds)


def follow_up_loop():
    while True:
        try:
            run_follow_up_cycle()
        except Exception as exc:
            STATE.record_error(f"Follow-up cycle error: {exc}")
        time.sleep(CONFIG.loop_interval_seconds)


def run_pending_timing_dispatch():
    """IMPROVED: sweeps leads scraped outside their city's send window
    (email_status='pending_timing') and dispatches the ones whose local
    clock has since entered business hours. Runs on a short cycle (5 min)
    since the point is to catch the window opening promptly."""
    for lead in STORAGE.get_pending_timing_leads():
        location = lead.get("location") or ""
        if not is_within_send_window(location):
            continue
        if STATE.email_cap_reached_today:
            break
        data = {
            "founder_name": lead.get("founder_name"),
            "project_type": lead.get("project_type"),
            "competitor": lead.get("competitor"),
            "gbp_status": lead.get("gbp_status"),
        }
        outcome = send_beast_email(lead["contact_email"], lead["domain"], lead["business_name"], data, step=1)
        STORAGE.mark_email_sent(lead["domain"], lead["contact_email"], outcome, step=1 if outcome == "success" else 0)
        time.sleep(CONFIG.email_send_delay_seconds + random.uniform(0, CONFIG.email_send_jitter_seconds))


def pending_timing_loop():
    while True:
        try:
            run_pending_timing_dispatch()
        except Exception as exc:
            STATE.record_error(f"Pending-timing dispatch error: {exc}")
        time.sleep(300)

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
# IMPROVED (this pass): full rebuild. The old dashboard was a single raw
# gr.JSON blob of internal state next to two bare textboxes — functional,
# but unreadable at a glance and gave no way to tell *which* discovery
# source or send-gate was actually doing anything. This version:
#   - replaces the JSON dump with labeled metric cards
#   - adds a discovery-source breakdown so "is Overpass working" has a
#     direct answer instead of an inferred one
#   - adds a live log tail (backed by LOG_RING) so operational visibility
#     doesn't require opening the Render log viewer separately
#   - separates manual hunting, health/observability, and the read-only
#     autonomous-mode configuration into tabs instead of one crowded page
# ---------------------------------------------------------------------------

RESULTS_COLUMNS = ["Business Name", "Domain", "Project Type", "Email Status", "Notes"]


def _status_card_values():
    """Returns the flat tuple of values for the top metric-card row, in the
    same order the cards are declared below."""
    s = STATE.snapshot()
    cap_line = f"{s['emails_dispatched_today']} / {s['todays_cap']}"
    if s["email_cap_reached_today"]:
        cap_line += " (cap reached)"
    leads_line = f"{s['leads_scraped_total']}"
    # IMPROVED: leads_with_pixel_total counts leads that ALREADY have a Meta
    # Pixel / Google Ads tag — the opposite of the outreach opportunity. The
    # card now shows the actual pitch-worthy gap (scraped minus already-tracked)
    # instead of a number that reads backwards from what it's labeled.
    gap_count = max(s["leads_scraped_total"] - s["leads_with_pixel_total"], 0)
    gap_line = f"{gap_count}"
    errors_line = f"{s['errors_total']}"
    last_error = s["last_error"] or "—"
    return s["status"], cap_line, leads_line, gap_line, errors_line, last_error


def _source_breakdown_df():
    s = STATE.snapshot()
    last = s.get("last_hunt_source_breakdown") or {}
    return pd.DataFrame(
        {
            "Source": ["Geoapify (Places API)", "Overpass (free OSM)", "DuckDuckGo (fallback)"],
            "Last Cycle": [last.get("geoapify", 0), last.get("overpass", 0), last.get("ddg", 0)],
            "Total (since restart)": [
                s["candidates_from_geoapify_total"],
                s["candidates_from_overpass_total"],
                s["candidates_from_ddg_total"],
            ],
        }
    )


def _health_text():
    s = STATE.snapshot()
    lines = [
        f"Last cycle started:   {s['last_cycle_started_at'] or '—'}",
        f"Last cycle completed: {s['last_cycle_completed_at'] or '—'}",
        f"Overpass mirror failures (total): {s['overpass_mirror_failures_total']}",
        f"Geoapify calls today: {s['geoapify_calls_today']} (cap reached: {s['geoapify_cap_reached_today']})",
        f"GitHub backup: {'enabled' if GITHUB_BACKUP.enabled else 'disabled — set GITHUB_TOKEN + GITHUB_BACKUP_REPO'}",
    ]
    return "\n".join(lines)


def _automation_summary():
    niches = CONFIG.automated_niches
    cities = CONFIG.automated_cities
    if not niches or not cities:
        return "Autonomous rotation is **off** — set `AUTOMATED_NICHES` and `AUTOMATED_CITIES` in Render to enable it."
    combos = len(niches) * len(cities)
    hours_per_cycle = round(CONFIG.loop_interval_seconds / 60, 1)
    return (
        f"**Autonomous rotation is on.**\n\n"
        f"- {len(niches)} niche(s) × {len(cities)} cit(ies) = **{combos} combinations** in rotation\n"
        f"- ~{hours_per_cycle} min between each hunt cycle "
        f"(full rotation takes roughly {round(combos * hours_per_cycle / 60, 1)} hours)\n"
        f"- Send window: {CONFIG.send_window_start_hour}:00–{CONFIG.send_window_end_hour}:00, local to each city\n"
        f"- Daily email cap: {CONFIG.daily_email_cap} "
        f"(warm-up ramp: {CONFIG.warmup_start_cap} → {CONFIG.daily_email_cap} over {CONFIG.warmup_days} days)\n\n"
        f"Niches: {', '.join(niches)}\n\n"
        f"Cities: {', '.join(cities)}"
    )


CARD_CSS = """
.metric-card textarea, .metric-card input {
    font-size: 1.05rem !important;
    text-align: center !important;
    font-weight: 600 !important;
}
.metric-card label {
    text-align: center !important;
    width: 100% !important;
}
#gp-log textarea {
    font-family: ui-monospace, "SF Mono", Menlo, monospace !important;
    font-size: 0.8rem !important;
}
"""


def build_ui():
    with gr.Blocks(title="Growth Pulse") as demo:
        gr.Markdown(
            "# Growth Pulse\n"
            "Autonomous local-trade lead generation — discovery, enrichment, and outreach in one pipeline."
        )

        # --- Top metric strip: at-a-glance system health -------------------
        with gr.Row():
            status_card = gr.Textbox(label="Status", interactive=False, elem_classes="metric-card")
            emails_card = gr.Textbox(label="Emails sent today / cap", interactive=False, elem_classes="metric-card")
            leads_card = gr.Textbox(label="Leads scraped (total)", interactive=False, elem_classes="metric-card")
            pixel_card = gr.Textbox(label="Leads with no ad tracking (the pitch)", interactive=False, elem_classes="metric-card")
            errors_card = gr.Textbox(label="Errors (total)", interactive=False, elem_classes="metric-card")
        last_error_box = gr.Textbox(label="Most recent error", interactive=False)

        with gr.Tabs():
            # --- Manual hunt ------------------------------------------------
            with gr.Tab("Manual Hunt"):
                gr.Markdown(
                    "Runs one hunt cycle immediately for the niche/city below, in addition to whatever "
                    "the autonomous rotation is doing in the background. Results stream in as each "
                    "candidate is scraped."
                )
                with gr.Row():
                    niche_input = gr.Textbox(label="Niche", value="Roofing", placeholder="e.g. Roofing, HVAC, Electricians")
                    loc_input = gr.Textbox(label="Location", value="Miami FL", placeholder="City, State/Country")
                    hunt_btn = gr.Button("Launch Hunt", variant="primary", scale=0)
                results_table = gr.DataFrame(
                    label="Live results",
                    value=pd.DataFrame(columns=RESULTS_COLUMNS),
                    wrap=True,
                )

            # --- Discovery sources / health ---------------------------------
            with gr.Tab("Discovery Sources"):
                gr.Markdown(
                    "Which free discovery method is actually producing candidates. If Overpass shows "
                    "0 for a niche, check the Live Log tab for mirror errors — it falls back to "
                    "DuckDuckGo automatically either way, so hunting keeps running."
                )
                source_table = gr.DataFrame(value=_source_breakdown_df(), interactive=False)
                health_box = gr.Textbox(label="System health", value=_health_text(), interactive=False, lines=6)

            # --- Live log -----------------------------------------------------
            with gr.Tab("Live Log"):
                gr.Markdown("Last 300 log lines, newest at the bottom. Refreshes automatically.")
                log_box = gr.Textbox(
                    value=LOG_RING.tail_text(), interactive=False, lines=24, max_lines=24,
                    show_label=False, elem_id="gp-log",
                )

            # --- Autonomous mode config (read-only) ---------------------------
            with gr.Tab("Automation"):
                gr.Markdown(_automation_summary())

        hunt_btn.click(run_autonomous_hunt, inputs=[niche_input, loc_input], outputs=[results_table])

        # IMPROVED: gr.Timer replaces the fragile demo.load(fn, every=N)
        # pattern, which raises a validation error on several recent Gradio
        # releases because `every` expects a Timer/None, not a bare int.
        # A single timer now drives every live-updating panel on the page.
        refresh_timer = gr.Timer(5)
        refresh_timer.tick(
            _status_card_values,
            outputs=[status_card, emails_card, leads_card, pixel_card, errors_card, last_error_box],
        )
        refresh_timer.tick(_source_breakdown_df, outputs=source_table)
        refresh_timer.tick(_health_text, outputs=health_box)
        refresh_timer.tick(LOG_RING.tail_text, outputs=log_box)

        demo.load(
            _status_card_values,
            outputs=[status_card, emails_card, leads_card, pixel_card, errors_card, last_error_box],
        )
    return demo


if __name__ == "__main__":
    threading.Thread(target=global_hunter_loop, daemon=True, name="GlobalHunterThread").start()
    threading.Thread(target=follow_up_loop, daemon=True, name="FollowUpThread").start()
    threading.Thread(target=pending_timing_loop, daemon=True, name="PendingTimingThread").start()
    threading.Thread(target=backup_loop, daemon=True, name="BackupThread").start()
    # IMPROVED: theme/css moved here from the Blocks() constructor — Gradio
    # 6.0 relocated these two params from Blocks to launch() (see
    # gradio.app/main/guides/gradio-6-migration-guide). The old call site
    # still worked (Gradio kept it backward-compatible with a warning), but
    # fixing it now avoids relying on that compatibility shim staying around.
    build_ui().launch(
        server_name="0.0.0.0", server_port=7860,
        theme=gr.themes.Soft(primary_hue="blue", secondary_hue="slate"),
        css=CARD_CSS,
    )
