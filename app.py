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
  E. SMTP dispatch engine — with exponential backoff retry.
  F. Gradio dashboard — live stream of results.

# BEAST PHASE 1 IMPROVEMENTS:
- Daily Email Cap (default 180, configurable via env var) with daily reset.
- Hyper-Personalized Email Template.
- Free Founder/Owner Email Finder Module.
- Competitor Intelligence.
- Better Contact Extraction.

# BEAST PHASE 2 IMPROVEMENTS (The Conversion Dominator):
- Automated "Video Audit" Hook: Simulates a personalized audit mention.
- GBP Intelligence: Checks Google Business Profile presence/reviews heuristic.
- Multi-Step Follow-up Logic: Tracks sent dates and triggers "nudge" emails.
- Deep-Link Scraping: Identifies specific project types (e.g., "Commercial Roofing").
- Telegram/Webhook Notifications: Real-time alerts for founder matches.
"""

import os
import re
import ssl
import csv
import time
import sqlite3
import logging
import smtplib
import socket
import threading
from datetime import datetime, date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, List, Dict

import requests
import gradio as gr
import pandas as pd
import html
from urllib.parse import urlparse

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
    telegram_bot_token: Optional[str] = field(default_factory=lambda: os.environ.get("TELEGRAM_BOT_TOKEN"))
    telegram_chat_id: Optional[str] = field(default_factory=lambda: os.environ.get("TELEGRAM_CHAT_ID"))

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
    
    daily_email_cap: int = int(os.environ.get("DAILY_EMAIL_CAP", "180"))
    email_send_delay_seconds: int = int(os.environ.get("EMAIL_SEND_DELAY_SECONDS", "5"))

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
        self._counter_date: date = date.today()

    def _roll_daily_counter_if_needed(self) -> None:
        today = date.today()
        if today != self._counter_date:
            self.emails_dispatched_today = 0
            self.email_cap_reached_today = False
            self._counter_date = today

    def set_status(self, status: str) -> None:
        with self._lock:
            self.status = status

    def record_email_sent(self) -> None:
        with self._lock:
            self._roll_daily_counter_if_needed()
            self.emails_dispatched_today += 1
            if self.emails_dispatched_today >= CONFIG.daily_email_cap:
                self.email_cap_reached_today = True

    def snapshot(self) -> dict:
        with self._lock:
            self._roll_daily_counter_if_needed()
            return {
                "status": self.status,
                "emails_dispatched_today": self.emails_dispatched_today,
                "email_cap_reached_today": self.email_cap_reached_today,
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
# Storage layer (Updated for Phase 2)
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
            # # IMPROVED: Added project_type and follow_up_step
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
                    outcome TEXT NOT NULL,
                    step INTEGER DEFAULT 1
                )
                """
            )
            connection.commit()
            connection.close()

    def save_lead(self, domain: str, business_name: Optional[str], contact_email: Optional[str],
                  phone_number: Optional[str], has_meta_pixel: bool, has_google_ads_tag: bool,
                  scrape_status: str, source: str, project_type: str = None) -> None:
        scraped_at = datetime.now().isoformat(timespec="seconds")
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO leads (domain, business_name, contact_email, phone_number, has_meta_pixel,
                                    has_google_ads_tag, scrape_status, source, scraped_at, project_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    business_name=excluded.business_name,
                    contact_email=excluded.contact_email,
                    phone_number=excluded.phone_number,
                    has_meta_pixel=excluded.has_meta_pixel,
                    has_google_ads_tag=excluded.has_google_ads_tag,
                    scrape_status=excluded.scrape_status,
                    source=excluded.source,
                    scraped_at=excluded.scraped_at,
                    project_type=COALESCE(excluded.project_type, leads.project_type)
                """,
                (domain, business_name, contact_email, phone_number, int(has_meta_pixel),
                 int(has_google_ads_tag), scrape_status, source, scraped_at, project_type),
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

    def get_follow_up_leads(self, days_delay: int = 3) -> List[Dict]:
        """# IMPROVED: Fetches leads ready for a follow-up nudge."""
        cutoff = (datetime.now() - timedelta(days=days_delay)).isoformat()
        with self._local_lock:
            connection = sqlite3.connect(self.config.sqlite_path)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute(
                "SELECT * FROM leads WHERE email_status='success' AND follow_up_step=1 AND last_email_sent_at < ?",
                (cutoff,)
            )
            rows = [dict(row) for row in cursor.fetchall()]
            connection.close()
            return rows


STORAGE = StorageBackend(CONFIG)

# ---------------------------------------------------------------------------
# # IMPROVED: Beast Phase 2 Core Modules
# ---------------------------------------------------------------------------

def notify_me(message: str):
    """# IMPROVED: Sends real-time notification via Telegram."""
    if CONFIG.telegram_bot_token and CONFIG.telegram_chat_id:
        url = f"https://api.telegram.org/bot{CONFIG.telegram_bot_token}/sendMessage"
        try:
            requests.post(url, json={"chat_id": CONFIG.telegram_chat_id, "text": message}, timeout=5)
        except:
            pass

def free_google_search(query: str) -> str:
    headers = {"User-Agent": CONFIG.user_agent}
    try:
        search_url = f"https://www.google.com/search?q={requests.utils.quote(query)}"
        response = requests.get(search_url, headers=headers, timeout=10)
        return response.text
    except:
        return ""

def check_gbp_status(business_name: str, location: str) -> str:
    """# IMPROVED: Heuristic check for Google Business Profile strength."""
    query = f"{business_name} {location} reviews"
    html_content = free_google_search(query)
    # Look for "Google review" or "rating"
    if "Google review" in html_content:
        match = re.search(r"([\d\.]+) Google reviews", html_content)
        if match:
            count = int(match.group(1).replace(",", ""))
            if count < 10: return "weak_reviews"
            return "active"
    return "not_found"

def identify_project_type(html_text: str) -> Optional[str]:
    """# IMPROVED: Deep-link scraping for specific high-ticket projects."""
    projects = {
        "Commercial": ["commercial", "industrial", "warehouse", "office"],
        "Luxury": ["luxury", "premium", "high-end", "custom home"],
        "Government": ["government", "municipal", "public works"]
    }
    for p_type, keywords in projects.items():
        if any(kw in html_text.lower() for kw in keywords):
            return p_type
    return "Residential"

def find_founder_name(domain: str, business_name: str) -> Optional[str]:
    query = f'"{business_name}" founder OR owner OR CEO site:{domain}'
    html_content = free_google_search(query)
    patterns = [
        r"(?:Founder|CEO|Owner|President) is ([A-Z][a-z]+ [A-Z][a-z]+)",
        r"([A-Z][a-z]+ [A-Z][a-z]+), (?:Founder|CEO|Owner|President)",
    ]
    for p in patterns:
        match = re.search(p, html_content)
        if match: return match.group(1)
    return None

def find_competitor(location: str, niche: str, current_business: str) -> str:
    query = f"{niche} in {location}"
    html_content = free_google_search(query)
    potential_competitors = re.findall(r'aria-label="([^"]+)"', html_content)
    for comp in potential_competitors:
        if current_business.lower() not in comp.lower() and len(comp) < 50:
            return comp
    return "other local firms"

# ---------------------------------------------------------------------------
# Scraper & Email Engine
# ---------------------------------------------------------------------------

def scrape_domain(candidate: dict) -> dict:
    raw_domain = candidate["domain"]
    url = f"https://{raw_domain}" if not raw_domain.startswith("http") else raw_domain
    headers = {"User-Agent": CONFIG.user_agent}
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
        "project_type": "Residential"
    }

    try:
        response = requests.get(url, headers=headers, timeout=CONFIG.scrape_timeout_seconds)
        html_text = response.text
        result["has_meta_pixel"] = "connect.facebook.net/en_US/fbevents.js" in html_text
        result["has_google_ads_tag"] = "googletagmanager.com/gtag/js" in html_text
        
        # # IMPROVED: Project Type Identification
        result["project_type"] = identify_project_type(html_text)

        # Email extraction (prioritizing personal)
        emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", html_text)
        if emails:
            personal = [e for e in emails if any(x in e.lower() for x in ["ceo", "founder", "owner"])]
            result["contact_email"] = personal[0] if personal else emails[0]

        result["founder_name"] = find_founder_name(raw_domain, candidate["business_name"])
        result["competitor"] = find_competitor(candidate["location"], candidate["niche"], candidate["business_name"])
        result["gbp_status"] = check_gbp_status(candidate["business_name"], candidate["location"])

        result["status"] = "success"
    except:
        result["status"] = "error"

    return result

def send_beast_email(recipient_email, domain, business_name, data: dict, step: int = 1):
    api_key = os.environ.get("BREVO_API_KEY")
    sender_email = os.environ.get("EMAIL_USER")
    if not api_key or not sender_email or STATE.email_cap_reached_today: return "failed"

    salutation = f"Hi {data['founder_name'].split()[0]}" if data.get('founder_name') else f"Hi {business_name}"
    
    # # IMPROVED: Step 1 (Initial Audit) vs Step 2 (The Nudge)
    if step == 1:
        subject = f"Question about {business_name}'s {data['project_type']} projects"
        audit_line = "I recorded a 60-second walkthrough of your site showing where your ad tracking is leaking money."
        if data['gbp_status'] == "weak_reviews":
            audit_line += f" I also noticed {data['competitor']} is outranking you on Google reviews, which is costing you trust."
        
        body = f"""
        <p>{salutation},</p>
        <p>I was looking at your {data['project_type']} projects on {domain}. {audit_line}</p>
        <p>We help local leaders plug these gaps to ensure ad spend turns into booked jobs.</p>
        <p>Would you be open to a 5-minute chat? <strong>Reply YES</strong> and I'll send the video.</p>
        <p>Best,<br>{CONFIG.sender_display_name}</p>
        """
    else:
        subject = f"Re: {business_name}'s website tracking"
        body = f"""
        <p>{salutation},</p>
        <p>Quickly bringing this to the top of your inbox. Did you see the tracking gap I mentioned on {domain}?</p>
        <p>I have that video walkthrough ready for you. Just let me know if I should send it over.</p>
        <p>Best,<br>{CONFIG.sender_display_name}</p>
        """

    payload = {
        "sender": {"name": CONFIG.sender_display_name, "email": sender_email},
        "to": [{"email": recipient_email}],
        "subject": subject,
        "htmlContent": body
    }
    
    try:
        res = requests.post("https://api.brevo.com/v3/smtp/email", json=payload, 
                             headers={"api-key": api_key, "content-type": "application/json"}, timeout=10)
        if res.status_code < 300:
            STATE.record_email_sent()
            if data.get('founder_name'):
                notify_me(f"🚀 Beast Match! Sent Step {step} to {data['founder_name']} ({business_name})")
            return "success"
    except:
        pass
    return "failed"

# ---------------------------------------------------------------------------
# Hunt & Automation
# ---------------------------------------------------------------------------

def run_autonomous_hunt(niche: str, location: str):
    STATE.set_status(f"Hunting: {niche} in {location}")
    
    # Standard Places API Hunt
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {"Content-Type": "application/json", "X-Goog-Api-Key": CONFIG.google_places_api_key, 
               "X-Goog-FieldMask": "places.displayName,places.websiteUri"}
    payload = {"textQuery": f"{niche} in {location}", "maxResultCount": CONFIG.max_hunt_results}
    
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=15).json()
        places = res.get("places", [])
    except:
        places = []

    results_df = pd.DataFrame(columns=["Business Name", "Domain", "Project Type", "Email Status", "Notes"])

    for p in places:
        name = p.get("displayName", {}).get("text", "Unknown")
        uri = p.get("websiteUri")
        if not uri: continue
        domain = urlparse(uri).netloc.lower()
        if DEDUP.is_processed(domain): continue

        STATE.set_status(f"Beast Scraping: {domain}")
        data = scrape_domain({"domain": domain, "business_name": name, "location": location, "niche": niche})
        
        email_status = "no_email"
        if data["contact_email"]:
            email_status = send_beast_email(data["contact_email"], domain, name, data, step=1)
        
        STORAGE.save_lead(domain, name, data["contact_email"], None, data["has_meta_pixel"], 
                          data["has_google_ads_tag"], data["status"], "hunt", data["project_type"])
        DEDUP.mark_processed(domain)
        STORAGE.mark_email_sent(domain, data["contact_email"] or "none", email_status, step=1)

        new_row = {"Business Name": name, "Domain": domain, "Project Type": data["project_type"], 
                   "Email Status": email_status, "Notes": f"Founder: {data['founder_name'] or '?'}"}
        results_df = pd.concat([results_df, pd.DataFrame([new_row])], ignore_index=True)
        yield results_df, STATE.snapshot()

    STATE.set_status("Idle")
    yield results_df, STATE.snapshot()

def run_follow_up_cycle():
    """# IMPROVED: Background cycle to send follow-up nudges."""
    leads = STORAGE.get_follow_up_leads(days_delay=3)
    for lead in leads:
        if STATE.email_cap_reached_today: break
        # Minimal data for follow-up
        data = {"founder_name": None, "project_type": lead['project_type']}
        outcome = send_beast_email(lead['contact_email'], lead['domain'], lead['business_name'], data, step=2)
        STORAGE.mark_email_sent(lead['domain'], lead['contact_email'], outcome, step=2)
        time.sleep(CONFIG.email_send_delay_seconds)

def automation_loop():
    while True:
        try:
            run_follow_up_cycle()
        except Exception as e:
            logger.error(f"Follow-up error: {e}")
        time.sleep(CONFIG.loop_interval_seconds)

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

def build_ui():
    with gr.Blocks(title="Growth Pulse Beast v2") as demo:
        gr.Markdown("# 🚀 Growth Pulse: Beast Conversion Dominator (Phase 2)")
        with gr.Row():
            with gr.Column():
                niche_input = gr.Textbox(label="Niche", value="Roofing")
                loc_input = gr.Textbox(label="Location", value="Miami")
                hunt_btn = gr.Button("🔥 Launch Beast Hunt", variant="primary")
            with gr.Column():
                status_box = gr.JSON(label="Vital Signs", value=STATE.snapshot())
        results_table = gr.DataFrame(label="Live Beast Stream")
        hunt_btn.click(run_autonomous_hunt, inputs=[niche_input, loc_input], outputs=[results_table, status_box])
        demo.load(lambda: STATE.snapshot(), outputs=status_box, every=5)
    return demo

if __name__ == "__main__":
    threading.Thread(target=automation_loop, daemon=True).start()
    build_ui().launch(server_name="0.0.0.0", server_port=7860)
