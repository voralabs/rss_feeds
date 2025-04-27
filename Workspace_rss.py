import os
import logging
import requests
import yaml
import feedparser
from dateutil import parser as dateparser
from datetime import datetime, timezone
from supabase import create_client, Client

# ----------------------------
# Logging Setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# ----------------------------
# Config & Environment Loading
# ----------------------------
CONFIG_PATH = 'config.yaml'

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY')

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    logging.error("Supabase credentials not found in environment variables. Set SUPABASE_URL and SUPABASE_SERVICE_KEY.")
    exit(1)

# Load feeds from config.yaml
def load_config(path):
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
            feeds = config.get('feeds', [])
            if not feeds:
                logging.error("No feeds found in config.yaml.")
                exit(1)
            return feeds
    except Exception as e:
        logging.error(f"Failed to load config.yaml: {e}")
        exit(1)

feeds = load_config(CONFIG_PATH)

# ----------------------------
# Supabase Client Init
# ----------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ----------------------------
# Helper Functions
# ----------------------------
def is_valid_url(url):
    return url.startswith('http://') or url.startswith('https://')

def clean_summary(summary):
    if not summary:
        return ''
    # Truncate to 2000 chars if too long
    return summary[:2000]

def parse_datetime(entry):
    # Prefer feedparser's parsed date
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    # Fallback: try published or updated fields
    for date_field in ['published', 'updated', 'created']:
        date_str = getattr(entry, date_field, None)
        if date_str:
            try:
                dt = dateparser.parse(date_str)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt
            except Exception:
                continue
    return None

def get_guid(entry):
    # Prefer entry.id, then entry.guid, then entry.link
    guid = getattr(entry, 'id', None) or getattr(entry, 'guid', None)
    if not guid or guid == getattr(entry, 'link', None):
        guid = getattr(entry, 'link', None)
    return guid

def article_exists(source_url, guid):
    try:
        res = supabase.table('articles').select('id').eq('source_url', source_url).eq('guid', guid).limit(1).execute()
        return bool(res.data)
    except Exception as e:
        logging.error(f"DB check failed for guid {guid}: {e}")
        return False

def insert_article(data):
    try:
        res = supabase.table('articles').insert(data).execute()
        if res.error:
            logging.error(f"Failed to insert article: {res.error}")
            return False
        return True
    except Exception as e:
        logging.error(f"Insert failed: {e}")
        return False

# ----------------------------
# Main Fetch & Store Logic
# ----------------------------
def fetch_and_store():
    for feed in feeds:
        name = feed.get('name')
        url = feed.get('url')
        logging.info(f"Processing feed: {name} ({url})")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code != 200:
                logging.error(f"Failed to fetch {url}: HTTP {resp.status_code}")
                continue
            feed_data = feedparser.parse(resp.content)
            if feed_data.bozo:
                logging.error(f"Feed parsing error for {url}: {feed_data.bozo_exception}")
                continue
            for entry in feed_data.entries:
                title = getattr(entry, 'title', '').strip()
                link = getattr(entry, 'link', '').strip()
                summary = clean_summary(getattr(entry, 'summary', '') or getattr(entry, 'description', ''))
                published_at = parse_datetime(entry)
                guid = get_guid(entry)

                # Data Quality Checks
                if not title or not link:
                    logging.warning(f"Skipping entry missing title/link in feed {name}")
                    continue
                if not is_valid_url(link):
                    logging.warning(f"Skipping entry with invalid link: {link}")
                    continue
                if not guid:
                    logging.warning(f"Skipping entry with missing guid in feed {name}")
                    continue
                if not published_at:
                    logging.warning(f"Skipping entry with invalid/missing date in feed {name}")
                    continue

                # Check for duplicate
                if article_exists(url, guid):
                    continue

                # Prepare data for DB
                article = {
                    'source_url': url,
                    'guid': guid,
                    'link': link,
                    'title': title,
                    'summary': summary,
                    'published_at': published_at.isoformat(),
                    # 'Workspaceed_at' will default to now() in DB
                }
                success = insert_article(article)
                if success:
                    logging.info(f"Inserted new article: {title}")
        except Exception as e:
            logging.error(f"Error processing feed {name}: {e}")

if __name__ == '__main__':
    fetch_and_store()

# ----------------------------
# Supabase Table Schema (Instructions)
# ----------------------------
"""
Supabase Table: articles

Columns:
- id: UUID, primary key, default uuid_generate_v4()
- source_url: TEXT
- guid: TEXT
- link: TEXT
- title: TEXT
- summary: TEXT
- published_at: TIMESTAMPTZ
- Workspaceed_at: TIMESTAMPTZ, default now()

UNIQUE constraint: (source_url, guid)

To create this table, run (in Supabase SQL editor):

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS articles (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_url text NOT NULL,
    guid text NOT NULL,
    link text NOT NULL,
    title text NOT NULL,
    summary text,
    published_at timestamptz NOT NULL,
    Workspaceed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_url, guid)
);

# RLS: Enable and configure as needed. For automation, use the service key.
"""

# ----------------------------
# GitHub Secrets Setup (Instructions)
# ----------------------------
"""
1. Go to your GitHub repository > Settings > Secrets and variables > Actions > New repository secret.
2. Add SUPABASE_URL and SUPABASE_SERVICE_KEY with your Supabase project credentials.
3. The GitHub Actions workflow will inject these as environment variables.
"""