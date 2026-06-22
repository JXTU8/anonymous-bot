#!/usr/bin/env python3
"""
Telegram Anonymous Q&A Bot (for classroom use)
- Students /start to ask a question, anonymously or publicly
- After typing, buttons appear: Post Anonymously / Post Publicly / Edit / Cancel
- Teacher (admin) always gets a silent DM with full asker identity
- AI filter (Groq llama-3.3-70b + Serper) queues high-risk messages for
  admin review — never auto-deletes
- Students react 👍/👎 directly on questions in the group using Telegram's
  native message reactions (no inline button — keeps the chat compact)
- Teacher answers by copy-pasting the question into the group as a reply —
  no bot command needed for this
- /ask <#> <text> sends the asker an anonymous DM requesting clarification;
  their reply is relayed back to the teacher
- Question count and pending reviews persisted in Upstash Redis
  (survive bot restarts)
"""

import asyncio
import os
import re
import json
import logging
import threading
import uuid
from collections import Counter
from datetime import datetime
from flask import Flask
from dotenv import load_dotenv
import requests as http_requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ApplicationHandlerStop,
    filters,
    ContextTypes,
)

load_dotenv()


def _env_int(name: str, default: int = 0) -> int:
    """
    Read an integer environment variable safely.

    Plain int(os.getenv(name, "0")) crashes the whole process at import
    time (before any error handler exists to catch it) if the variable is
    *set but blank* — e.g. someone added GROUP_CHAT_ID in the Render
    dashboard and left the value empty while typing it in. os.getenv then
    returns "" (not None), and int("") raises ValueError. That takes the
    entire bot down on every single startup attempt until the env var is
    fixed. This logs a clear warning and falls back to `default` instead.
    """
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logging.getLogger(__name__).warning(
            "Environment variable %s=%r is not a valid integer — using default %d",
            name, raw, default,
        )
        return default


# ─── Configuration ────────────────────────────────────────────────────────────
BOT_TOKEN           = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID       = _env_int("GROUP_CHAT_ID")
ADMIN_CHAT_ID       = _env_int("ADMIN_CHAT_ID")
PORT                = _env_int("PORT", 8080)
GROQ_API_KEY        = os.getenv("GROQ_API_KEY", "")
SERPER_API_KEY      = os.getenv("SERPER_API_KEY", "")
UPSTASH_REDIS_URL   = os.getenv("UPSTASH_REDIS_REST_URL", "")
UPSTASH_REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
LOG_FILE            = "confessions_log.json"
FILTER_LOG          = "filtered_log.json"

STATUS_PUBLISHED = "published"
STATUS_PENDING_APPROVAL = "pending_approval"
STATUS_REJECTED = "rejected"

# ─── Redis keys ───────────────────────────────────────────────────────────────
REDIS_COUNT_KEY   = "confession:count"
REDIS_PENDING_KEY = "confession:pending_reviews"
REDIS_LOG_KEY     = "confession:log"
REDIS_FILTER_KEY  = "confession:filter_log"
REDIS_DOMAINS_KEY = "confession:custom_block_domains"
REDIS_BANNED_KEY  = "confession:banned_users"
REDIS_CLARIFY_KEY  = "confession:awaiting_clarification"

RATE_LIMIT_SECONDS = 5
TELEGRAM_CAPTION_LIMIT = 1024
TELEGRAM_TEXT_LIMIT = 4096

# ─── Groq client (optional — features silently degrade without it) ────────────
try:
    from groq import Groq
    groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
except ImportError:
    groq_client = None

FILTER_ENABLED = bool(groq_client)

# ─── Conversation states ──────────────────────────────────────────────────────
WAITING_CONFESSION = 1
WAITING_CHOICE     = 2

last_submit_at: dict[int, datetime] = {}

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Flask health server ───────────────────────────────────────────────────────
# NOTE: this endpoint by itself does NOT keep a Render free-tier service awake.
# Render's free plan spins a web service down after 15 minutes with no INBOUND
# HTTP request — and a long-polling Telegram bot never receives inbound HTTP
# requests (it only makes outbound calls to Telegram). Render's own health
# probes don't count as activity either, by design. This route only helps if
# something external actually calls it periodically — e.g. a free uptime
# monitor (UptimeRobot, cron-job.org, etc.) hitting /health every 5–10 min.
# See the root-cause note in render.yaml for details.
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Q&A bot is running! 📚", 200

@flask_app.route("/health")
def health():
    return "OK", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)

# ─── Upstash Redis helpers ────────────────────────────────────────────────────
def _redis(*command: str):
    """
    Execute one Redis command via the Upstash REST API using the POST +
    JSON-body "command pipeline" style, e.g. _redis("SET", "key", "value").

    IMPORTANT: this sends the command as a JSON array in the POST body,
    NOT as raw text glued into the URL path. That matters because question
    content is arbitrary user text — it can contain "/", quotes, unicode,
    newlines, etc. Putting that directly in a URL path (the old approach)
    silently corrupts the request: a "/" splits the path into extra
    segments, Upstash misparses the argument count, and — because Upstash
    returns HTTP 200 with {"error": ...} rather than an HTTP error status —
    the failure was invisible unless you checked the response body.
    Sending the value inside a JSON POST body sidesteps all of this.

    Returns the parsed JSON response dict (e.g. {"result": "OK"}), or None
    if the request itself failed (network error, bad credentials, etc.).
    """
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return None
    url = UPSTASH_REDIS_URL.rstrip("/")
    headers = {
        "Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}",
        "Content-Type": "application/json",
    }
    try:
        resp = http_requests.post(url, headers=headers, json=list(command), timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            logger.warning("Upstash command error (%s): %s", command[0] if command else "?", data["error"])
            return None
        return data
    except Exception as exc:
        logger.warning("Redis REST error (%s): %s", command[0] if command else "?", exc)
        return None


async def _redis_async(*command: str):
    """
    Async wrapper around _redis(). The actual HTTP call (http_requests.post)
    is blocking — running it directly inside an `async def` handler would
    freeze the bot's single event loop for every other user (including the
    admin) for the duration of the network round-trip. asyncio.to_thread
    runs it on a worker thread instead, so the event loop stays responsive.
    """
    return await asyncio.to_thread(_redis, *command)


async def redis_get(key: str):
    """GET key — returns the value string or None."""
    result = await _redis_async("GET", key)
    if result and "result" in result:
        return result["result"]
    return None


async def redis_set(key: str, value: str) -> bool:
    """SET key value. Returns True only on a confirmed {"result": "OK"} response."""
    result = await _redis_async("SET", key, value)
    return bool(result) and result.get("result") == "OK"


async def redis_incr(key: str) -> int:
    """INCR key — atomic increment, returns new value."""
    result = await _redis_async("INCR", key)
    if result and "result" in result:
        return int(result["result"])
    return 0


async def redis_decr(key: str) -> int:
    """DECR key — atomic decrement, floors at 0 via a follow-up check."""
    result = await _redis_async("DECR", key)
    if result and "result" in result:
        val = int(result["result"])
        if val < 0:
            await redis_set(key, "0")
            return 0
        return val
    return 0


# ─── Confession counter (Redis-backed) ────────────────────────────────────────
async def load_count() -> int:
    """Read confession counter from Redis. Falls back to 0 if unavailable."""
    val = await redis_get(REDIS_COUNT_KEY)
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0


async def save_count(n: int) -> None:
    """Write confession counter to Redis."""
    await redis_set(REDIS_COUNT_KEY, str(n))


async def incr_count() -> int:
    """Atomically increment and return new confession count."""
    new_val = await redis_incr(REDIS_COUNT_KEY)
    if new_val == 0:
        # Redis unavailable — fall back to in-memory increment
        global confession_count
        confession_count += 1
        return confession_count
    return new_val


async def decr_count() -> None:
    """Atomically decrement confession count (rollback on post failure)."""
    await redis_decr(REDIS_COUNT_KEY)


# ─── Pending reviews (Redis-backed) ───────────────────────────────────────────
async def load_pending_reviews() -> dict:
    """Load pending reviews dict from Redis."""
    raw = await redis_get(REDIS_PENDING_KEY)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


async def save_pending_reviews(data: dict) -> None:
    """Persist pending reviews dict to Redis."""
    await redis_set(REDIS_PENDING_KEY, json.dumps(data, ensure_ascii=False))


async def load_json_list(key: str, file_path: str) -> list:
    raw = await redis_get(key)
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return data
        except (json.JSONDecodeError, TypeError):
            logger.warning("Could not decode Redis list for key=%s", key)
    try:
        with open(file_path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


async def save_json_list(key: str, file_path: str, data: list) -> None:
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    if not await redis_set(key, payload):
        logger.warning(
            "Redis save FAILED for key=%s (%d bytes) — data only written to local disk, "
            "which does not persist across Render restarts/redeploys.",
            key, len(payload),
        )
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(payload)


async def load_log() -> list:
    return await load_json_list(REDIS_LOG_KEY, LOG_FILE)


async def save_log(log: list) -> None:
    await save_json_list(REDIS_LOG_KEY, LOG_FILE, log)


async def append_log(entry: dict) -> None:
    log = await load_log()
    log.append(entry)
    await save_log(log)


async def update_log_by_review_id(review_id: str, updates: dict) -> bool:
    log = await load_log()
    changed = False
    for entry in log:
        if entry.get("review_id") == review_id:
            entry.update(updates)
            changed = True
            break
    if changed:
        await save_log(log)
    return changed


async def load_filter_log() -> list:
    return await load_json_list(REDIS_FILTER_KEY, FILTER_LOG)


async def save_filter_log(log: list) -> None:
    await save_json_list(REDIS_FILTER_KEY, FILTER_LOG, log)


async def append_filter_log(entry: dict) -> None:
    log = await load_filter_log()
    log.append(entry)
    await save_filter_log(log)


async def update_filter_log_by_review_id(review_id: str, updates: dict) -> bool:
    log = await load_filter_log()
    changed = False
    for entry in log:
        if entry.get("review_id") == review_id:
            entry.update(updates)
            changed = True
            break
    if changed:
        await save_filter_log(log)
    return changed


async def load_custom_block_domains() -> set[str]:
    raw = await redis_get(REDIS_DOMAINS_KEY)
    if not raw:
        return set()
    try:
        data = json.loads(raw)
        return {normalize_domain(d) for d in data if normalize_domain(d)}
    except (json.JSONDecodeError, TypeError):
        return set()


async def save_custom_block_domains(domains: set[str]) -> None:
    await redis_set(REDIS_DOMAINS_KEY, json.dumps(sorted(domains), ensure_ascii=False))


async def load_banned_users() -> set:
    """Load the set of banned user IDs from Redis."""
    raw = await redis_get(REDIS_BANNED_KEY)
    if not raw:
        return set()
    try:
        return set(int(uid) for uid in json.loads(raw))
    except (json.JSONDecodeError, TypeError, ValueError):
        return set()


async def save_banned_users(banned: set) -> None:
    """Persist the banned user IDs set to Redis."""
    await redis_set(REDIS_BANNED_KEY, json.dumps(sorted(banned), ensure_ascii=False))


# ─── Pending clarification requests (Redis-backed) ───────────────────────────
# Maps user_id -> question number the admin is asking them to clarify.
# Consumed by handle_clarification_reply when the student replies.
async def load_awaiting_clarification() -> dict:
    raw = await redis_get(REDIS_CLARIFY_KEY)
    if not raw:
        return {}
    try:
        return {int(k): int(v) for k, v in json.loads(raw).items()}
    except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
        return {}


async def save_awaiting_clarification(data: dict) -> None:
    await redis_set(REDIS_CLARIFY_KEY, json.dumps({str(k): v for k, v in data.items()}, ensure_ascii=False))


async def active_ad_domains() -> set[str]:
    return set(AD_DOMAINS) | await load_custom_block_domains()


def normalize_domain(domain: str) -> str:
    domain = domain.strip().lower()
    domain = re.sub(r"^https?://", "", domain)
    domain = domain.split("/", 1)[0].split("?", 1)[0].strip(".")
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def safe_preview(text: str, limit: int = 120) -> str:
    text = (text or "").replace("\n", " ").strip()
    return text[:limit] + ("..." if len(text) > limit else "")


async def post_to_group(
    context: ContextTypes.DEFAULT_TYPE,
    msg_type: str,
    author_label: str,
    content: str,
    file_id: str = "",
    reply_markup=None,
) -> tuple[int, int | None]:
    """
    Post a question to the group channel.
    Returns (primary_message_id, secondary_message_id_or_None).
    For photo/video that exceed the caption limit, two messages are sent —
    both IDs are returned so /delete can remove both.
    reply_markup (if given) is attached to the primary message only. Not
    currently used for posted questions — students react with Telegram's
    native 👍/👎 message reactions instead of an inline button, to keep
    the chat compact. Kept as a parameter in case a future feature needs
    an inline keyboard here.
    """
    text = f"{author_label}\n\n{content}".strip()
    if msg_type == "text":
        if len(text) <= TELEGRAM_TEXT_LIMIT:
            msg = await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=text, reply_markup=reply_markup)
            return msg.message_id, None
        else:
            # author_label + content together exceed Telegram's 4096-char
            # text limit (content alone is always within bounds, since
            # Telegram caps incoming messages at 4096 too) — split into two
            # messages, same pattern as the photo/video overflow case below.
            msg  = await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=author_label, reply_markup=reply_markup)
            msg2 = await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=content)
            return msg.message_id, msg2.message_id
    elif msg_type == "photo":
        if len(text) <= TELEGRAM_CAPTION_LIMIT:
            msg = await context.bot.send_photo(chat_id=GROUP_CHAT_ID, photo=file_id, caption=text, reply_markup=reply_markup)
            return msg.message_id, None
        else:
            msg  = await context.bot.send_photo(chat_id=GROUP_CHAT_ID, photo=file_id, caption=author_label, reply_markup=reply_markup)
            msg2 = await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=content)
            return msg.message_id, msg2.message_id
    elif msg_type == "video":
        if len(text) <= TELEGRAM_CAPTION_LIMIT:
            msg = await context.bot.send_video(chat_id=GROUP_CHAT_ID, video=file_id, caption=text, reply_markup=reply_markup)
            return msg.message_id, None
        else:
            msg  = await context.bot.send_video(chat_id=GROUP_CHAT_ID, video=file_id, caption=author_label, reply_markup=reply_markup)
            msg2 = await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=content)
            return msg.message_id, msg2.message_id
    elif msg_type == "voice":
        msg = await context.bot.send_voice(chat_id=GROUP_CHAT_ID, voice=file_id, caption=author_label, reply_markup=reply_markup)
        return msg.message_id, None
    else:
        raise ValueError(f"Unsupported message type: {msg_type}")


def is_rate_limited(user_id: int) -> int:
    now = datetime.now()
    last = last_submit_at.get(user_id)
    if not last:
        return 0
    elapsed = (now - last).total_seconds()
    remaining = int(RATE_LIMIT_SECONDS - elapsed)
    return max(0, remaining)


def mark_submit(user_id: int) -> None:
    last_submit_at[user_id] = datetime.now()

confession_count = 0  # loaded from Redis in main() — do not use before that

# ─── Pending reviews — loaded from Redis on startup ──────────────────────────
# Persisted to Redis after every change so reviews survive bot restarts.
pending_reviews: dict = {}   # populated in main() after Redis is ready

# ─── Banned users — loaded from Redis on startup ─────────────────────────────
banned_users: set = set()    # populated in main() after Redis is ready

# ─── Pending clarification requests — loaded from Redis on startup ──────────
awaiting_clarification: dict = {}   # populated in main() after Redis is ready

# ─── General helpers ──────────────────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return bool(ADMIN_CHAT_ID) and user_id == ADMIN_CHAT_ID

def is_banned(user_id: int) -> bool:
    return user_id in banned_users

def choice_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🤫 Post Anonymously", callback_data="post_anonymous"),
            InlineKeyboardButton("👤 Post Publicly",    callback_data="post_public"),
        ],
        [
            InlineKeyboardButton("✏️ Edit",   callback_data="edit_confession"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel"),
        ],
    ])


# ─── Known ad/spam domains — checked instantly before AI ─────────────────────
# Add any domain you keep seeing spammed in confessions.
AD_DOMAINS = {
    # RedNote / XHS
    "xhslink.com",
    "xiaohongshu.com",
    "xhs.link",
    # Malaysian e-commerce
    "shopee.com.my",
    "lazada.com.my",
    "temu.com",
    # Generic shorteners often used for referral spam
    "bit.ly",
    "tinyurl.com",
    "t.co",
    "rb.gy",
    "shorturl.at",
}


async def extract_urls(text: str) -> list:
    """
    Return all URLs found in text.
    Catches both explicit https:// links AND bare domain links
    (e.g. xhslink.com/abc posted without a protocol prefix).
    """
    # Standard http/https links
    explicit = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+', text)

    # Bare known-ad domains without protocol (e.g. "xhslink.com/abc123")
    domains = await active_ad_domains()
    bare = []
    if domains:
        bare_pattern = r'\b(?:' + '|'.join(re.escape(d) for d in domains) + r')[^\s]*'
        bare = re.findall(bare_pattern, text, re.IGNORECASE)

    # Deduplicate while preserving order
    seen = set()
    result = []
    for u in explicit + bare:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


async def is_blocklisted(urls: list) -> bool:
    """
    Return True if any URL belongs to a known ad/spam domain.
    Handles both https://domain/path and bare domain/path formats.
    """
    if not urls:
        return False
    domains = await active_ad_domains()  # fetched once, not once per URL
    for url in urls:
        # Try to extract domain from URL with or without protocol
        m = re.search(r'(?:https?://)?([^/\s?#]+)', url, re.IGNORECASE)
        if not m:
            continue
        domain = normalize_domain(m.group(1))
        for ad_domain in domains:
            if domain == ad_domain or domain.endswith("." + ad_domain):
                logger.info("Blocklist hit: domain=%s matched rule=%s", domain, ad_domain)
                return True
    return False

# ─── AI Filter ────────────────────────────────────────────────────────────────
CATEGORY_LABELS = {
    "ad":       "📢 Advertisement / Promo Spam",
    "phishing": "🎣 Phishing / Scam Link",
    "spam":     "🚫 Spam",
}

FILTER_CONFIDENCE_THRESHOLD = 0.60  # Flag when AI is ≥60% sure (lowered from 0.75)


async def check_urls_serper(urls: list) -> list:
    """
    Query Serper (Google Search API) for reputation data on extracted URLs.
    Caps at 2 URLs to keep latency acceptable.
    Returns a list of {url, domain, snippets} dicts.
    """
    if not SERPER_API_KEY or not urls:
        return []

    results = []
    for url in urls[:2]:
        try:
            m      = re.search(r'https?://([^/\s?#]+)', url)
            domain = m.group(1) if m else url

            def _search(d=domain):
                return http_requests.post(
                    "https://google.serper.dev/search",
                    headers={
                        "X-API-KEY":    SERPER_API_KEY,
                        "Content-Type": "application/json",
                    },
                    json={"q": f"{d} phishing OR scam OR malware OR fraud", "num": 5},
                    timeout=6,
                )

            resp = await asyncio.to_thread(_search)
            resp.raise_for_status()
            data = resp.json()

            snippets = [
                r.get("snippet", "")
                for r in data.get("organic", [])[:4]
                if r.get("snippet")
            ]
            results.append({"url": url, "domain": domain, "snippets": snippets})
            logger.info("Serper checked domain: %s (%d snippets)", domain, len(snippets))

        except Exception as exc:
            logger.warning("Serper lookup failed for %s: %s", url, exc)

    return results


async def run_ai_filter(content: str, url_reputation: list = None) -> dict:
    """
    Send question text + optional URL reputation context to Groq for moderation.

    Returns:
        {"flagged": bool, "category": str, "reason": str, "confidence": float}

    Fails OPEN (returns clean) if the API is unavailable — questions are never
    silently lost due to a filter outage.
    """
    if not groq_client:
        return {"flagged": False, "category": "clean", "reason": "AI filter not configured", "confidence": 0.0}

    url_block = ""
    if url_reputation:
        url_block = "\n\n[Live URL Reputation — sourced from Google Search]\n"
        for item in url_reputation:
            url_block += f"\nDomain: {item['domain']}\n"
            for snippet in item.get("snippets", []):
                url_block += f"  • {snippet}\n"

    system_prompt = (
        "You are a precision content-moderation classifier for a Telegram anonymous classroom Q&A bot.\n\n"
        "Your job is NOT to judge whether a question is awkward, off-topic, or poorly phrased. "
        "Student questions, even casual or unrelated ones, are clean unless they are clearly ad "
        "spam or phishing/scam content.\n\n"
        "Allowed categories:\n"
        "  clean: normal student question content, including casual brand mentions without promotion.\n"
        "  ad: unsolicited promotion, referral/affiliate spam, MLM recruitment, product/service selling, "
        "traffic-driving social/shop links, discount codes, repeated copy-paste marketing, or calls to DM/buy/join.\n"
        "  phishing: scam, malware, fake prize/job/investment offer, credential harvesting, financial fraud, "
        "or a URL with strong scam/phishing reputation evidence.\n\n"
        "Decision rules:\n"
        "  - Do not flag just because a message mentions Shopee, Lazada, TikTok, RedNote/XHS, a brand, or a price.\n"
        "  - Flag as ad when the message is trying to promote, sell, recruit, drive traffic, share a referral, "
        "or contains a product/social/referral link with promotional intent.\n"
        "  - Flag XHS/xiaohongshu/RedNote, ecommerce, shortener, referral, affiliate, or shop links as ad unless "
        "the surrounding confession is clearly discussing them without promotion.\n"
        "  - Flag as phishing only when there is scam/fraud language, suspicious financial/credential claims, "
        "or URL reputation snippets strongly indicate phishing, malware, fraud, or scam.\n"
        "  - If evidence is weak or ambiguous, return clean with confidence below 0.60 instead of guessing.\n"
        "  - confidence must be a number from 0.0 to 1.0 reflecting evidence strength.\n\n"
        "Return ONLY this raw JSON object shape, with no markdown and no extra text:\n"
        '{"flagged": false, "category": "clean", "reason": "brief evidence-based reason", "confidence": 0.0}'
    )

    user_msg = f"Question to analyze:\n\n{content}{url_block}"

    raw = ""
    def _call():
        return groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_msg},
            ],
            temperature=0,
            max_tokens=150,
        )

    try:
        resp = await asyncio.to_thread(_call)
        raw  = resp.choices[0].message.content.strip()

        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
            raw = re.sub(r"\n?```\s*$",           "", raw)
        raw = raw.strip()

        result = json.loads(raw)
        category = str(result.get("category", "clean")).lower().strip()
        if category not in {"clean", "ad", "phishing", "spam"}:
            category = "clean"
        if category == "spam":
            category = "ad"
        try:
            confidence = max(0.0, min(1.0, float(result.get("confidence", 0.0))))
        except (TypeError, ValueError):
            confidence = 0.0
        result["category"] = category
        result["confidence"] = confidence
        result["flagged"] = (
            str(result.get("flagged", False)).lower() == "true"
            and category in {"ad", "phishing"}
        )
        return result

    except json.JSONDecodeError:
        logger.warning("Groq returned non-JSON (letting through): %.300s", raw)
        return {"flagged": False, "category": "clean", "reason": "parse error", "confidence": 0.0}
    except Exception as exc:
        logger.error("Groq filter error (letting through): %s", exc)
        return {"flagged": False, "category": "clean", "reason": str(exc), "confidence": 0.0}


# ─── NEW Feature 1: apply_filter now queues for review instead of auto-deleting
async def apply_filter(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    content: str,
) -> bool:
    """
    Full filter pipeline: Serper URL lookup → Groq AI analysis.

    CHANGED BEHAVIOUR (Feature 1):
      HIGH-RISK result → message is forwarded to admin for manual review.
      The bot no longer auto-deletes. Admin sees Approve / Reject buttons.

    Returns True  → question is pending admin review (caller ends conversation).
    Returns False → message is clean / filter is off (caller should proceed).
    """
    if not content or content == "(voice message)":
        return False

    urls = await extract_urls(content)
    blocklisted = await is_blocklisted(urls)
    if not FILTER_ENABLED and not blocklisted:
        return False

    checking_msg = await update.message.reply_text(
        "🔍 _Scanning your question…_", parse_mode="Markdown"
    )

    # ── Fast blocklist check — no AI cost, instant result ─────────────────
    if blocklisted:
        try:
            await checking_msg.delete()
        except Exception:
            pass
        category   = "ad"
        confidence = 1.0
        reason     = "Domain is on the known ad/spam blocklist (e.g. XHS, Shopee referral link)."
        category_label = CATEGORY_LABELS.get(category, "⚠️ Policy Violation")
        user           = update.effective_user
        username_str   = f"@{user.username}" if user.username else "no username"
        timestamp      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        review_id      = uuid.uuid4().hex[:12]

        pending_reviews[review_id] = {
            "user_id":            user.id,
            "chat_id":            update.effective_chat.id,
            "full_name":          user.full_name,
            "username_str":       username_str,
            "user_data_snapshot": dict(context.user_data),
            "category":           category,
            "confidence":         confidence,
            "reason":             reason,
            "content":            content,
            "timestamp":          timestamp,
            "status":             STATUS_PENDING_APPROVAL,
        }
        await save_pending_reviews(pending_reviews)

        logger.info(
            "BLOCKLIST HIT | %-10s | 100%% | id=%s | user_id=%d | %s",
            category, review_id, user.id, user.full_name,
        )

        review_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Approve (Anon)",   callback_data=f"rev_anon_{review_id}"),
                InlineKeyboardButton("✅ Approve (Public)", callback_data=f"rev_pub_{review_id}"),
            ],
            [InlineKeyboardButton("❌ Reject", callback_data=f"rev_rej_{review_id}")],
        ])

        if ADMIN_CHAT_ID:
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_CHAT_ID,
                    text=(
                        f"🚫 Blocklisted Domain Detected — Pending Review\n"
                        f"{'━' * 28}\n"
                        f"👤 Name:       {user.full_name}\n"
                        f"🔗 Username:   {username_str}\n"
                        f"🆔 User ID:    {user.id}\n"
                        f"🕐 Time:       {timestamp}\n"
                        f"⚠️ Category:   {category_label}\n"
                        f"🤖 Reason:     {reason}\n"
                        f"📊 Confidence: 100% (blocklist)\n"
                        f"{'━' * 28}\n"
                        f"📝 Question:\n{content}"
                    ),
                    reply_markup=review_keyboard,
                )
            except Exception as exc:
                logger.warning("Failed to send blocklist review to admin: %s", exc)

        await append_filter_log({
            "timestamp":  timestamp,
            "user_id":    user.id,
            "full_name":  user.full_name,
            "username":   username_str,
            "category":   category,
            "reason":     reason,
            "confidence": confidence,
            "content":    content,
            "status":     STATUS_PENDING_APPROVAL,
            "review_id":  review_id,
            "method":     "blocklist",
        })

        await append_log({
            "number":     None,
            "timestamp":  timestamp,
            "post_type":  None,
            "user_id":    user.id,
            "full_name":  user.full_name,
            "username":   username_str,
            "content":    content,
            "status":     STATUS_PENDING_APPROVAL,
            "review_id":  review_id,
            "moderation": {
                "category": category,
                "confidence": confidence,
                "reason": reason,
                "method": "blocklist",
            },
        })

        await update.message.reply_text(
            "⏳ *Your question is under admin review.*\n\n"
            "An admin will look at it shortly and you'll be notified of the outcome.\n"
            "Type /start if you'd like to submit a different question in the meantime.",
            parse_mode="Markdown",
        )
        context.user_data.clear()
        return True

    # ── AI filter (runs only if blocklist passed) ──────────────────────────
    url_rep       = await check_urls_serper(urls)
    filter_result = await run_ai_filter(content, url_rep)

    try:
        await checking_msg.delete()
    except Exception:
        pass

    confidence = filter_result.get("confidence", 0.0)
    if not filter_result.get("flagged") or confidence < FILTER_CONFIDENCE_THRESHOLD:
        return False  # ✅ Clean — let it through

    # ── High-risk detected ─────────────────────────────────────────────────
    category       = filter_result.get("category", "spam")
    reason         = filter_result.get("reason", "Policy violation detected.")
    category_label = CATEGORY_LABELS.get(category, "⚠️ Policy Violation")
    user           = update.effective_user
    username_str   = f"@{user.username}" if user.username else "no username"
    timestamp      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 12-char hex ID — well within Telegram's 64-byte callback_data limit
    # even with the longest prefix (rev_anon_ = 9 chars + 12 = 21 chars total)
    review_id = uuid.uuid4().hex[:12]

    # Snapshot context.user_data NOW (type, content, file_id) before we clear it.
    # The admin approval handler will use this snapshot to post the confession.
    pending_reviews[review_id] = {
        "user_id":            user.id,
        "chat_id":            update.effective_chat.id,
        "full_name":          user.full_name,
        "username_str":       username_str,
        "user_data_snapshot": dict(context.user_data),   # shallow copy is enough
        "category":           category,
        "confidence":         confidence,
        "reason":             reason,
        "content":            content,
        "timestamp":          timestamp,
        "status":             STATUS_PENDING_APPROVAL,
    }
    await save_pending_reviews(pending_reviews)

    logger.info(
        "PENDING REVIEW | %-10s | %.0f%% | id=%s | user_id=%d | %s",
        category, confidence * 100, review_id, user.id, user.full_name,
    )

    # ── Send admin a review card with Approve / Reject inline buttons ──────
    review_keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve (Anon)",   callback_data=f"rev_anon_{review_id}"),
            InlineKeyboardButton("✅ Approve (Public)", callback_data=f"rev_pub_{review_id}"),
        ],
        [InlineKeyboardButton("❌ Reject", callback_data=f"rev_rej_{review_id}")],
    ])

    if ADMIN_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"⚠️ High-Risk Question — Pending Your Review\n"
                    f"{'━' * 28}\n"
                    f"👤 Name:       {user.full_name}\n"
                    f"🔗 Username:   {username_str}\n"
                    f"🆔 User ID:    {user.id}\n"
                    f"🕐 Time:       {timestamp}\n"
                    f"⚠️ Category:   {category_label}\n"
                    f"🤖 AI Reason:  {reason}\n"
                    f"📊 Confidence: {confidence:.0%}\n"
                    f"{'━' * 28}\n"
                    f"📝 Question:\n{content}"
                ),
                reply_markup=review_keyboard,
            )
        except Exception as exc:
            logger.warning("Failed to send review request to admin: %s", exc)

    # ── Log as pending_approval (status field added for filter_stats) ──────
    await append_filter_log({
        "timestamp":  timestamp,
        "user_id":    user.id,
        "full_name":  user.full_name,
        "username":   username_str,
        "category":   category,
        "reason":     reason,
        "confidence": confidence,
        "content":    content,
        "status":     STATUS_PENDING_APPROVAL,
        "review_id":  review_id,
    })

    await append_log({
        "number":     None,
        "timestamp":  timestamp,
        "post_type":  None,
        "user_id":    user.id,
        "full_name":  user.full_name,
        "username":   username_str,
        "content":    content,
        "status":     STATUS_PENDING_APPROVAL,
        "review_id":  review_id,
        "moderation": {
            "category": category,
            "confidence": confidence,
            "reason": reason,
            "method": "ai",
        },
    })

    # ── Tell the user their question is under review ─────────────────────
    await update.message.reply_text(
        "⏳ *Your question is under admin review.*\n\n"
        "An admin will look at it shortly and you'll be notified of the outcome.\n"
        "Type /start if you'd like to submit a different question in the meantime.",
        parse_mode="Markdown",
    )

    context.user_data.clear()
    return True  # Conversation ends here; resumes via admin decision


# ─── NEW Feature 1: Admin review callback handler ─────────────────────────────
async def handle_admin_review(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles the admin pressing ✅ Approve (Anon/Public) or ❌ Reject on a
    pending high-risk question review card.

    Callback data formats (all safely under Telegram's 64-byte limit):
        rev_anon_<12-hex>   → approve and post anonymously
        rev_pub_<12-hex>    → approve and post publicly
        rev_rej_<12-hex>    → reject (notify user, discard question)
    """
    query = update.callback_query
    admin_user = update.effective_user

    if not admin_user or not is_admin(admin_user.id):
        await query.answer("Admins only.", show_alert=True)
        return

    await query.answer()

    data = query.data

    # ── Parse action and review_id ─────────────────────────────────────────
    if data.startswith("rev_anon_"):
        action    = "approve_anon"
        review_id = data[len("rev_anon_"):]
    elif data.startswith("rev_pub_"):
        action    = "approve_pub"
        review_id = data[len("rev_pub_"):]
    elif data.startswith("rev_rej_"):
        action    = "reject"
        review_id = data[len("rev_rej_"):]
    else:
        return  # Not our callback — let other handlers try

    # Pop from dict so double-clicks are safely ignored
    review = pending_reviews.pop(review_id, None)
    if review:
        await save_pending_reviews(pending_reviews)  # persist removal to Redis
    if not review:
        await query.answer("⚠️ Already handled or expired.", show_alert=True)
        try:
            await query.edit_message_reply_markup(reply_markup=None)  # remove buttons
        except Exception:
            pass
        return

    user_chat_id  = review["chat_id"]
    ud            = review["user_data_snapshot"]
    timestamp     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    original_text = query.message.text or ""

    # ── REJECT ────────────────────────────────────────────────────────────
    if action == "reject":
        # DM the user
        try:
            await context.bot.send_message(
                chat_id=user_chat_id,
                text=(
                    "❌ *Your question was reviewed and could not be approved.*\n\n"
                    "Type /start if you'd like to submit a different question."
                ),
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Could not DM rejection to user: %s", exc)

        # Update the review card (removes buttons, appends status)
        try:
            await query.edit_message_text(
                original_text + "\n\n❌ Rejected by admin.",
            )
        except Exception as exc:
            logger.warning("Could not edit review card after rejection: %s", exc)

        await update_log_by_review_id(review_id, {
            "status": STATUS_REJECTED,
            "rejected_at": timestamp,
            "rejected_by": admin_user.id if admin_user else None,
        })
        await update_filter_log_by_review_id(review_id, {
            "status": STATUS_REJECTED,
            "reviewed_at": timestamp,
            "reviewed_by": admin_user.id if admin_user else None,
        })
        logger.info("Admin REJECTED review_id=%s (user_id=%d)", review_id, review["user_id"])
        return

    # ── APPROVE ───────────────────────────────────────────────────────────
    is_anonymous = (action == "approve_anon")
    new_count = await incr_count()
    post_type = "Anonymous" if is_anonymous else "Public"

    if is_anonymous:
        author_label = f"Anonymous Question #{new_count}"
    else:
        # Use the stored username/name from when the question was submitted
        display_name = (
            review["username_str"]
            if review["username_str"] != "no username"
            else review["full_name"]
        )
        author_label = f"👤 Question #{new_count} by {display_name}"

    msg_type = ud.get("type")
    content  = ud.get("content", "")
    file_id  = ud.get("file_id", "")

    try:
        # Post to channel — no inline keyboard; students react with 👍/👎
        # directly on the message using Telegram's native reactions.
        msg_id, msg_id_2 = await post_to_group(
            context, msg_type, author_label, content, file_id,
        )

        # DM the user with the approval news
        try:
            await context.bot.send_message(
                chat_id=user_chat_id,
                text=(
                    f"✅ *Your question #{new_count} was approved and posted!*\n\n"
                    "Type /start to send another question."
                ),
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Could not DM approval to user: %s", exc)

        # Update the pending database/log row instead of creating a duplicate.
        saved = await update_log_by_review_id(review_id, {
            "number":    new_count,
            "timestamp": timestamp,
            "post_type": post_type,
            "user_id":   review["user_id"],
            "full_name": review["full_name"],
            "username":  review["username_str"],
            "content":   content,
            "status":    STATUS_PUBLISHED,
            "approved_at": timestamp,
            "approved_by": admin_user.id if admin_user else None,
            "message_id":  msg_id,
            "msg_type":     msg_type,
            "author_label": author_label,
            **( {"message_id_2": msg_id_2} if msg_id_2 else {} ),
            "note":      (
                f"admin-approved after AI flagged as {review['category']} "
                f"({review['confidence']:.0%} confidence)"
            ),
        })
        if not saved:
            await append_log({
                "number":    new_count,
                "timestamp": timestamp,
                "post_type": post_type,
                "user_id":   review["user_id"],
                "full_name": review["full_name"],
                "username":  review["username_str"],
                "content":   content,
                "status":    STATUS_PUBLISHED,
                "review_id": review_id,
                "approved_at": timestamp,
                "approved_by": admin_user.id if admin_user else None,
                "message_id":  msg_id,
                "msg_type":     msg_type,
                "author_label": author_label,
                **( {"message_id_2": msg_id_2} if msg_id_2 else {} ),
                "note":      (
                    f"admin-approved after AI flagged as {review['category']} "
                    f"({review['confidence']:.0%} confidence)"
                ),
            })
        await update_filter_log_by_review_id(review_id, {
            "status": STATUS_PUBLISHED,
            "reviewed_at": timestamp,
            "reviewed_by": admin_user.id if admin_user else None,
            "posted_number": new_count,
        })

        # Update the review card in admin chat (removes buttons, appends status)
        try:
            await query.edit_message_text(
                original_text
                + f"\n\n✅ Approved — posted as Question #{new_count} ({post_type}).",
            )
        except Exception as exc:
            logger.warning("Could not edit review card after approval: %s", exc)

        logger.info(
            "Admin APPROVED review_id=%s → Question #%d | %s | user_id=%d",
            review_id, new_count, post_type, review["user_id"],
        )

    except Exception as exc:
        logger.error("Failed to post admin-approved question: %s", exc)
        await decr_count()
        pending_reviews[review_id] = review
        await save_pending_reviews(pending_reviews)
        try:
            await query.edit_message_text(
                original_text
                + "\n\n⚠️ Post failed. This item was returned to pending review. Check bot logs for the exact Telegram error.",
            )
        except Exception:
            pass


# ─── /start ───────────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if is_banned(user.id):
        await update.message.reply_text("🚫 You are not allowed to submit questions.")
        return ConversationHandler.END
    remaining = is_rate_limited(user.id)
    if remaining:
        await update.message.reply_text(
            f"⏳ Please wait {remaining}s before sending another question."
        )
        return ConversationHandler.END
    # Starting a fresh question supersedes any pending clarification request
    if awaiting_clarification.pop(user.id, None) is not None:
        await save_awaiting_clarification(awaiting_clarification)
    context.user_data.clear()
    await update.message.reply_text(
        "🤫 *Anonymous Q&A*\n\n"
        "Go ahead — type your question now 👇\n\n"
        "_You can send text, a photo, video, or voice message._\n\n"
        "Type /cancel at any time to cancel.",
        parse_mode="Markdown",
    )
    return WAITING_CONFESSION

# ─── Receive confession content ───────────────────────────────────────────────
async def receive_confession(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    msg = update.message
    ud  = context.user_data
    user = update.effective_user

    # ── Ban check ──────────────────────────────────────────────────────────
    if is_banned(user.id):
        await update.message.reply_text("🚫 You are not allowed to submit questions.")
        context.user_data.clear()
        return ConversationHandler.END

    # ── Rate limit (skipped when the user clicked Edit to rewrite) ─────────
    if not context.user_data.pop("editing", False):
        remaining = is_rate_limited(user.id)
        if remaining:
            await update.message.reply_text(
                f"Please wait {remaining}s before sending another question."
            )
            return WAITING_CONFESSION

    if msg.text:
        ud["type"]    = "text"
        ud["content"] = msg.text
        preview       = safe_preview(msg.text)

    elif msg.photo:
        ud["type"]    = "photo"
        ud["file_id"] = msg.photo[-1].file_id
        ud["content"] = msg.caption or ""
        preview       = "📷 Photo" + (f": {safe_preview(msg.caption)}" if msg.caption else "")

    elif msg.video:
        ud["type"]    = "video"
        ud["file_id"] = msg.video.file_id
        ud["content"] = msg.caption or ""
        preview       = "🎥 Video" + (f": {safe_preview(msg.caption)}" if msg.caption else "")

    elif msg.voice:
        ud["type"]    = "voice"
        ud["file_id"] = msg.voice.file_id
        ud["content"] = "(voice message)"
        preview       = "🎤 Voice message"

    else:
        await update.message.reply_text(
            "⚠️ I can only forward text, photos, videos, and voice messages.\n"
            "Please try again."
        )
        return WAITING_CONFESSION

    # ── AI Filter (skips voice — no text to analyze) ───────────────────────
    blocked = await apply_filter(update, context, ud.get("content", ""))
    if blocked:
        mark_submit(user.id)
        return ConversationHandler.END

    # ── All clear — show posting options ───────────────────────────────────
    await update.message.reply_text(
        f"📝 Your question:\n{preview}\n\n"
        "How do you want to post this?",
        reply_markup=choice_keyboard(),
    )
    mark_submit(user.id)
    return WAITING_CHOICE

# ─── Handle button click ──────────────────────────────────────────────────────
async def handle_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    global confession_count

    query = update.callback_query
    await query.answer()

    choice = query.data
    user   = update.effective_user
    ud     = context.user_data

    # ── Cancel ─────────────────────────────────────────────────────────────
    if choice == "cancel":
        await query.edit_message_text("❌ Cancelled.\n\nType /start to send a new question.")
        context.user_data.clear()
        return ConversationHandler.END

    # ── Edit — let the user rewrite before posting ─────────────────────────
    if choice == "edit_confession":
        await query.edit_message_text(
            "✏️ *Edit your question*\n\n"
            "Send your updated question now 👇\n\n"
            "_You can send text, a photo, video, or voice message._\n\n"
            "Type /cancel at any time to cancel.",
            parse_mode="Markdown",
        )
        context.user_data["editing"] = True
        return WAITING_CONFESSION

    # ── Guard against a fast double-tap posting the same question twice ────
    if ud.pop("posted", None):
        return ConversationHandler.END
    ud["posted"] = True

    # ── Build label shown in channel ───────────────────────────────────────
    is_anonymous = (choice == "post_anonymous")
    confession_count = await incr_count()

    if is_anonymous:
        author_label = f"Anonymous Question #{confession_count}"
    else:
        display_name = f"@{user.username}" if user.username else user.full_name
        author_label = f"👤 Question #{confession_count} by {display_name}"

    msg_type = ud.get("type")
    content  = ud.get("content", "")
    file_id  = ud.get("file_id", "")

    try:
        # ── Post to channel — no inline keyboard; students react with 👍/👎
        #    directly on the message using Telegram's native reactions. ──────
        msg_id, msg_id_2 = await post_to_group(
            context, msg_type, author_label, content, file_id,
        )

        # ── Silent admin notification ──────────────────────────────────────
        timestamp    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        username_str = f"@{user.username}" if user.username else "no username"
        post_type    = "Anonymous" if is_anonymous else "Public"

        if ADMIN_CHAT_ID:
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_CHAT_ID,
                    text=(
                        f"🔔 New Question #{confession_count} ({post_type})\n"
                        f"{'━' * 28}\n"
                        f"👤 Name: {user.full_name}\n"
                        f"🔗 Username: {username_str}\n"
                        f"🆔 User ID: {user.id}\n"
                        f"🕐 Time: {timestamp}\n"
                        f"{'━' * 28}\n"
                        f"📝 Question:\n{content}"
                    ),
                )
            except Exception as exc:
                logger.warning("Failed to send admin notification for question #%d: %s", confession_count, exc)

        # ── Save to log (including message IDs for /delete and /answer) ───
        log_entry: dict = {
            "number":     confession_count,
            "timestamp":  timestamp,
            "post_type":  post_type,
            "user_id":    user.id,
            "full_name":  user.full_name,
            "username":   username_str,
            "content":    content,
            "status":     STATUS_PUBLISHED,
            "message_id": msg_id,
            "msg_type":     msg_type,
            "author_label": author_label,
        }
        if msg_id_2:
            log_entry["message_id_2"] = msg_id_2
        await append_log(log_entry)

        # ── Confirm to user ────────────────────────────────────────────────
        await query.edit_message_text(
            f"✅ *Question #{confession_count} posted!*\n\n"
            f"Type /start to send another question.",
            parse_mode="Markdown",
        )
        logger.info(
            "Question #%d | %s | user_id=%d | %s",
            confession_count, post_type, user.id, user.full_name,
        )

    except Exception as exc:
        logger.error("Failed to post question: %s", exc)
        await decr_count()
        await query.edit_message_text(
            "❌ Couldn't post your question.\n"
            "The item was not published. Please try again later."
        )

    context.user_data.clear()
    return ConversationHandler.END

# ─── /cancel command ──────────────────────────────────────────────────────────
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("❌ Cancelled.\n\nType /start to send a new question.")
    context.user_data.clear()
    return ConversationHandler.END

# ─── Message outside conversation ─────────────────────────────────────────────
async def prompt_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Type /start to ask a question.")

# ─── Admin: /getid ────────────────────────────────────────────────────────────
async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"Chat ID: `{update.effective_chat.id}`",
        parse_mode="Markdown",
    )

# ─── Admin: /lookup <number> ──────────────────────────────────────────────────
async def lookup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /lookup 5")
        return

    try:
        target = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Please provide a valid number.")
        return

    entry = next((e for e in await load_log() if e.get("number") == target), None)

    if not entry:
        await update.message.reply_text(f"❌ No record for Question #{target}.")
        return

    await update.message.reply_text(
        f"🔍 Question #{target} — Asker Info\n"
        f"{'━' * 28}\n"
        f"📌 Type: {entry.get('post_type', '?')}\n"
        f"👤 Name: {entry.get('full_name', '?')}\n"
        f"🔗 Username: {entry.get('username', '?')}\n"
        f"🆔 User ID: {entry.get('user_id', '?')}\n"
        f"🕐 Sent at: {entry.get('timestamp', '?')}\n"
        f"Status: {entry.get('status', 'unknown')}\n"
        f"{'━' * 28}\n"
        f"📝 Question:\n{entry.get('content', '(media)')}",
    )

# ─── Admin: /filter_stats ─────────────────────────────────────────────────────
async def filter_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show how many questions have been flagged for review and by which category."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    log   = await load_filter_log()
    total = len(log)

    if total == 0:
        await update.message.reply_text(
            "✅ No questions have been flagged by the AI filter yet.\n"
            f"Filter status: {'🟢 Active' if FILTER_ENABLED else '🔴 Disabled (set GROQ_API_KEY)'}"
        )
        return

    cats  = Counter(e.get("category", "unknown") for e in log)
    lines = [
        "🚫 *AI Filter Statistics*",
        f"{'━' * 28}",
        f"Status: {'🟢 Active' if FILTER_ENABLED else '🔴 Disabled'}",
        f"Total flagged for review: *{total}*\n",
    ]

    for cat, count in cats.most_common():
        label = CATEGORY_LABELS.get(cat, cat)
        lines.append(f"{label}: *{count}*")

    recent = log[-1]
    lines += [
        f"\n{'━' * 28}",
        f"Last flagged: {recent['timestamp']}",
        f"Category: {CATEGORY_LABELS.get(recent.get('category', ''), '?')}",
        f"Confidence: {recent.get('confidence', 0):.0%}",
        f"Status: {recent.get('status', 'unknown')}",
    ]

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ─── NEW Admin: /pending — list questions currently awaiting review ──────────
async def list_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Show all questions currently sitting in the pending_reviews queue.
    Useful if the admin missed a review notification.
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    if not pending_reviews:
        await update.message.reply_text("✅ No questions are pending review right now.")
        return

    lines = [f"⏳ Pending Reviews ({len(pending_reviews)})", f"{'━' * 28}"]
    for rid, r in pending_reviews.items():
        cat_label = CATEGORY_LABELS.get(r["category"], r["category"])
        lines.append(
            f"🆔 {rid} | {r['full_name']} | {cat_label} | {r['timestamp']}"
        )
    lines.append(f"\n{'━' * 28}")
    lines.append("Use /review <id> to resend the approval buttons for a pending question.")

    await update.message.reply_text("\n".join(lines))


async def resend_review(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Resend the Approve / Reject card for a pending review id."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /review <pending_id>")
        return

    review_id = context.args[0].strip()
    review = pending_reviews.get(review_id)
    if not review:
        await update.message.reply_text("❌ No pending review found for that ID.")
        return

    category = review.get("category", "unknown")
    category_label = CATEGORY_LABELS.get(category, category)
    try:
        confidence = float(review.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    method = review.get("moderation", {}).get("method") or review.get("method") or "moderation"

    review_keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve (Anon)",   callback_data=f"rev_anon_{review_id}"),
            InlineKeyboardButton("✅ Approve (Public)", callback_data=f"rev_pub_{review_id}"),
        ],
        [InlineKeyboardButton("❌ Reject", callback_data=f"rev_rej_{review_id}")],
    ])

    await update.message.reply_text(
        (
            f"⚠️ Pending Question Review\n"
            f"{'━' * 28}\n"
            f"🆔 Review ID:  {review_id}\n"
            f"👤 Name:       {review.get('full_name', '?')}\n"
            f"🔗 Username:   {review.get('username_str', '?')}\n"
            f"🆔 User ID:    {review.get('user_id', '?')}\n"
            f"🕐 Time:       {review.get('timestamp', '?')}\n"
            f"⚠️ Category:   {category_label}\n"
            f"🤖 Reason:     {review.get('reason', '?')}\n"
            f"📊 Confidence: {confidence:.0%} ({method})\n"
            f"{'━' * 28}\n"
            f"📝 Question:\n{review.get('content', '')}"
        ),
        reply_markup=review_keyboard,
    )


# ─── Admin: /ban <user_id> ────────────────────────────────────────────────────
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Permanently block a user from submitting questions."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id>\n\nGet the user ID from /lookup <#> or the admin notification.")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Please provide a valid numeric user ID.")
        return

    banned_users.add(target_id)
    await save_banned_users(banned_users)
    logger.info("Admin BANNED user_id=%d", target_id)
    await update.message.reply_text(
        f"✅ User `{target_id}` has been banned.\n"
        "They will no longer be able to submit questions.\n"
        "Use /unban <user_id> to reverse this.",
        parse_mode="Markdown",
    )


# ─── Admin: /unban <user_id> ──────────────────────────────────────────────────
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a user from the ban list."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Please provide a valid numeric user ID.")
        return

    if target_id not in banned_users:
        await update.message.reply_text(f"ℹ️ User `{target_id}` is not currently banned.", parse_mode="Markdown")
        return

    banned_users.discard(target_id)
    await save_banned_users(banned_users)
    logger.info("Admin UNBANNED user_id=%d", target_id)
    await update.message.reply_text(
        f"✅ User `{target_id}` has been unbanned and can submit questions again.",
        parse_mode="Markdown",
    )


# ─── Admin: /delete <confession_number> ───────────────────────────────────────
async def delete_confession(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Delete a posted question from the group channel by its question number.
    Also removes the paired overflow-text message and answer reply, if any.
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /delete <question_number>\n\nExample: /delete 42")
        return

    try:
        target = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Please provide a valid question number.")
        return

    entry = next((e for e in await load_log() if e.get("number") == target), None)
    if not entry:
        await update.message.reply_text(f"❌ No record found for Question #{target}.")
        return

    msg_id = entry.get("message_id")
    if not msg_id:
        await update.message.reply_text(
            f"❌ Question #{target} has no stored message ID.\n"
            "It may have been posted before this feature was added."
        )
        return

    deleted = []
    failed  = []

    # Delete primary message
    try:
        await context.bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=msg_id)
        deleted.append("question")
    except Exception as exc:
        failed.append(f"question ({exc})")

    # Delete overflow text message (long photo/video captions)
    msg_id_2 = entry.get("message_id_2")
    if msg_id_2:
        try:
            await context.bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=msg_id_2)
            deleted.append("overflow text")
        except Exception as exc:
            failed.append(f"overflow text ({exc})")

    # Update log entry status
    log = await load_log()
    for e in log:
        if e.get("number") == target:
            e["status"]     = "deleted"
            e["deleted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            e["deleted_by"] = update.effective_user.id
            break
    await save_log(log)

    parts = []
    if deleted:
        parts.append(f"✅ Deleted: {', '.join(deleted)}")
    if failed:
        parts.append(f"⚠️ Could not delete: {', '.join(failed)}")
    await update.message.reply_text(
        f"Question #{target}\n" + "\n".join(parts)
    )
    logger.info("Admin DELETED Question #%d (user_id=%d)", target, update.effective_user.id)


# ─── /help — user guide ───────────────────────────────────────────────────────
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show a user-facing guide for the Q&A bot."""
    await update.message.reply_text(
        "🤫 *Anonymous Q&A — Help*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📮 *How to ask a question:*\n"
        "1. Type /start to begin\n"
        "2. Send your question — text, photo, video, or voice note\n"
        "3. Choose how you want it posted\n\n"
        "🎛️ *Posting options:*\n"
        "• 🤫 *Post Anonymously* — your name stays hidden\n"
        "• 👤 *Post Publicly* — your @username is shown\n"
        "• ✏️ *Edit* — rewrite your question before posting\n"
        "• ❌ *Cancel* — discard and start over\n\n"
        "👍 *Reacting:* tap and hold (long-press) any question in the group "
        "and pick 👍 or 👎 to show it's something you'd like answered too\n\n"
        "⏱️ *Rate limit:* 5 seconds between submissions\n\n"
        "📋 *Commands:*\n"
        "/start — Ask a new question\n"
        "/cancel — Cancel your current question\n"
        "/help — Show this guide",
        parse_mode="Markdown",
    )


# ─── Teacher: /ask <#> <text> — request clarification from the asker ──────────
async def ask_clarification(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Send the original asker an anonymous DM requesting clarification on
    their question. Their next message back to the bot is automatically
    relayed to the teacher (see handle_clarification_reply).
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /ask <question_number> <what you need clarified>\n\n"
            "Example: /ask 7 Which chapter are you referring to?"
        )
        return

    try:
        target = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Please provide a valid question number.")
        return

    clarification_text = " ".join(context.args[1:]).strip()

    entry = next((e for e in await load_log() if e.get("number") == target), None)
    if not entry or not entry.get("user_id"):
        await update.message.reply_text(f"❌ No record found for Question #{target}.")
        return

    asker_id = entry["user_id"]

    try:
        await context.bot.send_message(
            chat_id=asker_id,
            text=(
                f"🧑‍🏫 Your teacher would like more details on your Question #{target}:\n\n"
                f"❓ {clarification_text}\n\n"
                "Just reply here with more info — it'll be sent straight to your teacher."
            ),
        )
    except Exception as exc:
        await update.message.reply_text(
            f"⚠️ Could not DM the asker for Question #{target}: {exc}\n"
            "They may have blocked the bot or never started a chat with it."
        )
        return

    awaiting_clarification[asker_id] = target
    await save_awaiting_clarification(awaiting_clarification)

    await update.message.reply_text(
        f"✅ Clarification request sent for Question #{target}.\n"
        "I'll forward their reply to you here as soon as they respond."
    )
    logger.info("Admin requested clarification on Question #%d from user_id=%d", target, asker_id)


# ─── Catches a student's reply to a pending /ask clarification request ────────
async def handle_clarification_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Registered in an earlier handler group than the conversation flow, so it
    sees every private message first. If the sender has a pending
    clarification request, their message is relayed to the teacher and
    consumed here (via ApplicationHandlerStop) — it never reaches the normal
    /start flow. Otherwise this is a silent no-op and the update falls
    through to the regular handlers untouched.
    """
    user = update.effective_user
    if not user or user.id not in awaiting_clarification:
        return  # not a clarification reply — let other handlers process it

    target = awaiting_clarification.pop(user.id)
    await save_awaiting_clarification(awaiting_clarification)

    reply_text = (
        update.message.text
        or update.message.caption
        or "(sent a photo/video/voice message with no caption)"
    )

    if ADMIN_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"💬 Clarification reply for Question #{target}:\n\n{reply_text}",
            )
        except Exception as exc:
            logger.warning("Failed to relay clarification reply for Question #%d: %s", target, exc)

    await update.message.reply_text("✅ Sent to your teacher. Thanks for clarifying!")

    raise ApplicationHandlerStop


# ─── Startup: load persisted state once the bot's event loop is running ──────
async def post_init(application: Application) -> None:
    """
    Runs once, automatically, after the Application's event loop starts but
    before polling begins. This is the supported place to do async setup —
    it replaces the old pattern of calling the (now-async) Redis loaders
    directly inside the synchronous main().
    """
    global confession_count, pending_reviews, banned_users, awaiting_clarification
    confession_count        = await load_count()
    pending_reviews         = await load_pending_reviews()
    banned_users            = await load_banned_users()
    awaiting_clarification  = await load_awaiting_clarification()
    logger.info(
        "Loaded from Redis — question_count=%d, pending_reviews=%d, banned_users=%d, awaiting_clarification=%d",
        confession_count, len(pending_reviews), len(banned_users), len(awaiting_clarification),
    )


# ─── Global error handler — keeps a bad update from failing silently ─────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception while processing an update", exc_info=context.error)
    if ADMIN_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"⚠️ Bot error while processing an update: {context.error}",
            )
        except Exception:
            pass  # don't let a failed error-report itself raise


# ─── Entry point ──────────────────────────────────────────────────────────────
def main() -> None:
    if not BOT_TOKEN:
        print("❌  BOT_TOKEN is not set.")
        return
    if not GROUP_CHAT_ID:
        logger.warning("GROUP_CHAT_ID is not set — approved questions have nowhere to post.")
    if not ADMIN_CHAT_ID:
        logger.warning("ADMIN_CHAT_ID is not set — admin-only commands and review queues won't work.")

    if not FILTER_ENABLED:
        logger.warning(
            "Groq AI filter is DISABLED — set GROQ_API_KEY to enable AI moderation. "
            "Blocklisted domains still go to admin review."
        )
    else:
        serper_status = (
            "Groq + Serper (URL reputation enabled)"
            if SERPER_API_KEY
            else "Groq only (no URL reputation)"
        )
        logger.info(
            "AI filter ENABLED — %s | Admin review workflow: ON",
            serper_status,
        )

    # Start Flask in background thread so Render sees an HTTP server
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask health server started on port %d", PORT)

    # Build Telegram app — post_init runs the Redis state load below once
    # the application's own event loop is up.
    telegram_app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    telegram_app.add_error_handler(error_handler)

    # ── IMPORTANT: admin review handler must be registered BEFORE the
    #   ConversationHandler so its pattern is matched first. The patterns
    #   don't overlap (rev_anon_* vs post_anonymous) so there's no conflict.
    telegram_app.add_handler(CallbackQueryHandler(
        handle_admin_review,
        pattern=r"^rev_(anon|pub|rej)_[a-f0-9]{12}$",
    ))

    # ── Confession conversation flow ───────────────────────────────────────
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            WAITING_CONFESSION: [
                MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, receive_confession)
            ],
            WAITING_CHOICE: [
                CallbackQueryHandler(handle_choice, pattern="^(post_anonymous|post_public|cancel|edit_confession)$")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_command)],
        per_user=True,
        per_chat=True,
    )

    # ── Clarification-reply catcher — runs BEFORE the conversation handler so
    #   a student's reply to a pending /ask request is intercepted first.
    #   It's a silent no-op for everyone without a pending request.
    telegram_app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, handle_clarification_reply),
        group=-1,
    )

    telegram_app.add_handler(conv_handler)
    telegram_app.add_handler(CommandHandler("getid",        get_chat_id))
    telegram_app.add_handler(CommandHandler("lookup",       lookup))
    telegram_app.add_handler(CommandHandler("filter_stats", filter_stats))
    telegram_app.add_handler(CommandHandler("pending",      list_pending))
    telegram_app.add_handler(CommandHandler("review",       resend_review))
    telegram_app.add_handler(CommandHandler("ban",          ban_user))
    telegram_app.add_handler(CommandHandler("unban",        unban_user))
    telegram_app.add_handler(CommandHandler("delete",       delete_confession))
    telegram_app.add_handler(CommandHandler("ask",          ask_clarification))
    telegram_app.add_handler(CommandHandler("help",         help_command))

    # Catch any message sent outside the /start flow
    telegram_app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & ~filters.COMMAND,
        prompt_start,
    ))

    print("🤖  Q&A bot is running.")
    # drop_pending_updates=True sends a getUpdates(timeout=0) on startup which
    # forces Telegram to close any long-poll held by a lingering old instance.
    # Without this, a Render redeploy causes a brief Conflict spam while the
    # old process is still alive alongside the new one.
    # Trade-off: messages sent during the seconds of downtime are skipped —
    # acceptable for a confession bot.
    telegram_app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()