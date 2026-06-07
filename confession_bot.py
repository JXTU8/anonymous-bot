#!/usr/bin/env python3
"""
Telegram Anonymous Confession Bot
- User must /start every time to send a confession
- After typing, two buttons appear: Post Anonymously / Post Publicly
- Admin always gets a silent DM with full sender identity
- AI filter (Groq llama-3.3-70b + Serper) queues high-risk messages for
  admin review instead of auto-deleting them          ← NEW Feature 1
- Chinese text in confessions is auto-translated to English in the channel ← NEW Feature 2
"""

import asyncio
import os
import re
import json
import logging
import threading
import uuid                          # NEW — for generating unique review IDs
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
    filters,
    ContextTypes,
)

load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID  = int(os.getenv("GROUP_CHAT_ID", "0"))
ADMIN_CHAT_ID  = int(os.getenv("ADMIN_CHAT_ID", "0"))
PORT           = int(os.getenv("PORT", 8080))
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
COUNTER_FILE   = "confession_count.json"
LOG_FILE       = "confessions_log.json"
FILTER_LOG     = "filtered_log.json"

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

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Flask health server (keeps Render free tier alive) ───────────────────────
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Confession bot is running! 🤫", 200

@flask_app.route("/health")
def health():
    return "OK", 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)

# ─── Persistence helpers ──────────────────────────────────────────────────────
def load_count() -> int:
    try:
        with open(COUNTER_FILE) as f:
            return json.load(f).get("count", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        return 0

def save_count(n: int) -> None:
    with open(COUNTER_FILE, "w") as f:
        json.dump({"count": n}, f)

def load_log() -> list:
    try:
        with open(LOG_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def append_log(entry: dict) -> None:
    log = load_log()
    log.append(entry)
    with open(LOG_FILE, "w") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)

def load_filter_log() -> list:
    try:
        with open(FILTER_LOG) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def append_filter_log(entry: dict) -> None:
    log = load_filter_log()
    log.append(entry)
    with open(FILTER_LOG, "w") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)

confession_count = load_count()

# ─── NEW: In-memory store for confessions awaiting admin review ───────────────
# Keyed by a 12-char hex review_id (generated per flagged message).
# Each entry holds everything needed to post the confession later if approved.
# Note: this is in-memory only — pending reviews are lost on bot restart.
# For persistence, you could serialise this dict to a JSON file similarly to
# the other log files above.
#
# Schema:
#   { review_id: {
#       user_id, chat_id, full_name, username_str,
#       user_data_snapshot,   # copy of context.user_data at filter time
#       category, confidence, reason, content, timestamp
#   } }
pending_reviews: dict = {}

# ─── General helpers ──────────────────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return bool(ADMIN_CHAT_ID) and user_id == ADMIN_CHAT_ID

def choice_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🤫 Post Anonymously", callback_data="post_anonymous"),
            InlineKeyboardButton("👤 Post Publicly",    callback_data="post_public"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ])

def extract_urls(text: str) -> list:
    """Return all http/https URLs found in text."""
    return re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+', text)

# ─── NEW Feature 2 helpers: Chinese detection & translation ───────────────────
def has_chinese(text: str) -> bool:
    """
    Return True if the text contains any CJK (Chinese) characters.
    Covers the three most common Unicode blocks:
      - CJK Unified Ideographs       U+4E00–U+9FFF  (most common Chinese chars)
      - CJK Extension A               U+3400–U+4DBF
      - CJK Compatibility Ideographs  U+F900–U+FAFF
    """
    return bool(re.search(r'[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]', text))


async def translate_chinese(text: str) -> str | None:
    """
    Translate text containing Chinese characters into English using Groq
    (llama-3.3-70b-versatile, same model already used for moderation).

    Returns the English translation string, or None if Groq is unavailable
    or the API call fails. Caller should handle None gracefully.
    """
    if not groq_client:
        return None

    def _call():
        return groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a professional translator. "
                        "Translate the following text to English. "
                        "Output ONLY the English translation — "
                        "no preamble, no explanations, no quotes."
                    ),
                },
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=500,
        )

    try:
        resp = await asyncio.to_thread(_call)
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        logger.warning("Chinese translation failed: %s", exc)
        return None


async def post_translation(
    context: ContextTypes.DEFAULT_TYPE,
    content: str,
    confession_num: int,
) -> None:
    """
    If `content` contains Chinese characters, translate it and post a
    bilingual follow-up message to the group channel immediately after
    the confession.

    Silently skips when:
      - content is empty or voice-only  "(voice message)"
      - no Chinese characters are found
      - translation returns None (Groq unavailable / error)
    """
    if not content or not has_chinese(content):
        return

    translation = await translate_chinese(content)
    if not translation:
        return

    try:
        await context.bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=(
                f"🌏 *Auto-Translation — Confession #{confession_num}*\n"
                f"{'━' * 28}\n"
                f"🇨🇳 *Original:*\n{content}\n\n"
                f"🇬🇧 *English:*\n{translation}"
            ),
            parse_mode="Markdown",
        )
        logger.info("Auto-translated Confession #%d (Chinese → English)", confession_num)
    except Exception as exc:
        logger.warning("Failed to post translation message: %s", exc)


# ─── AI Filter ────────────────────────────────────────────────────────────────
CATEGORY_LABELS = {
    "ad":       "📢 Advertisement / Promo Spam",
    "phishing": "🎣 Phishing / Scam Link",
    "spam":     "🚫 Spam",
}

FILTER_CONFIDENCE_THRESHOLD = 0.75  # Only flag when AI is ≥75% sure


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
    Send confession text + optional URL reputation context to Groq for moderation.

    Returns:
        {"flagged": bool, "category": str, "reason": str, "confidence": float}

    Fails OPEN (returns clean) if the API is unavailable — confessions are never
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
        "You are a strict content-moderation AI for a Telegram anonymous confession bot.\n\n"
        "Flag ONLY two categories:\n"
        "  1. AD      — unsolicited promotions, referral spam, MLM recruitment, product/service ads\n"
        "  2. PHISHING — scam/malware URLs, fake prize offers, credential-harvesting links, financial fraud\n\n"
        "Rules:\n"
        "  • Genuine personal confessions (embarrassing stories, secrets, rants, opinions) → ALWAYS clean\n"
        "  • Only flag when confidence ≥ 0.75 — when in doubt, return clean\n"
        "  • A confession that casually mentions a brand or product is NOT an ad\n"
        "  • Use the URL reputation data (if provided) as strong evidence for phishing\n\n"
        "Respond with ONLY a raw JSON object — no markdown fences, no extra text:\n"
        '{"flagged": false, "category": "clean", "reason": "...", "confidence": 0.95}'
    )

    user_msg = f"Confession to analyze:\n\n{content}{url_block}"

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
        result["flagged"] = str(result.get("flagged", False)).lower() == "true"
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

    Returns True  → confession is pending admin review (caller ends conversation).
    Returns False → message is clean / filter is off (caller should proceed).
    """
    if not FILTER_ENABLED or not content or content == "(voice message)":
        return False

    checking_msg = await update.message.reply_text(
        "🔍 _Scanning your confession…_", parse_mode="Markdown"
    )

    urls          = extract_urls(content)
    url_rep       = await check_urls_serper(urls)
    filter_result = await run_ai_filter(content, url_rep)

    await checking_msg.delete()

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
    }

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
                    f"⚠️ *High-Risk Confession — Pending Your Review*\n"
                    f"{'━' * 28}\n"
                    f"👤 Name:       {user.full_name}\n"
                    f"🔗 Username:   {username_str}\n"
                    f"🆔 User ID:    `{user.id}`\n"
                    f"🕐 Time:       {timestamp}\n"
                    f"⚠️ Category:   {category_label}\n"
                    f"🤖 AI Reason:  {reason}\n"
                    f"📊 Confidence: {confidence:.0%}\n"
                    f"{'━' * 28}\n"
                    f"📝 Content:\n{content}"
                ),
                parse_mode="Markdown",
                reply_markup=review_keyboard,
            )
        except Exception as exc:
            logger.warning("Failed to send review request to admin: %s", exc)

    # ── Log as pending_review (status field added for filter_stats) ────────
    append_filter_log({
        "timestamp":  timestamp,
        "user_id":    user.id,
        "full_name":  user.full_name,
        "username":   username_str,
        "category":   category,
        "reason":     reason,
        "confidence": confidence,
        "content":    content,
        "status":     "pending_review",   # was "blocked" in previous version
        "review_id":  review_id,
    })

    # ── Tell the user their confession is under review ─────────────────────
    await update.message.reply_text(
        "⏳ *Your confession is under admin review.*\n\n"
        "An admin will look at it shortly and you'll be notified of the outcome.\n"
        "Type /start if you'd like to submit a different confession in the meantime.",
        parse_mode="Markdown",
    )

    context.user_data.clear()
    return True  # Conversation ends here; resumes via admin decision


# ─── NEW Feature 1: Admin review callback handler ─────────────────────────────
async def handle_admin_review(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles the admin pressing ✅ Approve (Anon/Public) or ❌ Reject on a
    pending high-risk confession review card.

    Callback data formats (all safely under Telegram's 64-byte limit):
        rev_anon_<12-hex>   → approve and post anonymously
        rev_pub_<12-hex>    → approve and post publicly
        rev_rej_<12-hex>    → reject (notify user, discard confession)
    """
    global confession_count

    query = update.callback_query
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
                    "❌ *Your confession was reviewed and could not be approved.*\n\n"
                    "Type /start if you'd like to submit a different confession."
                ),
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Could not DM rejection to user: %s", exc)

        # Update the review card (removes buttons, appends status)
        try:
            await query.edit_message_text(
                original_text + "\n\n❌ *Rejected by admin.*",
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Could not edit review card after rejection: %s", exc)

        logger.info("Admin REJECTED review_id=%s (user_id=%d)", review_id, review["user_id"])
        return

    # ── APPROVE ───────────────────────────────────────────────────────────
    is_anonymous = (action == "approve_anon")
    confession_count += 1
    save_count(confession_count)
    post_type = "Anonymous" if is_anonymous else "Public"

    if is_anonymous:
        author_label = f"🤫 *Anonymous Confession #{confession_count}*"
    else:
        # Use the stored username/name from when the confession was submitted
        display_name = (
            review["username_str"]
            if review["username_str"] != "no username"
            else review["full_name"]
        )
        author_label = f"👤 *Confession #{confession_count} by {display_name}*"

    msg_type = ud.get("type")
    content  = ud.get("content", "")
    file_id  = ud.get("file_id", "")

    try:
        # Post to channel
        if msg_type == "text":
            await context.bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=f"{author_label}\n\n{content}",
                parse_mode="Markdown",
            )
        elif msg_type == "photo":
            await context.bot.send_photo(
                chat_id=GROUP_CHAT_ID,
                photo=file_id,
                caption=f"{author_label}\n\n{content}",
                parse_mode="Markdown",
            )
        elif msg_type == "video":
            await context.bot.send_video(
                chat_id=GROUP_CHAT_ID,
                video=file_id,
                caption=f"{author_label}\n\n{content}",
                parse_mode="Markdown",
            )
        elif msg_type == "voice":
            await context.bot.send_voice(
                chat_id=GROUP_CHAT_ID,
                voice=file_id,
                caption=author_label,
                parse_mode="Markdown",
            )

        # ── Feature 2: auto-translate Chinese content (if any) ────────────
        await post_translation(context, content, confession_count)

        # DM the user with the approval news
        try:
            await context.bot.send_message(
                chat_id=user_chat_id,
                text=(
                    f"✅ *Your confession #{confession_count} was approved and posted!*\n\n"
                    "Type /start to send another confession."
                ),
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Could not DM approval to user: %s", exc)

        # Log to confessions_log.json (same format as normal confessions)
        append_log({
            "number":    confession_count,
            "timestamp": timestamp,
            "post_type": post_type,
            "user_id":   review["user_id"],
            "full_name": review["full_name"],
            "username":  review["username_str"],
            "content":   content,
            "note":      (
                f"admin-approved after AI flagged as {review['category']} "
                f"({review['confidence']:.0%} confidence)"
            ),
        })

        # Update the review card in admin chat (removes buttons, appends status)
        try:
            await query.edit_message_text(
                original_text
                + f"\n\n✅ *Approved — posted as Confession #{confession_count} ({post_type}).*",
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Could not edit review card after approval: %s", exc)

        logger.info(
            "Admin APPROVED review_id=%s → Confession #%d | %s | user_id=%d",
            review_id, confession_count, post_type, review["user_id"],
        )

    except Exception as exc:
        # Roll back the counter if posting failed
        logger.error("Failed to post admin-approved confession: %s", exc)
        confession_count -= 1
        save_count(confession_count)
        try:
            await query.edit_message_text(
                original_text
                + "\n\n⚠️ *Post failed — is the bot an admin in the channel?*",
                parse_mode="Markdown",
            )
        except Exception:
            pass


# ─── /start ───────────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text(
        "🤫 *Confession Bot*\n\n"
        "Go ahead — type your confession now 👇\n\n"
        "_You can send text, a photo, video, or voice message._",
        parse_mode="Markdown",
    )
    return WAITING_CONFESSION

# ─── Receive confession content ───────────────────────────────────────────────
async def receive_confession(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    msg = update.message
    ud  = context.user_data

    if msg.text:
        ud["type"]    = "text"
        ud["content"] = msg.text
        preview       = msg.text[:120] + ("..." if len(msg.text) > 120 else "")

    elif msg.photo:
        ud["type"]    = "photo"
        ud["file_id"] = msg.photo[-1].file_id
        ud["content"] = msg.caption or ""
        preview       = "📷 Photo" + (f": {msg.caption}" if msg.caption else "")

    elif msg.video:
        ud["type"]    = "video"
        ud["file_id"] = msg.video.file_id
        ud["content"] = msg.caption or ""
        preview       = "🎥 Video" + (f": {msg.caption}" if msg.caption else "")

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
        return ConversationHandler.END

    # ── All clear — show posting options ───────────────────────────────────
    await update.message.reply_text(
        f"📝 *Your confession:*\n_{preview}_\n\n"
        "How do you want to post this?",
        reply_markup=choice_keyboard(),
        parse_mode="Markdown",
    )
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
        await query.edit_message_text("❌ Cancelled.\n\nType /start to send a new confession.")
        context.user_data.clear()
        return ConversationHandler.END

    # ── Build label shown in channel ───────────────────────────────────────
    is_anonymous = (choice == "post_anonymous")

    if is_anonymous:
        author_label = f"🤫 *Anonymous Confession #{confession_count + 1}*"
    else:
        display_name = f"@{user.username}" if user.username else user.full_name
        author_label = f"👤 *Confession #{confession_count + 1} by {display_name}*"

    confession_count += 1
    save_count(confession_count)

    msg_type = ud.get("type")
    content  = ud.get("content", "")
    file_id  = ud.get("file_id", "")

    try:
        # ── Post to channel ────────────────────────────────────────────────
        if msg_type == "text":
            await context.bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=f"{author_label}\n\n{content}",
                parse_mode="Markdown",
            )
        elif msg_type == "photo":
            await context.bot.send_photo(
                chat_id=GROUP_CHAT_ID,
                photo=file_id,
                caption=f"{author_label}\n\n{content}",
                parse_mode="Markdown",
            )
        elif msg_type == "video":
            await context.bot.send_video(
                chat_id=GROUP_CHAT_ID,
                video=file_id,
                caption=f"{author_label}\n\n{content}",
                parse_mode="Markdown",
            )
        elif msg_type == "voice":
            await context.bot.send_voice(
                chat_id=GROUP_CHAT_ID,
                voice=file_id,
                caption=author_label,
                parse_mode="Markdown",
            )

        # ── Feature 2: Auto-translate Chinese content ──────────────────────
        # Runs right after posting. If content has no Chinese, or Groq is
        # not configured, this is a no-op.
        await post_translation(context, content, confession_count)

        # ── Silent admin notification ──────────────────────────────────────
        timestamp    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        username_str = f"@{user.username}" if user.username else "no username"
        post_type    = "Anonymous" if is_anonymous else "Public"

        if ADMIN_CHAT_ID:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"🔔 *New Confession #{confession_count}* ({post_type})\n"
                    f"{'━' * 28}\n"
                    f"👤 Name: {user.full_name}\n"
                    f"🔗 Username: {username_str}\n"
                    f"🆔 User ID: `{user.id}`\n"
                    f"🕐 Time: {timestamp}\n"
                    f"{'━' * 28}\n"
                    f"📝 Content:\n{content}"
                ),
                parse_mode="Markdown",
            )

        # ── Save to log ────────────────────────────────────────────────────
        append_log({
            "number":    confession_count,
            "timestamp": timestamp,
            "post_type": post_type,
            "user_id":   user.id,
            "full_name": user.full_name,
            "username":  username_str,
            "content":   content,
        })

        # ── Confirm to user ────────────────────────────────────────────────
        await query.edit_message_text(
            f"✅ *Confession #{confession_count} posted!*\n\n"
            f"Type /start to send another confession.",
            parse_mode="Markdown",
        )
        logger.info(
            "Confession #%d | %s | user_id=%d | %s",
            confession_count, post_type, user.id, user.full_name,
        )

    except Exception as exc:
        logger.error("Failed to post confession #%d: %s", confession_count, exc)
        confession_count -= 1
        save_count(confession_count)
        await query.edit_message_text(
            "❌ Couldn't post your confession.\n"
            "Make sure the bot is an admin in the channel."
        )

    context.user_data.clear()
    return ConversationHandler.END

# ─── /cancel command ──────────────────────────────────────────────────────────
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("❌ Cancelled.\n\nType /start to send a new confession.")
    context.user_data.clear()
    return ConversationHandler.END

# ─── Message outside conversation ─────────────────────────────────────────────
async def prompt_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Type /start to send a confession.")

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

    entry = next((e for e in load_log() if e.get("number") == target), None)

    if not entry:
        await update.message.reply_text(f"❌ No record for Confession #{target}.")
        return

    await update.message.reply_text(
        f"🔍 *Confession #{target} — Sender Info*\n"
        f"{'━' * 28}\n"
        f"📌 Type: {entry.get('post_type', '?')}\n"
        f"👤 Name: {entry.get('full_name', '?')}\n"
        f"🔗 Username: {entry.get('username', '?')}\n"
        f"🆔 User ID: `{entry.get('user_id', '?')}`\n"
        f"🕐 Sent at: {entry.get('timestamp', '?')}\n"
        f"{'━' * 28}\n"
        f"📝 Content:\n{entry.get('content', '(media)')}",
        parse_mode="Markdown",
    )

# ─── Admin: /filter_stats ─────────────────────────────────────────────────────
async def filter_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show how many confessions have been flagged for review and by which category."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    log   = load_filter_log()
    total = len(log)

    if total == 0:
        await update.message.reply_text(
            "✅ No confessions have been flagged by the AI filter yet.\n"
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

# ─── NEW Admin: /pending — list confessions currently awaiting review ──────────
async def list_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Show all confessions currently sitting in the pending_reviews queue.
    Useful if the admin missed a review notification.
    """
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    if not pending_reviews:
        await update.message.reply_text("✅ No confessions are pending review right now.")
        return

    lines = [f"⏳ *Pending Reviews ({len(pending_reviews)})*", f"{'━' * 28}"]
    for rid, r in pending_reviews.items():
        cat_label = CATEGORY_LABELS.get(r["category"], r["category"])
        lines.append(
            f"🆔 `{rid}` | {r['full_name']} | {cat_label} | {r['timestamp']}"
        )
    lines.append(f"\n{'━' * 28}")
    lines.append("_Re-send /start on each case to re-trigger the review card if needed._")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─── Entry point ──────────────────────────────────────────────────────────────
def main() -> None:
    if not BOT_TOKEN:
        print("❌  BOT_TOKEN is not set.")
        return

    if not FILTER_ENABLED:
        logger.warning(
            "AI filter is DISABLED — set GROQ_API_KEY to enable filtering, "
            "admin review workflow, and Chinese auto-translation."
        )
    else:
        serper_status = (
            "Groq + Serper (URL reputation enabled)"
            if SERPER_API_KEY
            else "Groq only (no URL reputation)"
        )
        logger.info(
            "AI filter ENABLED — %s | Admin review workflow: ON | Auto-translate: ON",
            serper_status,
        )

    # Fix for Python 3.10+ event-loop policy
    asyncio.set_event_loop(asyncio.new_event_loop())

    # Start Flask in background thread so Render sees an HTTP server
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask health server started on port %d", PORT)

    # Build Telegram app
    telegram_app = Application.builder().token(BOT_TOKEN).build()

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
                CallbackQueryHandler(handle_choice, pattern="^(post_anonymous|post_public|cancel)$")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_command)],
        per_user=True,
        per_chat=True,
    )

    telegram_app.add_handler(conv_handler)
    telegram_app.add_handler(CommandHandler("getid",        get_chat_id))
    telegram_app.add_handler(CommandHandler("lookup",       lookup))
    telegram_app.add_handler(CommandHandler("filter_stats", filter_stats))
    telegram_app.add_handler(CommandHandler("pending",      list_pending))   # NEW

    # Catch any message sent outside the /start flow
    telegram_app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & ~filters.COMMAND,
        prompt_start,
    ))

    print("🤖  Confession bot is running.")
    telegram_app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()