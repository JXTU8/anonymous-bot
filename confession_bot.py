#!/usr/bin/env python3
"""
Telegram Anonymous Confession Bot
- User must /start every time to send a confession
- After typing, two buttons appear: Post Anonymously / Post Publicly
- Admin always gets a silent DM with full sender identity
- AI filter (Groq llama-3.3-70b + Serper) blocks ads & phishing links
"""

import asyncio
import os
import re
import json
import logging
import threading
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

# ─── Groq client (optional — filter is silently disabled without it) ──────────
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

# ─── AI Filter ────────────────────────────────────────────────────────────────
CATEGORY_LABELS = {
    "ad":       "📢 Advertisement / Promo Spam",
    "phishing": "🎣 Phishing / Scam Link",
    "spam":     "🚫 Spam",
}

FILTER_CONFIDENCE_THRESHOLD = 0.75  # Only block when AI is ≥75% sure


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

    # Build URL context block from Serper results
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

        # Strip accidental markdown code fences
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
            raw = re.sub(r"\n?```\s*$",           "", raw)
        raw = raw.strip()

        result = json.loads(raw)
        # Normalise 'flagged' in case model returns string "true"/"false"
        result["flagged"] = str(result.get("flagged", False)).lower() == "true"
        return result

    except json.JSONDecodeError:
        logger.warning("Groq returned non-JSON (letting through): %.300s", raw)
        return {"flagged": False, "category": "clean", "reason": "parse error", "confidence": 0.0}
    except Exception as exc:
        logger.error("Groq filter error (letting through): %s", exc)
        return {"flagged": False, "category": "clean", "reason": str(exc), "confidence": 0.0}


async def apply_filter(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    content: str,
) -> bool:
    """
    Full filter pipeline: Serper URL lookup → Groq AI analysis → block or pass.

    Returns True  → message is blocked (caller should end the conversation).
    Returns False → message is clean / filter is off (caller should proceed).
    """
    if not FILTER_ENABLED or not content or content == "(voice message)":
        return False  # Nothing to check

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

    # ── Blocked ────────────────────────────────────────────────────────────
    category       = filter_result.get("category", "spam")
    reason         = filter_result.get("reason", "Policy violation detected.")
    category_label = CATEGORY_LABELS.get(category, "⚠️ Policy Violation")
    user           = update.effective_user
    username_str   = f"@{user.username}" if user.username else "no username"
    timestamp      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info(
        "BLOCKED | %-10s | %.0f%% confidence | user_id=%-12d | %s",
        category, confidence * 100, user.id, user.full_name,
    )

    # Silent admin alert
    if ADMIN_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"🚫 *Blocked Confession Attempt*\n"
                    f"{'━' * 28}\n"
                    f"👤 Name:        {user.full_name}\n"
                    f"🔗 Username:    {username_str}\n"
                    f"🆔 User ID:     `{user.id}`\n"
                    f"🕐 Time:        {timestamp}\n"
                    f"⚠️ Category:    {category_label}\n"
                    f"🤖 AI Reason:   {reason}\n"
                    f"📊 Confidence:  {confidence:.0%}\n"
                    f"{'━' * 28}\n"
                    f"📝 Content:\n{content}"
                ),
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Admin block-notification failed: %s", exc)

    # Persist to filter log
    append_filter_log({
        "timestamp":  timestamp,
        "user_id":    user.id,
        "full_name":  user.full_name,
        "username":   username_str,
        "category":   category,
        "reason":     reason,
        "confidence": confidence,
        "content":    content,
    })

    await update.message.reply_text(
        f"🚫 *Confession Blocked*\n\n"
        f"Your message was flagged as: *{category_label}*\n"
        f"_{reason}_\n\n"
        f"If you believe this is a mistake, type /start and rephrase your message.",
        parse_mode="Markdown",
    )
    context.user_data.clear()
    return True  # Blocked


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

    # ── AI Filter (skips voice-only since there is no analyzable text) ─────
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
    """Show how many confessions have been blocked and by which category."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Admins only.")
        return

    log   = load_filter_log()
    total = len(log)

    if total == 0:
        await update.message.reply_text(
            "✅ No confessions have been blocked by the AI filter yet.\n"
            f"Filter status: {'🟢 Active' if FILTER_ENABLED else '🔴 Disabled (set GROQ_API_KEY)'}"
        )
        return

    cats  = Counter(e.get("category", "unknown") for e in log)
    lines = [
        f"🚫 *AI Filter Statistics*",
        f"{'━' * 28}",
        f"Status: {'🟢 Active' if FILTER_ENABLED else '🔴 Disabled'}",
        f"Total blocked: *{total}*\n",
    ]

    for cat, count in cats.most_common():
        label = CATEGORY_LABELS.get(cat, cat)
        lines.append(f"{label}: *{count}*")

    recent = log[-1]
    lines += [
        f"\n{'━' * 28}",
        f"Last blocked: {recent['timestamp']}",
        f"Category: {CATEGORY_LABELS.get(recent.get('category', ''), '?')}",
        f"Confidence: {recent.get('confidence', 0):.0%}",
    ]

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

# ─── Entry point ──────────────────────────────────────────────────────────────
def main() -> None:
    if not BOT_TOKEN:
        print("❌  BOT_TOKEN is not set.")
        return

    if not FILTER_ENABLED:
        logger.warning(
            "AI filter is DISABLED — add GROQ_API_KEY to your environment to enable it."
        )
    else:
        serper_status = "Groq + Serper (URL reputation enabled)" if SERPER_API_KEY else "Groq only (no URL reputation)"
        logger.info("AI filter is ENABLED — %s", serper_status)

    # Fix for Python 3.10+ event-loop policy
    asyncio.set_event_loop(asyncio.new_event_loop())

    # Start Flask in background thread so Render sees an HTTP server
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask health server started on port %d", PORT)

    # Build Telegram app
    telegram_app = Application.builder().token(BOT_TOKEN).build()

    # Confession conversation flow
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
    telegram_app.add_handler(CommandHandler("filter_stats", filter_stats))  # NEW

    # Catch any message sent outside the /start flow
    telegram_app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & ~filters.COMMAND,
        prompt_start,
    ))

    print("🤖  Confession bot is running.")
    telegram_app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
