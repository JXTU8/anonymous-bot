#!/usr/bin/env python3
"""
Telegram Anonymous Confession Bot
- Runs a Flask health server so Render free tier stays alive
- UptimeRobot pings /health every 5 min to prevent spin-down
- Group sees anonymous confessions
- Admin gets silent DM with full sender identity
"""

import os
import json
import logging
import threading
from datetime import datetime
from flask import Flask
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────
BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "0"))
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
PORT          = int(os.getenv("PORT", 8080))       # Render injects PORT automatically
COUNTER_FILE  = "confession_count.json"
LOG_FILE      = "confessions_log.json"

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

confession_count = load_count()

# ─── Helpers ──────────────────────────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return ADMIN_CHAT_ID and user_id == ADMIN_CHAT_ID

# ─── Command handlers ─────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤫 *Anonymous Confession Bot*\n\n"
        "Send me your confession here and it will be posted to the channel "
        "completely anonymously.\n\n"
        "Just type away 👇",
        parse_mode="Markdown",
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📖 *How to use*\n\n"
        "1️⃣ DM me any text, photo, video, or voice message\n"
        "2️⃣ I'll post it to the channel as an anonymous confession\n"
        "3️⃣ Your identity stays completely hidden 😌\n\n"
        "*Admin:* /lookup <number>",
        parse_mode="Markdown",
    )

async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    await update.message.reply_text(
        f"Chat ID: `{chat.id}`",
        parse_mode="Markdown",
    )

async def lookup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin only: /lookup <confession_number>"""
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
        await update.message.reply_text(f"❌ No record found for Confession #{target}.")
        return

    await update.message.reply_text(
        f"🔍 *Confession #{target} — Sender Info*\n"
        f"{'━' * 28}\n"
        f"👤 Name: {entry.get('full_name', '?')}\n"
        f"🔗 Username: {entry.get('username', 'no username')}\n"
        f"🆔 User ID: `{entry.get('user_id', '?')}`\n"
        f"🕐 Sent at: {entry.get('timestamp', '?')}\n"
        f"{'━' * 28}\n"
        f"📝 Content:\n{entry.get('content', '(media)')}",
        parse_mode="Markdown",
    )

# ─── Core confession handler ──────────────────────────────────────────────────
async def handle_confession(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global confession_count

    if update.message.chat.type != "private":
        return

    if not GROUP_CHAT_ID:
        await update.message.reply_text("⚠️ Bot is not fully configured yet.")
        return

    confession_count += 1
    save_count(confession_count)

    msg          = update.message
    user         = update.effective_user
    prefix       = f"🤫 *Anonymous Confession #{confession_count}*\n\n"
    content_text = msg.text or msg.caption or "(media)"

    try:
        # ── Post anonymously to channel/group ─────────────────────────────
        if msg.text:
            await context.bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=prefix + msg.text,
                parse_mode="Markdown",
            )
        elif msg.photo:
            await context.bot.send_photo(
                chat_id=GROUP_CHAT_ID,
                photo=msg.photo[-1].file_id,
                caption=prefix + (msg.caption or ""),
                parse_mode="Markdown",
            )
        elif msg.video:
            await context.bot.send_video(
                chat_id=GROUP_CHAT_ID,
                video=msg.video.file_id,
                caption=prefix + (msg.caption or ""),
                parse_mode="Markdown",
            )
        elif msg.voice:
            await context.bot.send_voice(
                chat_id=GROUP_CHAT_ID,
                voice=msg.voice.file_id,
                caption=prefix.strip(),
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text("⚠️ I can forward text, photos, videos, and voice messages.")
            confession_count -= 1
            save_count(confession_count)
            return

        # ── Notify admin silently with full sender identity ────────────────
        timestamp    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        username_str = f"@{user.username}" if user.username else "no username"

        if ADMIN_CHAT_ID:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"🔔 *New Confession #{confession_count}*\n"
                    f"{'━' * 28}\n"
                    f"👤 Name: {user.full_name}\n"
                    f"🔗 Username: {username_str}\n"
                    f"🆔 User ID: `{user.id}`\n"
                    f"🕐 Time: {timestamp}\n"
                    f"{'━' * 28}\n"
                    f"📝 Content:\n{content_text}"
                ),
                parse_mode="Markdown",
            )

        # ── Save to log file ───────────────────────────────────────────────
        append_log({
            "number":    confession_count,
            "timestamp": timestamp,
            "user_id":   user.id,
            "full_name": user.full_name,
            "username":  username_str,
            "content":   content_text,
        })

        # ── Confirm to sender ──────────────────────────────────────────────
        await update.message.reply_text(
            f"✅ Posted as *Confession #{confession_count}*!\n"
            "Your identity is completely hidden 😌",
            parse_mode="Markdown",
        )
        logger.info("Confession #%d | user_id=%d | %s", confession_count, user.id, user.full_name)

    except Exception as exc:
        logger.error("Failed to post confession #%d: %s", confession_count, exc)
        confession_count -= 1
        save_count(confession_count)
        await update.message.reply_text(
            "❌ Couldn't post your confession. Make sure the bot is admin in the channel."
        )

# ─── Entry point ──────────────────────────────────────────────────────────────
def main() -> None:
    if not BOT_TOKEN:
        print("❌  BOT_TOKEN is not set.")
        return

    # Fix for Python 3.10+ — explicitly create an event loop before anything async
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())

    # Start Flask in a background thread so Render sees an HTTP server
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask health server started on port %d", PORT)

    # Run Telegram bot (blocking — keeps the process alive)
    telegram_app = Application.builder().token(BOT_TOKEN).build()
    telegram_app.add_handler(CommandHandler("start",  start))
    telegram_app.add_handler(CommandHandler("help",   help_command))
    telegram_app.add_handler(CommandHandler("getid",  get_chat_id))
    telegram_app.add_handler(CommandHandler("lookup", lookup))
    telegram_app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_confession,
    ))

    print("🤖  Confession bot is running.")
    telegram_app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()