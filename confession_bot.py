#!/usr/bin/env python3
"""
Telegram Anonymous Confession Bot
- User must /start every time to send a confession
- After typing, two buttons appear: Post Anonymously / Post Publicly
- Admin always gets a silent DM with full sender identity
"""

import os
import json
import logging
import threading
from datetime import datetime
from flask import Flask
from dotenv import load_dotenv
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
BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "0"))
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
PORT          = int(os.getenv("PORT", 8080))
COUNTER_FILE  = "confession_count.json"
LOG_FILE      = "confessions_log.json"

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

confession_count = load_count()

# ─── Helpers ──────────────────────────────────────────────────────────────────
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

        # ── Notify admin silently ──────────────────────────────────────────
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
        logger.info("Confession #%d | %s | user_id=%d | %s", confession_count, post_type, user.id, user.full_name)

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

    # Build app
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
    telegram_app.add_handler(CommandHandler("getid",  get_chat_id))
    telegram_app.add_handler(CommandHandler("lookup", lookup))

    # Catch any message sent outside of /start flow
    telegram_app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & ~filters.COMMAND,
        prompt_start,
    ))

    print("🤖  Confession bot is running.")
    telegram_app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()