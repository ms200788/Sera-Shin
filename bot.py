# bot.py
# Vault-style Telegram bot implementing upload sessions, deep links,
# persistent auto-delete jobs, DB backups, admin commands, dual channel backup,
# Neon PostgreSQL backup, and aiohttp health check server for Render.

import os
import logging
import asyncio
import sqlite3
import tempfile
import traceback
import json
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiohttp import web
import psycopg2

# --- Logging ---
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("vaultbot")

# --- Environment vars ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UPLOAD_CHANNEL_ID = int(os.getenv("UPLOAD_CHANNEL_ID", "0"))
UPLOAD_CHANNEL_ID2 = int(os.getenv("UPLOAD_CHANNEL_ID2", "0"))
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID", "0"))
DB_CHANNEL_ID2 = int(os.getenv("DB_CHANNEL_ID2", "0"))
DB_PATH = os.getenv("DB_PATH", "/app/data/database.sqlite3")
JOB_DB_PATH = os.getenv("JOB_DB_PATH", "/app/data/jobs.sqlite")
NEON_DSN = os.getenv("NEON_DSN")  # Neon PostgreSQL DSN
PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required!")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# --- Database setup ---
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT,
            caption TEXT,
            created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    logger.info("SQLite DB initialized at %s", DB_PATH)

def init_job_db():
    os.makedirs(os.path.dirname(JOB_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(JOB_DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT,
            delete_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    logger.info("Jobs DB initialized at %s", JOB_DB_PATH)

init_db()
init_job_db()

# --- Health check server (aiohttp) ---
async def healthcheck(request):
    return web.Response(text="OK")

async def start_webserver():
    app = web.Application()
    app.router.add_get("/health", healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health check server running on 0.0.0.0:{PORT}")

# --- Persistence helpers ---
def save_file(file_id, caption):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO files (file_id, caption) VALUES (?, ?)", (file_id, caption))
    conn.commit()
    conn.close()
    logger.info("Saved file %s", file_id)

def schedule_delete(file_id, minutes):
    delete_time = datetime.utcnow() + timedelta(minutes=minutes)
    conn = sqlite3.connect(JOB_DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO jobs (file_id, delete_at) VALUES (?, ?)", (file_id, delete_time))
    conn.commit()
    conn.close()
    logger.info("Scheduled delete for %s at %s", file_id, delete_time)

async def delete_due_files():
    while True:
        now = datetime.utcnow()
        conn = sqlite3.connect(JOB_DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, file_id FROM jobs WHERE delete_at <= ?", (now,))
        jobs = c.fetchall()
        for job_id, file_id in jobs:
            try:
                await bot.delete_message(UPLOAD_CHANNEL_ID, file_id)
            except Exception as e:
                logger.warning("Delete failed for %s: %s", file_id, e)
            c.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        conn.commit()
        conn.close()
        await asyncio.sleep(60)

# --- Neon PostgreSQL backup ---
def backup_to_neon():
    if not NEON_DSN:
        return
    try:
        conn_sqlite = sqlite3.connect(DB_PATH)
        sqlite_data = conn_sqlite.iterdump()
        dump = "\n".join(sqlite_data)
        conn_sqlite.close()

        conn_pg = psycopg2.connect(NEON_DSN)
        cur = conn_pg.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS sqlite_backups (id SERIAL PRIMARY KEY, dump TEXT, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        cur.execute("INSERT INTO sqlite_backups (dump) VALUES (%s)", (dump,))
        conn_pg.commit()
        cur.close()
        conn_pg.close()
        logger.info("Backup pushed to Neon")
    except Exception as e:
        logger.error("Neon backup failed: %s", e)

# --- File download helper (fixed) ---
async def _download_doc_to_tempfile(document):
    file = await bot.get_file(document.file_id)
    file_bytes = await bot.download_file(file.file_path)
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        if hasattr(file_bytes, "read"):
            tmp.write(file_bytes.read())
        else:
            tmp.write(file_bytes)
    finally:
        tmp.close()
    return tmp.name

# --- DB restore from pinned or history (fixed) ---
async def restore_db_from_pinned(ch: int):
    try:
        chat = await bot.get_chat(ch)
        if chat.pinned_message and chat.pinned_message.document:
            tmp_path = await _download_doc_to_tempfile(chat.pinned_message.document)
            if await restore_from_file(tmp_path):
                logger.info("DB restored from pinned message in channel %s", ch)
                return True
            else:
                logger.warning("Pinned backup in channel %s failed integrity; will try history.", ch)

        logger.info("Scanning recent messages in channel %s for backups", ch)
        history = await bot.get_chat_history(ch, limit=200)
        for msg in history:
            if msg.document:
                tmp_path = await _download_doc_to_tempfile(msg.document)
                if await restore_from_file(tmp_path):
                    logger.info("DB restored from history in channel %s", ch)
                    return True

        logger.error("No valid DB backup found in channel %s", ch)
        return False

    except Exception as e:
        logger.error("Failed scanning channel %s history: %s", ch, e)
        return False

# --- Bot Handlers ---
@dp.message_handler(commands=["start", "help"])
async def send_welcome(message: types.Message):
    await message.answer("👋 Welcome to Vault Bot!\nUse /upload to save files.")

@dp.message_handler(commands=["ping"])
async def ping(message: types.Message):
    await message.reply("Pong!")

def is_admin(user_id: int) -> bool:
    return user_id == OWNER_ID
# -------------------------
# Part 2/2 of bot.py
# Remaining handlers, helpers, startup/shutdown
# -------------------------

# --- restore helper (file path -> restore) ---
async def restore_from_file(path: str) -> bool:
    """
    Validate sqlite file and replace current DB if integrity ok.
    Returns True on success.
    """
    try:
        if not os.path.exists(path):
            logger.warning("restore_from_file: path missing %s", path)
            return False
        ok = check_sqlite_integrity(path)
        if not ok:
            logger.warning("restore_from_file: integrity check failed for %s", path)
            return False
        try:
            db.close()
        except Exception:
            pass
        # atomically replace
        os.replace(path, DB_PATH)
        # re-init global db connection
        init_db(DB_PATH)
        await clear_db_dirty()
        logger.info("restore_from_file: DB successfully restored from %s", path)
        return True
    except Exception:
        logger.exception("restore_from_file failed")
        return False

# -------------------------
# Upload commands (owner only)
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip().lower()
    exclude_text = False
    if "exclude_text" in args:
        exclude_text = True
    start_upload_session(OWNER_ID, exclude_text)
    await message.reply("Upload session started. Send media/text you want included. Use /d to finalize, /e to cancel.")

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    cancel_upload_session(OWNER_ID)
    await message.reply("Upload canceled.")

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    upload = active_uploads.get(OWNER_ID)
    if not upload:
        await message.reply("No active upload session.")
        return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Protect ON", callback_data=cb_choose_protect.new(session="pending", choice="1")),
           InlineKeyboardButton("Protect OFF", callback_data=cb_choose_protect.new(session="pending", choice="0")))
    await message.reply("Choose Protect setting:", reply_markup=kb)
    upload["_finalize_requested"] = True

@dp.callback_query_handler(cb_choose_protect.filter())
async def _on_choose_protect(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    try:
        choice = int(callback_data.get("choice", "0"))
        if OWNER_ID not in active_uploads:
            await call.message.answer("Upload session expired.")
            return
        active_uploads[OWNER_ID]["_protect_choice"] = choice
        await call.message.answer("Enter auto-delete timer in minutes (0-10080). 0 = no auto-delete. Reply with a number (e.g., 60).")
    except Exception:
        logger.exception("Error in choose_protect callback")

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and "_finalize_requested" in active_uploads.get(OWNER_ID, {}), content_types=types.ContentTypes.TEXT)
async def _receive_minutes(m: types.Message):
    try:
        txt = m.text.strip()
        try:
            mins = int(txt)
            if mins < 0 or mins > 10080:
                raise ValueError()
        except Exception:
            await m.reply("Please send a valid integer between 0 and 10080.")
            return
        upload = active_uploads.get(OWNER_ID)
        if not upload:
            await m.reply("Upload session missing.")
            return
        messages: List[types.Message] = upload.get("messages", [])
        protect = upload.get("_protect_choice", 0)

        # send header in upload channel
        try:
            header = await bot.send_message(UPLOAD_CHANNEL_ID, "Uploading session...")
        except ChatNotFound:
            await m.reply("Upload channel not found. Please ensure the bot is in the UPLOAD_CHANNEL.")
            logger.error("ChatNotFound uploading to UPLOAD_CHANNEL_ID")
            return
        header_msg_id = header.message_id
        header_chat_id = header.chat.id

        # generate token and ensure unique
        token = generate_token(8)
        attempt = 0
        while sql_get_session_by_token(token) is not None and attempt < 5:
            token = generate_token(8)
            attempt += 1

        session_temp_id = sql_insert_session(OWNER_ID, protect, mins, "Untitled", header_chat_id, header_msg_id, token)

        me = await bot.get_me()
        bot_username = me.username or db_get("bot_username") or ""
        deep_link = f"https://t.me/{bot_username}?start={token}"

        try:
            await bot.edit_message_text(f"Session {session_temp_id}\n{deep_link}", UPLOAD_CHANNEL_ID, header_msg_id)
        except Exception:
            pass

        for m0 in messages:
            try:
                if m0.text and m0.text.strip().startswith("/"):
                    continue

                if m0.text and (not upload.get("exclude_text")) and not (m0.photo or m0.video or m0.document or m0.sticker or m0.animation):
                    sent = await bot.send_message(UPLOAD_CHANNEL_ID, m0.text)
                    sql_add_file(session_temp_id, "text", "", m0.text or "", m0.message_id, sent.message_id)
                elif m0.photo:
                    file_id = m0.photo[-1].file_id
                    sent = await bot.send_photo(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "photo", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif m0.video:
                    file_id = m0.video.file_id
                    sent = await bot.send_video(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "video", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif m0.document:
                    file_id = m0.document.file_id
                    sent = await bot.send_document(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "document", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif m0.sticker:
                    file_id = m0.sticker.file_id
                    sent = await bot.send_sticker(UPLOAD_CHANNEL_ID, file_id)
                    sql_add_file(session_temp_id, "sticker", file_id, "", m0.message_id, sent.message_id)
                elif m0.animation:
                    file_id = m0.animation.file_id
                    sent = await bot.send_animation(UPLOAD_CHANNEL_ID, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "animation", file_id, m0.caption or "", m0.message_id, sent.message_id)
                else:
                    try:
                        sent = await bot.copy_message(UPLOAD_CHANNEL_ID, m0.chat.id, m0.message_id)
                        caption = getattr(m0, "caption", None) or getattr(m0, "text", "") or ""
                        sql_add_file(session_temp_id, "other", "", caption or "", m0.message_id, sent.message_id)
                    except Exception:
                        logger.exception("Failed copying message during finalize")
            except Exception:
                logger.exception("Error copying message during finalize")

        # update session row and mark dirty
        cur = db.cursor()
        cur.execute("UPDATE sessions SET deep_link=?, header_msg_id=?, header_chat_id=? WHERE id=?", (token, header_msg_id, header_chat_id, session_temp_id))
        db.commit()
        mark_db_dirty()

        # mirror header to upload channel 2 if set
        if UPLOAD_CHANNEL_ID2 and UPLOAD_CHANNEL_ID2 != 0:
            try:
                await bot.copy_message(UPLOAD_CHANNEL_ID2, UPLOAD_CHANNEL_ID, header_msg_id)
            except Exception:
                logger.exception("Failed mirroring header to UPLOAD_CHANNEL_ID2")

        # Kick off backup to channels + neon
        await backup_db_to_channel()

        cancel_upload_session(OWNER_ID)
        await m.reply(f"Session finalized: {deep_link}")
        try:
            active_uploads.pop(OWNER_ID, None)
        except Exception:
            pass
        raise CancelHandler()
    except CancelHandler:
        raise
    except Exception:
        logger.exception("Error finalizing upload")
        await m.reply("An error occurred during finalization.")

# -------------------------
# setmessage / setimage flows (already present in Part 1)
# (Assume Part 1 included the combined pending resolution handler)
# -------------------------

# -------------------------
# Admin & utility commands (rest)
# -------------------------
@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    rows = sql_list_sessions(200)
    if not rows:
        await message.reply("No sessions.")
        return
    out = []
    for r in rows:
        out.append(f"ID:{r['id']} created:{r['created_at']} protect:{r['protect']} auto_min:{r['auto_delete_minutes']} revoked:{r['revoked']} token:{r['deep_link']}")
    msg = "\n".join(out)
    if len(msg) > 4000:
        await message.reply("Too many sessions to display.")
    else:
        await message.reply(msg)

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /revoke <id>")
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id")
        return
    sql_set_session_revoked(sid, 1)
    await message.reply(f"Session {sid} revoked.")

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast.")
        return
    cur = db.cursor()
    cur.execute("SELECT id FROM users")
    users = [r["id"] for r in cur.fetchall()]
    if not users:
        await message.reply("No users to broadcast to.")
        return
    await message.reply(f"Starting broadcast to {len(users)} users.")
    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    lock = asyncio.Lock()
    stats = {"success": 0, "failed": 0, "removed": []}

    async def worker(uid):
        nonlocal stats
        async with sem:
            try:
                await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                async with lock:
                    stats["success"] += 1
            except BotBlocked:
                sql_remove_user(uid)
                async with lock:
                    stats["removed"].append(uid)
            except ChatNotFound:
                sql_remove_user(uid)
                async with lock:
                    stats["removed"].append(uid)
            except BadRequest:
                async with lock:
                    stats["failed"] += 1
            except RetryAfter as e:
                logger.warning("Broadcast RetryAfter %s seconds", e.timeout)
                await asyncio.sleep(e.timeout + 1)
                try:
                    await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                    async with lock:
                        stats["success"] += 1
                except Exception:
                    async with lock:
                        stats["failed"] += 1
            except Exception:
                async with lock:
                    stats["failed"] += 1

    tasks = [worker(u) for u in users]
    await asyncio.gather(*tasks)
    removed_count = len(stats["removed"])
    await message.reply(f"Broadcast complete. Success: {stats['success']} Failed: {stats['failed']} Removed: {removed_count}")
    if removed_count:
        r_sample = stats["removed"][:10]
        await bot.send_message(OWNER_ID, f"Broadcast removed {removed_count} users (e.g. {r_sample}). These users were removed from DB.")

# -------------------------
# Backup & restore commands
# -------------------------
@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    await backup_db_to_channel()
    await message.reply("Backup attempted (channels + Neon if configured).")

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    ok = False
    # try pinned then history for both channels
    for ch in (DB_CHANNEL_ID, DB_CHANNEL_ID2):
        if not ch or ch == 0:
            continue
        ok = await restore_db_from_pinned(ch)
        if ok:
            break
    if ok:
        await message.reply("DB restored.")
    else:
        await message.reply("Restore failed. Check logs.")

# -------------------------
# Catch-all uploader & lastseen handler
# -------------------------
@dp.message_handler(lambda m: True, content_types=types.ContentTypes.ANY)
async def catch_all(m: types.Message):
    try:
        # ignore bot messages
        if not m.from_user:
            return
        if m.from_user.id != OWNER_ID:
            # update last seen
            sql_update_user_lastseen(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "", m.from_user.last_name or "")
            return
        # owner path: store in upload session if active
        if OWNER_ID in active_uploads:
            if m.text and m.text.strip().startswith("/"):
                return
            if m.text and active_uploads[OWNER_ID].get("exclude_text"):
                pass
            else:
                append_upload_message(OWNER_ID, m)
                try:
                    await m.reply("Stored in upload session.")
                except Exception:
                    pass
    except Exception:
        logger.exception("Error in catch_all")

# -------------------------
# Debounced & periodic backup jobs
# -------------------------
async def debounced_backup_job():
    try:
        if DB_DIRTY:
            logger.info("Debounced backup triggered (DB dirty). Starting backup...")
            await backup_db_to_channel()
        else:
            logger.debug("Debounced backup: DB not dirty; skipping upload.")
    except Exception:
        logger.exception("Debounced backup job failed")

async def periodic_safety_backup_job():
    try:
        logger.info("Periodic safety backup triggered.")
        await backup_db_to_channel()
    except Exception:
        logger.exception("Periodic safety backup failed")

# -------------------------
# Startup & shutdown
# -------------------------
async def on_startup(dispatcher):
    logger.info("on_startup: initializing services")
    # init neon pool (if asyncpg available / configured call earlier init_neon_pool)
    try:
        await init_neon_pool()
    except Exception:
        logger.exception("init_neon_pool failed on startup")

    # attempt restore if local db missing or corrupted
    try:
        # try both channels, pinned first then history
        restored = False
        for ch in (DB_CHANNEL_ID, DB_CHANNEL_ID2):
            if not ch or ch == 0:
                continue
            try:
                ok = await restore_db_from_pinned(ch)
                if ok:
                    restored = True
                    break
            except Exception:
                logger.exception("restore attempt failed for channel %s", ch)
        if not restored:
            logger.info("No restore performed (local DB kept)")
    except Exception:
        logger.exception("restore_db_from_pinned error on startup")

    # start scheduler
    try:
        scheduler.start()
    except Exception:
        logger.exception("Scheduler start error")

    # restore delete jobs
    try:
        await restore_pending_jobs_and_schedule()
    except Exception:
        logger.exception("restore_pending_jobs_and_schedule error")

    # schedule backup jobs
    try:
        try:
            scheduler.add_job(debounced_backup_job, 'interval', minutes=DEBOUNCE_BACKUP_MINUTES, id="debounced_backup")
        except Exception:
            pass
        try:
            scheduler.add_job(periodic_safety_backup_job, 'interval', hours=AUTO_BACKUP_HOURS, id="periodic_safety_backup")
        except Exception:
            pass
    except Exception:
        logger.exception("Failed scheduling backup jobs")

    # start health endpoint
    try:
        asyncio.create_task(start_webserver())
    except Exception:
        logger.exception("Failed to start health app task")

    # basic checks for presence in required channels
    try:
        await bot.get_chat(UPLOAD_CHANNEL_ID)
    except Exception:
        logger.error("Upload channel may be inaccessible; ensure bot is a member.")
    if UPLOAD_CHANNEL_ID2 and UPLOAD_CHANNEL_ID2 != 0:
        try:
            await bot.get_chat(UPLOAD_CHANNEL_ID2)
        except Exception:
            logger.error("Upload channel 2 may be inaccessible.")
    try:
        await bot.get_chat(DB_CHANNEL_ID)
    except Exception:
        logger.error("DB channel may be inaccessible; ensure bot is a member.")
    if DB_CHANNEL_ID2 and DB_CHANNEL_ID2 != 0:
        try:
            await bot.get_chat(DB_CHANNEL_ID2)
        except Exception:
            logger.error("DB channel 2 may be inaccessible.")

    # store bot username
    try:
        me = await bot.get_me()
        db_set("bot_username", me.username or "")
    except Exception:
        logger.exception("Failed fetching bot username")

    # default texts
    if db_get("start_text") is None:
        db_set("start_text", "Welcome, {first_name}!")
    if db_get("help_text") is None:
        db_set("help_text", "This bot delivers sessions.")

    # set commands: only start/help globally; owner gets admin cmds via BotFather or scoped commands
    try:
        default_commands = [
            BotCommand("start", "Start / open deep link"),
            BotCommand("help", "Show help"),
        ]
        await bot.set_my_commands(default_commands)
    except Exception:
        logger.exception("Couldn't set bot commands")

    logger.info("on_startup complete")

async def on_shutdown(dispatcher):
    logger.info("Shutting down")
    try:
        if DB_DIRTY:
            logger.info("Final backup on shutdown (DB dirty).")
            await backup_db_to_channel()
    except Exception:
        logger.exception("Final backup failed")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await close_neon_pool()
    except Exception:
        logger.exception("Failed closing Neon pool")
    try:
        await stop_web_server()
    except Exception:
        logger.exception("Failed stopping web server")
    await bot.close()

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    try:
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopped by user")
    except Exception:
        logger.exception("Fatal error")