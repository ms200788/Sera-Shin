#!/usr/bin/env python3
# bot.py
# Vault-style Telegram bot with robust persistence:
# - SQLite primary DB (local)
# - Rolling backups to DB_CHANNEL_ID and DB_CHANNEL_ID2 (Telegram channels)
# - Neon mirror (Postgres) if NEON_URL set (asyncpg)
# - APScheduler persistent jobstore (JOB_DB_PATH)
# - Debounced backups + periodic safety backups
# - Upload sessions, deep links, auto-delete scheduling
# - Admin-scoped commands (owner sees admin panel)
# - aiohttp healthcheck server (for web deployments)

import os
import logging
import asyncio
import json
import sqlite3
import tempfile
import secrets
import string
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, BotCommand
from aiogram.dispatcher.handler import CancelHandler
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.callback_data import CallbackData

from aiogram.utils.exceptions import (
    BotBlocked,
    ChatNotFound,
    RetryAfter,
    BadRequest,
    MessageToDeleteNotFound,
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

import aiohttp
from aiohttp import web

# optional asyncpg for Neon backup
try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except Exception:
    asyncpg = None
    ASYNCPG_AVAILABLE = False

# -------------------------
# Environment configuration
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID") or 0)
UPLOAD_CHANNEL_ID = int(os.environ.get("UPLOAD_CHANNEL_ID") or 0)
UPLOAD_CHANNEL_ID2 = int(os.environ.get("UPLOAD_CHANNEL_ID2") or 0)  # optional mirror upload channel
DB_CHANNEL_ID = int(os.environ.get("DB_CHANNEL_ID") or 0)
DB_CHANNEL_ID2 = int(os.environ.get("DB_CHANNEL_ID2") or 0)  # optional second DB backup channel
DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "8080"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))
AUTO_BACKUP_HOURS = int(os.environ.get("AUTO_BACKUP_HOURS", "6"))  # periodic safety backup
DEBOUNCE_BACKUP_MINUTES = int(os.environ.get("DEBOUNCE_BACKUP_MINUTES", "5"))  # debounced backup frequency
MAX_BACKUPS = int(os.environ.get("MAX_BACKUPS", "10"))  # rolling history per channel

# Neon (Postgres) mirror settings
NEON_URL = os.environ.get("NEON_URL")  # example: "postgresql://user:pass@host:5432/dbname"
NEON_MAX_BACKUPS = int(os.environ.get("NEON_MAX_BACKUPS", str(MAX_BACKUPS)))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if OWNER_ID == 0:
    raise RuntimeError("OWNER_ID is required")
if UPLOAD_CHANNEL_ID == 0:
    raise RuntimeError("UPLOAD_CHANNEL_ID is required")
if DB_CHANNEL_ID == 0:
    raise RuntimeError("DB_CHANNEL_ID is required")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("vaultbot")

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN, parse_mode=None)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# -------------------------
# Ensure job DB folder exists (for SQLAlchemy jobstore)
# -------------------------
def _ensure_dir_for_path(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

_ensure_dir_for_path(JOB_DB_PATH)
_ensure_dir_for_path(DB_PATH)

# -------------------------
# Scheduler with persistent jobstore
# -------------------------
jobstores = {
    'default': SQLAlchemyJobStore(url=f"sqlite:///{JOB_DB_PATH}")
}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.configure(timezone="UTC")

# -------------------------
# Callback data factories
# -------------------------
cb_choose_protect = CallbackData("protect", "session", "choice")
cb_retry = CallbackData("retry", "session")
cb_help_button = CallbackData("helpbtn", "action")

# -------------------------
# DB schema
# -------------------------
SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    last_seen TEXT
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER,
    created_at TEXT,
    protect INTEGER DEFAULT 0,
    auto_delete_minutes INTEGER DEFAULT 0,
    title TEXT,
    revoked INTEGER DEFAULT 0,
    header_msg_id INTEGER,
    header_chat_id INTEGER,
    deep_link TEXT
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    file_type TEXT,
    file_id TEXT,
    caption TEXT,
    original_msg_id INTEGER,
    vault_msg_id INTEGER,
    FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS delete_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    target_chat_id INTEGER,
    message_ids TEXT,
    run_at TEXT,
    created_at TEXT,
    status TEXT DEFAULT 'scheduled'
);
"""

# -------------------------
# Database initialization
# -------------------------
db: sqlite3.Connection  # global

def init_db(path: str = DB_PATH):
    global db
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    need_init = not os.path.exists(path)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    db = conn
    if need_init:
        conn.executescript(SCHEMA)
        conn.commit()
    return conn

db = init_db(DB_PATH)

# -------------------------
# DB dirty / debounce backup state
# -------------------------
DB_DIRTY = False
DB_DIRTY_SINCE: Optional[datetime] = None
DB_DIRTY_LOCK = asyncio.Lock()

def mark_db_dirty():
    global DB_DIRTY, DB_DIRTY_SINCE
    DB_DIRTY = True
    DB_DIRTY_SINCE = datetime.utcnow()
    logger.debug("DB marked dirty at %s", DB_DIRTY_SINCE.isoformat())

async def clear_db_dirty():
    global DB_DIRTY, DB_DIRTY_SINCE
    async with DB_DIRTY_LOCK:
        DB_DIRTY = False
        DB_DIRTY_SINCE = None
        logger.debug("DB dirty flag cleared")

# -------------------------
# Neon (asyncpg) pool
# -------------------------
neon_pool: Optional["asyncpg.pool.Pool"] = None

async def init_neon_pool():
    global neon_pool
    if not NEON_URL:
        logger.info("NEON_URL not set; skipping Neon initialization")
        return
    if not ASYNCPG_AVAILABLE:
        logger.warning("asyncpg not installed; Neon backup disabled (install asyncpg to enable)")
        return
    try:
        neon_pool = await asyncpg.create_pool(dsn=NEON_URL, min_size=1, max_size=3)
        async with neon_pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sqlite_backups (
                    id BIGSERIAL PRIMARY KEY,
                    filename TEXT,
                    data bytea,
                    created_at timestamptz DEFAULT now()
                )
                """
            )
        logger.info("Neon pool initialized and table ensured")
    except Exception:
        logger.exception("Failed to initialize Neon pool")
        neon_pool = None

async def close_neon_pool():
    global neon_pool
    try:
        if neon_pool:
            await neon_pool.close()
            neon_pool = None
    except Exception:
        logger.exception("Failed to close Neon pool")

async def neon_store_backup(file_path: str):
    """
    Store sqlite file bytes into Neon table as extra backup.
    Keep only last NEON_MAX_BACKUPS rows.
    """
    global neon_pool
    if not neon_pool:
        return False
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        fname = os.path.basename(file_path)
        async with neon_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sqlite_backups (filename, data) VALUES ($1, $2)",
                fname, data
            )
            # trim older
            await conn.execute(
                """
                DELETE FROM sqlite_backups
                WHERE id NOT IN (
                    SELECT id FROM sqlite_backups ORDER BY created_at DESC LIMIT $1
                )
                """,
                NEON_MAX_BACKUPS
            )
        logger.info("Stored sqlite backup to Neon (kept last %s)", NEON_MAX_BACKUPS)
        return True
    except Exception:
        logger.exception("Failed to store backup to Neon")
        return False

# -------------------------
# Database helpers (marked dirty)
# -------------------------
def db_set(key: str, value: str):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
    db.commit()
    mark_db_dirty()

def db_get(key: str, default=None):
    cur = db.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    r = cur.fetchone()
    return r["value"] if r else default

def sql_insert_session(owner_id:int, protect:int, auto_delete_minutes:int, title:str, header_chat_id:int, header_msg_id:int, deep_link_token:str)->int:
    cur = db.cursor()
    cur.execute(
        "INSERT INTO sessions (owner_id,created_at,protect,auto_delete_minutes,title,header_chat_id,header_msg_id,deep_link) VALUES (?,?,?,?,?,?,?,?)",
        (owner_id, datetime.utcnow().isoformat(), protect, auto_delete_minutes, title, header_chat_id, header_msg_id, deep_link_token)
    )
    db.commit()
    session_id = cur.lastrowid
    mark_db_dirty()
    return session_id

def sql_add_file(session_id:int, file_type:str, file_id:str, caption:str, original_msg_id:int, vault_msg_id:int):
    cur = db.cursor()
    cur.execute(
        "INSERT INTO files (session_id,file_type,file_id,caption,original_msg_id,vault_msg_id) VALUES (?,?,?,?,?,?)",
        (session_id, file_type, file_id, caption, original_msg_id, vault_msg_id)
    )
    db.commit()
    fid = cur.lastrowid
    mark_db_dirty()
    return fid

def sql_list_sessions(limit=50):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def sql_get_session_by_id(session_id:int):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions WHERE id=?", (session_id,))
    r = cur.fetchone()
    return dict(r) if r else None

def sql_get_session_by_token(token: str):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions WHERE deep_link=?", (token,))
    r = cur.fetchone()
    return dict(r) if r else None

def sql_get_session_files(session_id:int):
    cur = db.cursor()
    cur.execute("SELECT * FROM files WHERE session_id=? ORDER BY id", (session_id,))
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def sql_set_session_revoked(session_id:int, revoked:int=1):
    cur = db.cursor()
    cur.execute("UPDATE sessions SET revoked=? WHERE id=?", (revoked, session_id))
    db.commit()
    mark_db_dirty()

def sql_add_user(user: types.User):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO users (id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)",
                (user.id, user.username or "", user.first_name or "", user.last_name or "", datetime.utcnow().isoformat()))
    db.commit()
    mark_db_dirty()

def sql_update_user_lastseen(user_id:int, username:str="", first_name:str="", last_name:str=""):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO users (id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)",
                (user_id, username or "", first_name or "", last_name or "", datetime.utcnow().isoformat()))
    db.commit()
    mark_db_dirty()

def sql_remove_user(user_id:int):
    cur = db.cursor()
    cur.execute("DELETE FROM users WHERE id=?", (user_id,))
    db.commit()
    mark_db_dirty()

def sql_stats():
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM users")
    total_users = cur.fetchone()["cnt"]
    cur.execute("SELECT COUNT(*) as active FROM users WHERE last_seen >= ?", ((datetime.utcnow()-timedelta(days=2)).isoformat(),))
    row = cur.fetchone()
    active = row["active"] if row else 0
    cur.execute("SELECT COUNT(*) as files FROM files")
    files = cur.fetchone()["files"]
    cur.execute("SELECT COUNT(*) as sessions FROM sessions")
    sessions = cur.fetchone()["sessions"]
    return {"total_users": total_users, "active_2d": active, "files": files, "sessions": sessions}

def sql_add_delete_job(session_id:int, target_chat_id:int, message_ids:List[int], run_at:datetime):
    cur = db.cursor()
    cur.execute("INSERT INTO delete_jobs (session_id,target_chat_id,message_ids,run_at,created_at) VALUES (?,?,?,?,?)",
                (session_id, target_chat_id, json.dumps(message_ids), run_at.isoformat(), datetime.utcnow().isoformat()))
    db.commit()
    jid = cur.lastrowid
    mark_db_dirty()
    return jid

def sql_list_pending_jobs():
    cur = db.cursor()
    cur.execute("SELECT * FROM delete_jobs WHERE status='scheduled'")
    return [dict(r) for r in cur.fetchall()]

def sql_mark_job_done(job_id:int):
    cur = db.cursor()
    cur.execute("UPDATE delete_jobs SET status='done' WHERE id=?", (job_id,))
    db.commit()
    mark_db_dirty()

# -------------------------
# In-memory upload sessions
# -------------------------
active_uploads: Dict[int, Dict[str, Any]] = {}

def start_upload_session(owner_id:int, exclude_text:bool):
    active_uploads[owner_id] = {
        "messages": [], "exclude_text": exclude_text, "started_at": datetime.utcnow()
    }

def cancel_upload_session(owner_id:int):
    active_uploads.pop(owner_id, None)

def append_upload_message(owner_id:int, msg: types.Message):
    if owner_id not in active_uploads:
        return
    active_uploads[owner_id]["messages"].append(msg)

def get_upload_messages(owner_id:int) -> List[types.Message]:
    return active_uploads.get(owner_id, {}).get("messages", [])

# -------------------------
# Pending stores for setmessage/setimage two-step flows
# -------------------------
pending_setmessage: Dict[int, Dict[str, Any]] = {}
pending_setimage: Dict[int, Dict[str, Any]] = {}

# -------------------------
# Utilities
# -------------------------
async def safe_send(chat_id, text=None, **kwargs):
    try:
        if text is None:
            return None
        return await bot.send_message(chat_id, text, **kwargs)
    except BotBlocked:
        logger.warning("Bot blocked by %s", chat_id)
    except ChatNotFound:
        logger.warning("Chat not found: %s", chat_id)
    except RetryAfter as e:
        logger.warning("Flood wait %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_send(chat_id, text, **kwargs)
    except Exception:
        logger.exception("Failed to send message")
    return None

async def safe_copy(to_chat_id:int, from_chat_id:int, message_id:int, **kwargs):
    try:
        return await bot.copy_message(to_chat_id, from_chat_id, message_id, **kwargs)
    except RetryAfter as e:
        logger.warning("RetryAfter copying: %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_copy(to_chat_id, from_chat_id, message_id, **kwargs)
    except Exception:
        logger.exception("safe_copy failed")
        return None

async def resolve_channel_link(link: str) -> Optional[int]:
    link = (link or "").strip()
    if not link:
        return None
    try:
        if link.startswith("-100") or (link.startswith("-") and link[1:].isdigit()):
            return int(link)
        if "t.me" in link:
            base = link.split("?")[0]
            name = base.rstrip("/").split("/")[-1]
            if name:
                if name.startswith("@"):
                    name = name[1:]
                ch = await bot.get_chat(name)
                return ch.id
        if link.startswith("@"):
            name = link[1:]
            ch = await bot.get_chat(name)
            return ch.id
        ch = await bot.get_chat(link)
        return ch.id
    except ChatNotFound:
        logger.warning("resolve_channel_link: chat not found %s", link)
        return None
    except Exception as e:
        logger.warning("resolve_channel_link error %s : %s", link, e)
        return None

# -------------------------
# SQLite integrity check
# -------------------------
def check_sqlite_integrity(path: str) -> bool:
    """
    Perform PRAGMA integrity_check on the sqlite file. Returns True if 'ok'.
    """
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check;")
        row = cur.fetchone()
        conn.close()
        if row and row[0] == "ok":
            return True
        logger.warning("SQLite integrity_check failed: %s", row)
        return False
    except Exception:
        logger.exception("Failed to run integrity_check on sqlite file")
        return False

# -------------------------
# DB backup & restore (multi-channel, rolling, integrity-checked, Neon mirror)
# -------------------------
async def _send_backup_to_channel(channel_id: int) -> Optional[types.Message]:
    """
    Uploads DB_PATH to channel_id, pins it, and trims older backups to MAX_BACKUPS.
    Returns the sent message object or None.
    """
    try:
        if channel_id == 0:
            return None
        if not os.path.exists(DB_PATH):
            logger.error("Local DB missing for backup")
            return None
        caption = f"DB backup {datetime.utcnow().isoformat()}"
        with open(DB_PATH, "rb") as f:
            sent = await bot.send_document(channel_id, InputFile(f, filename=os.path.basename(DB_PATH)),
                                           caption=caption,
                                           disable_notification=True)
        # pin newest (best-effort)
        try:
            await bot.pin_chat_message(channel_id, sent.message_id, disable_notification=True)
        except Exception:
            logger.exception("Failed to pin DB backup in channel %s (non-fatal)", channel_id)

        logger.info("DB backup sent to channel %s (msg %s)", channel_id, getattr(sent, "message_id", "unknown"))

        # Trim older backups (keep most recent MAX_BACKUPS documents that look like DB files)
        try:
            docs = []
            async for msg in bot.iter_history(channel_id, limit=200):
                if getattr(msg, "document", None):
                    fn = getattr(msg.document, "file_name", "") or ""
                    if os.path.basename(DB_PATH) in fn or fn.lower().endswith(".sqlite") or fn.lower().endswith(".sqlite3"):
                        docs.append(msg)
            if len(docs) > MAX_BACKUPS:
                for old in docs[MAX_BACKUPS:]:
                    try:
                        await bot.delete_message(channel_id, old.message_id)
                    except Exception:
                        logger.exception("Failed deleting old backup msg %s in channel %s", getattr(old, "message_id", None), channel_id)
        except Exception:
            logger.exception("Failed trimming old backups in channel %s", channel_id)

        return sent
    except Exception:
        logger.exception("backup to channel failed")
        return None

async def backup_db_to_channel():
    """
    Backup DB_PATH to configured DB channels (DB_CHANNEL_ID and DB_CHANNEL_ID2).
    Keeps rolling history and pins newest. Also mirrors to Neon when configured.
    Clears dirty flag on success.
    """
    try:
        results = []
        # Primary channel
        sent1 = await _send_backup_to_channel(DB_CHANNEL_ID)
        results.append(sent1)
        # Secondary channel if set
        if DB_CHANNEL_ID2 and DB_CHANNEL_ID2 != 0:
            sent2 = await _send_backup_to_channel(DB_CHANNEL_ID2)
            results.append(sent2)

        # Mirror to Neon (best-effort)
        if NEON_URL and ASYNCPG_AVAILABLE:
            try:
                ok = await neon_store_backup(DB_PATH)
                if not ok:
                    logger.warning("Neon mirror attempted but failed")
            except Exception:
                logger.exception("Neon mirror failed in backup flow")

        # on success, clear dirty flag
        await clear_db_dirty()
        return results
    except Exception:
        logger.exception("backup_db_to_channel failed")
        return None

async def _download_doc_to_tempfile(file_id: str) -> Optional[str]:
    """
    Download a Telegram file (by file_id) to a temporary file and return path, or None.
    """
    try:
        file = await bot.get_file(file_id)
        file_path = getattr(file, "file_path", None)
        file_bytes = None
        if file_path:
            try:
                file_bytes = await bot.download_file(file_path)
            except Exception:
                # fallback to HTTP download
                try:
                    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(file_url) as resp:
                            if resp.status == 200:
                                file_bytes = await resp.read()
                            else:
                                logger.error("Failed to fetch file from file_url; status %s", resp.status)
                                return None
                except Exception:
                    logger.exception("Failed fallback download of file")
                    return None
        else:
            # try direct download by id (some wrappers)
            try:
                fd = await bot.download_file_by_id(file_id)
                file_bytes = fd.read()
            except Exception:
                logger.exception("Failed direct download by id")
                return None

        if not file_bytes:
            return None

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmpname = tmp.name
        tmp.close()
        with open(tmpname, "wb") as out:
            out.write(file_bytes)
        return tmpname
    except Exception:
        logger.exception("Failed to download document to tempfile")
        return None

async def _try_restore_from_message(msg: types.Message) -> bool:
    """
    Given a Telegram message object that contains a document, attempt to download it,
    validate integrity, and replace local DB_PATH. Returns True on success.
    """
    try:
        if not getattr(msg, "document", None):
            return False
        file_id = msg.document.file_id
        tmpname = await _download_doc_to_tempfile(file_id)
        if not tmpname:
            return False
        try:
            ok = check_sqlite_integrity(tmpname)
            if not ok:
                logger.warning("Candidate DB backup failed integrity check: %s", getattr(msg, "message_id", None))
                return False
            # replace DB atomically
            try:
                db.close()
            except Exception:
                pass
            os.replace(tmpname, DB_PATH)
            # reinit db connection
            init_db(DB_PATH)
            logger.info("DB restored from message %s", getattr(msg, "message_id", None))
            await clear_db_dirty()
            return True
        finally:
            try:
                if os.path.exists(tmpname):
                    os.unlink(tmpname)
            except Exception:
                pass
    except Exception:
        logger.exception("Failed restoring from message")
        return False

async def restore_db_from_pinned(force: bool = False) -> bool:
    """
    Restore DB from pinned doc in DB channels, or from recent historical backups.
    If force=True, overwrite local DB even if it exists.
    Returns True on successful restore or if local DB already ok.
    """
    global db
    try:
        if not force and os.path.exists(DB_PATH):
            # quick integrity check: if ok, skip restore
            if check_sqlite_integrity(DB_PATH):
                logger.info("Local DB present and passed integrity check; skipping restore.")
                return True
            else:
                logger.warning("Local DB present but failing integrity; attempting restore from channel backups.")
        # Try pinned message first for primary channel, then secondary
        for ch in (DB_CHANNEL_ID, DB_CHANNEL_ID2):
            try:
                if not ch or ch == 0:
                    continue
                chat = await bot.get_chat(ch)
            except ChatNotFound:
                logger.error("DB channel %s not found during restore", ch)
                continue
            except Exception:
                logger.exception("Error fetching chat for channel %s", ch)
                continue
            pinned = getattr(chat, "pinned_message", None)
            if pinned and getattr(pinned, "document", None):
                logger.info("Found pinned backup in channel %s; attempting restore.", ch)
                ok = await _try_restore_from_message(pinned)
                if ok:
                    return True
                else:
                    logger.warning("Pinned backup in channel %s failed integrity; will try history.", ch)

        # If pinned didn't work, scan recent messages in both channels (primary first)
        for ch in (DB_CHANNEL_ID, DB_CHANNEL_ID2):
            if not ch or ch == 0:
                continue
            try:
                logger.info("Scanning recent messages in channel %s for backups", ch)
                async for msg in bot.iter_history(ch, limit=200):
                    if getattr(msg, "document", None):
                        fn = getattr(msg.document, "file_name", "") or ""
                        if os.path.basename(DB_PATH) in fn or fn.lower().endswith(".sqlite") or fn.lower().endswith(".sqlite3"):
                            ok = await _try_restore_from_message(msg)
                            if ok:
                                return True
                logger.info("No valid backups found in channel %s", ch)
            except Exception:
                logger.exception("Failed scanning channel %s history", ch)
        logger.error("No valid DB backup found in configured channels")
        return False
    except Exception:
        logger.exception("restore_db_from_pinned failed")
        return False

# -------------------------
# Delete job executor
# -------------------------
async def execute_delete_job(job_id:int, job_row:Dict[str,Any]):
    try:
        msg_ids = json.loads(job_row["message_ids"])
        target_chat = int(job_row["target_chat_id"])
        for mid in msg_ids:
            try:
                await bot.delete_message(target_chat, int(mid))
            except MessageToDeleteNotFound:
                pass
            except ChatNotFound:
                logger.warning("Chat not found when deleting messages for job %s", job_id)
            except BotBlocked:
                logger.warning("Bot blocked when deleting messages for job %s", job_id)
            except Exception:
                logger.exception("Error deleting message %s in %s", mid, target_chat)
        sql_mark_job_done(job_id)
        try:
            scheduler.remove_job(f"deljob_{job_id}")
        except Exception:
            pass
        logger.info("Executed delete job %s", job_id)
    except Exception:
        logger.exception("Failed delete job %s", job_id)

async def restore_pending_jobs_and_schedule():
    logger.info("Restoring pending delete jobs")
    pending = sql_list_pending_jobs()
    for job in pending:
        try:
            run_at = datetime.fromisoformat(job["run_at"])
            now = datetime.utcnow()
            job_id = job["id"]
            if run_at <= now:
                asyncio.create_task(execute_delete_job(job_id, job))
            else:
                scheduler.add_job(execute_delete_job, 'date', run_date=run_at, args=(job_id, job), id=f"deljob_{job_id}")
                logger.info("Scheduled delete job %s at %s", job_id, run_at.isoformat())
        except Exception:
            logger.exception("Failed to restore job %s", job.get("id"))

# -------------------------
# Health endpoint (aiohttp)
# -------------------------
WEB_RUNNER: Optional[web.AppRunner] = None

async def healthcheck(request):
    return web.Response(text="Bot is running!")

async def start_web_server():
    global WEB_RUNNER
    try:
        app = web.Application()
        app.router.add_get("/", healthcheck)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        WEB_RUNNER = runner
        logger.info("Health endpoint running on 0.0.0.0:%s/", PORT)
    except Exception:
        logger.exception("Failed to start health server")

async def stop_web_server():
    global WEB_RUNNER
    try:
        if WEB_RUNNER:
            await WEB_RUNNER.cleanup()
            WEB_RUNNER = None
            logger.info("Health endpoint stopped")
    except Exception:
        logger.exception("Failed to stop health server")

# -------------------------
# Utilities for buttons and owner check
# -------------------------
def is_owner(user_id:int)->bool:
    return user_id == OWNER_ID

def build_channel_buttons(optional_list:List[Dict[str,str]], forced_list:List[Dict[str,str]]):
    kb = InlineKeyboardMarkup()
    for ch in (optional_list or [])[:4]:
        kb.add(InlineKeyboardButton(ch.get("name","Channel"), url=ch.get("link")))
    for ch in (forced_list or [])[:3]:
        kb.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
    kb.add(InlineKeyboardButton("Help", callback_data=cb_help_button.new(action="open")))
    return kb

# -------------------------
# Deep-link token generator
# -------------------------
def generate_token(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# -------------------------
# Command handlers (start + others)
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        sql_add_user(message.from_user)
        args = message.get_args().strip()
        payload = args if args else None

        start_text = db_get("start_text", "Welcome, {first_name}!")
        start_text = start_text.replace("{username}", message.from_user.username or "").replace("{first_name}", message.from_user.first_name or "")
        optional_json = db_get("optional_channels", "[]")
        forced_json = db_get("force_channels", "[]")
        try:
            optional = json.loads(optional_json)
        except Exception:
            optional = []
        try:
            forced = json.loads(forced_json)
        except Exception:
            forced = []
        kb = build_channel_buttons(optional, forced)

        if not payload:
            start_image = db_get("start_image")
            if start_image:
                try:
                    try:
                        await bot.send_photo(message.chat.id, start_image, caption=start_text, reply_markup=kb)
                    except Exception:
                        try:
                            await bot.send_document(message.chat.id, start_image, caption=start_text, reply_markup=kb)
                        except Exception:
                            try:
                                await bot.send_animation(message.chat.id, start_image, caption=start_text, reply_markup=kb)
                            except Exception:
                                try:
                                    await bot.send_sticker(message.chat.id, start_image)
                                    await message.answer(start_text, reply_markup=kb)
                                except Exception:
                                    await message.answer(start_text, reply_markup=kb)
                except Exception:
                    await message.answer(start_text, reply_markup=kb)
            else:
                await message.answer(start_text, reply_markup=kb)
            return

        s = None
        try:
            sid = int(payload)
            s = sql_get_session_by_id(sid)
        except Exception:
            s = sql_get_session_by_token(payload)

        if not s or s.get("revoked"):
            await message.answer("This session link is invalid or revoked.")
            return

        blocked = False
        unresolved = []
        for ch in forced[:3]:
            link = ch.get("link")
            resolved = await resolve_channel_link(link)
            if resolved:
                try:
                    member = await bot.get_chat_member(resolved, message.from_user.id)
                    if getattr(member, "status", None) in ("left", "kicked"):
                        blocked = True
                        break
                except BadRequest:
                    blocked = True
                    break
                except ChatNotFound:
                    unresolved.append(link)
                except Exception:
                    unresolved.append(link)
            else:
                unresolved.append(link)

        if blocked:
            kb2 = InlineKeyboardMarkup()
            for ch in forced[:3]:
                kb2.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=s["id"])))
            await message.answer("You must join the required channels first.", reply_markup=kb2)
            return

        if unresolved:
            kb2 = InlineKeyboardMarkup()
            for ch in forced[:3]:
                kb2.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=s["id"])))
            await message.answer("Some channels could not be automatically verified. Please join them and press Retry.", reply_markup=kb2)
            return

        files = sql_get_session_files(s["id"])
        delivered_msg_ids = []
        owner_is_requester = (message.from_user.id == s.get("owner_id"))
        protect_flag = s.get("protect", 0)
        for f in files:
            try:
                if f["file_type"] == "text":
                    m = await bot.send_message(message.chat.id, f.get("caption") or "")
                    delivered_msg_ids.append(m.message_id)
                else:
                    try:
                        m = await bot.copy_message(message.chat.id, UPLOAD_CHANNEL_ID, f["vault_msg_id"],
                                                   caption=f.get("caption") or "",
                                                   protect_content=bool(protect_flag) and not owner_is_requester)
                        delivered_msg_ids.append(m.message_id)
                    except Exception:
                        # fallback by file_id
                        if f["file_type"] == "photo":
                            sent = await bot.send_photo(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "video":
                            sent = await bot.send_video(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "document":
                            sent = await bot.send_document(message.chat.id, f["file_id"], caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "sticker":
                            try:
                                sent = await bot.send_sticker(message.chat.id, f["file_id"])
                                delivered_msg_ids.append(sent.message_id)
                            except Exception:
                                pass
                        else:
                            sent = await bot.send_message(message.chat.id, f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
            except Exception:
                logger.exception("Error delivering file in session %s", s["id"])

        minutes = int(s.get("auto_delete_minutes", 0) or 0)
        if minutes and delivered_msg_ids:
            run_at = datetime.utcnow() + timedelta(minutes=minutes)
            job_db_id = sql_add_delete_job(s["id"], message.chat.id, delivered_msg_ids, run_at)
            scheduler.add_job(execute_delete_job, 'date', run_date=run_at,
                              args=(job_db_id, {"id": job_db_id, "message_ids": json.dumps(delivered_msg_ids),
                                                "target_chat_id": message.chat.id, "run_at": run_at.isoformat()}),
                              id=f"deljob_{job_db_id}")
            await message.answer(f"Messages will be auto-deleted in {minutes} minutes.")

        await message.answer("Delivery complete.")
    except Exception:
        logger.exception("Error in /start handler")
        await message.reply("An error occurred while processing your request.", parse_mode=None)

# -------------------------
# Upload commands (owner only)
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip().lower()
    exclude_text = False
    if "exclude_text" in args:
        exclude_text = True
    start_upload_session(OWNER_ID, exclude_text)
    await message.reply("Upload session started. Send media/text you want included. Use /d to finalize, /e to cancel.", parse_mode=None)

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    cancel_upload_session(OWNER_ID)
    await message.reply("Upload canceled.", parse_mode=None)

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    upload = active_uploads.get(OWNER_ID)
    if not upload:
        await message.reply("No active upload session.", parse_mode=None)
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
            await call.message.answer("Upload session expired.", parse_mode=None)
            return
        active_uploads[OWNER_ID]["_protect_choice"] = choice
        await call.message.answer("Enter auto-delete timer in minutes (0-10080). 0 = no auto-delete. Reply with a number (e.g., 60).", parse_mode=None)
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
            await m.reply("Please send a valid integer between 0 and 10080.", parse_mode=None)
            return
        upload = active_uploads.get(OWNER_ID)
        if not upload:
            await m.reply("Upload session missing.", parse_mode=None)
            return
        messages: List[types.Message] = upload.get("messages", [])
        protect = upload.get("_protect_choice", 0)

        try:
            header = await bot.send_message(UPLOAD_CHANNEL_ID, "Uploading session...")
        except ChatNotFound:
            await m.reply("Upload channel not found. Please ensure the bot is in the UPLOAD_CHANNEL.", parse_mode=None)
            logger.error("ChatNotFound uploading to UPLOAD_CHANNEL_ID")
            return
        header_msg_id = header.message_id
        header_chat_id = header.chat.id

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

        # copy/upload messages into upload channel (vault)
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

        cur = db.cursor()
        cur.execute("UPDATE sessions SET deep_link=?, header_msg_id=?, header_chat_id=? WHERE id=?", (token, header_msg_id, header_chat_id, session_temp_id))
        db.commit()
        mark_db_dirty()

        # Mirror vault header to UPLOAD_CHANNEL_ID2 if provided (best-effort)
        if UPLOAD_CHANNEL_ID2 and UPLOAD_CHANNEL_ID2 != 0:
            try:
                await bot.copy_message(UPLOAD_CHANNEL_ID2, UPLOAD_CHANNEL_ID, header_msg_id)
            except Exception:
                logger.exception("Failed mirroring header to UPLOAD_CHANNEL_ID2")

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
# Settings handlers (setmessage, setimage, setchannel)
# -------------------------
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return

    args_raw = message.get_args().strip().lower()

    if message.reply_to_message and args_raw in ("start", "help"):
        target = args_raw
        if getattr(message.reply_to_message, "text", None):
            db_set(f"{target}_text", message.reply_to_message.text)
            await message.reply(f"{target} message updated.", parse_mode=None)
            return
        else:
            await message.reply("Replied message has no text to set.", parse_mode=None)
            return

    if message.reply_to_message and not args_raw:
        if getattr(message.reply_to_message, "text", None):
            pending_setmessage[message.from_user.id] = {
                "text": message.reply_to_message.text,
                "from_chat_id": message.chat.id,
                "reply_msg_id": message.reply_to_message.message_id,
            }
            await message.reply("Reply received. Send `start` or `help` (just the word) to choose which message to set.", parse_mode=None)
            return
        else:
            await message.reply("Replied message has no text to set.", parse_mode=None)
            return

    parts = args_raw.split(" ", 1)
    if parts and parts[0] in ("start", "help") and len(parts) > 1:
        target = parts[0]
        txt = parts[1]
        db_set(f"{target}_text", txt)
        await message.reply(f"{target} message updated.", parse_mode=None)
        return

    await message.reply("Usage:\nReply to a message and use `/setmessage start` or `/setmessage help`\nOr reply and use `/setmessage` then reply with `start` or `help`.", parse_mode=None)

@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return

    args_raw = message.get_args().strip().lower()

    if message.reply_to_message and args_raw in ("start", "help"):
        target = args_raw
        rt = message.reply_to_message
        file_id = None
        if getattr(rt, "photo", None):
            file_id = rt.photo[-1].file_id
        elif getattr(rt, "document", None):
            file_id = rt.document.file_id
        elif getattr(rt, "sticker", None):
            file_id = rt.sticker.file_id
        elif getattr(rt, "animation", None):
            file_id = rt.animation.file_id
        else:
            await message.reply("Replied message must contain a photo, document (image), sticker, or animation.", parse_mode=None)
            return
        if file_id:
            db_set(f"{target}_image", file_id)
            await message.reply(f"{target} image set.", parse_mode=None)
            return
        else:
            await message.reply("Could not determine file to save.", parse_mode=None)
            return

    if message.reply_to_message and not args_raw:
        rt = message.reply_to_message
        file_id = None
        if getattr(rt, "photo", None):
            file_id = rt.photo[-1].file_id
        elif getattr(rt, "document", None):
            file_id = rt.document.file_id
        elif getattr(rt, "sticker", None):
            file_id = rt.sticker.file_id
        elif getattr(rt, "animation", None):
            file_id = rt.animation.file_id

        if not file_id:
            await message.reply("Replied message must contain a photo, document (image), sticker, or animation.", parse_mode=None)
            return

        pending_setimage[message.from_user.id] = {
            "file_id": file_id,
            "from_chat_id": message.chat.id,
            "reply_msg_id": message.reply_to_message.message_id,
        }
        await message.reply("Media received. Send `start` or `help` (just the word) to choose which image to set.", parse_mode=None)
        return

    parts = args_raw.split(" ", 1)
    if parts and parts[0] in ("start", "help") and len(parts) > 1:
        await message.reply("To set an image, reply to a media message with /setimage start (or help).", parse_mode=None)
        return

    await message.reply("Usage:\nReply to a photo/document/sticker/animation and use `/setimage start` or `/setimage help`\nOr reply and use `/setimage` then reply with `start` or `help`.", parse_mode=None)

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and m.text and m.text.strip().lower() in ("start", "help"))
async def _finalize_pending(m: types.Message):
    key = m.from_user.id
    choice = m.text.strip().lower()
    responses = []

    if key in pending_setimage:
        data_img = pending_setimage.pop(key, None)
        if data_img and data_img.get("file_id"):
            try:
                db_set(f"{choice}_image", data_img["file_id"])
                responses.append(f"{choice} image updated.")
            except Exception:
                logger.exception("Failed to set pending image")
                responses.append(f"Failed to set {choice} image.")
        else:
            responses.append(f"No pending image to set for {choice}.")

    if key in pending_setmessage:
        data_txt = pending_setmessage.pop(key, None)
        if data_txt and data_txt.get("text"):
            try:
                db_set(f"{choice}_text", data_txt["text"])
                responses.append(f"{choice} message updated.")
            except Exception:
                logger.exception("Failed to set pending message")
                responses.append(f"Failed to set {choice} message.")
        else:
            responses.append(f"No pending message to set for {choice}.")

    if not responses:
        return

    await m.reply("\n".join(responses), parse_mode=None)

@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setchannel <name> <channel_link> OR /setchannel none", parse_mode=None)
        return
    if args.lower() == "none":
        db_set("force_channels", json.dumps([]))
        await message.reply("Forced channels cleared.", parse_mode=None)
        return
    parts = args.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Provide name and link.", parse_mode=None)
        return
    name, link = parts[0].strip(), parts[1].strip()
    try:
        arr = json.loads(db_get("force_channels", "[]"))
    except Exception:
        arr = []
    updated = False
    for entry in arr:
        if entry.get("name") == name or entry.get("link") == link:
            entry["name"] = name
            entry["link"] = link
            updated = True
            break
    if not updated:
        if len(arr) >= 3:
            await message.reply("Max 3 forced channels allowed.", parse_mode=None)
            return
        arr.append({"name": name, "link": link})
    db_set("force_channels", json.dumps(arr))
    await message.reply("Forced channels updated.", parse_mode=None)

# -------------------------
# Help handlers
# -------------------------
@dp.callback_query_handler(cb_help_button.filter())
async def cb_help(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    txt = db_get("help_text", "Help is not set.")
    img = db_get("help_image")
    try:
        if img:
            try:
                await bot.send_photo(call.from_user.id, img, caption=txt)
            except Exception:
                try:
                    await bot.send_document(call.from_user.id, img, caption=txt)
                except Exception:
                    try:
                        await bot.send_animation(call.from_user.id, img, caption=txt)
                    except Exception:
                        try:
                            await bot.send_sticker(call.from_user.id, img)
                            await bot.send_message(call.from_user.id, txt)
                        except Exception:
                            await bot.send_message(call.from_user.id, txt)
        else:
            await bot.send_message(call.from_user.id, txt)
    except Exception:
        logger.exception("Failed to send help to user")
        try:
            await call.message.answer("Failed to open help.", parse_mode=None)
        except Exception:
            pass

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    txt = db_get("help_text", "Help is not set.")
    img = db_get("help_image")
    if img:
        try:
            await message.reply_photo(img, caption=txt)
        except Exception:
            await message.reply(txt, parse_mode=None)
    else:
        await message.reply(txt, parse_mode=None)

# -------------------------
# Admin & utility commands
# -------------------------
@dp.message_handler(commands=["adminp"])
async def cmd_adminp(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    s = sql_stats()
    txt = (
        "Owner panel:\n"
        "/upload - start upload session\n"
        "/d - finalize upload (choose protect + minutes)\n"
        "/e - cancel upload\n"
        "/setmessage - set start/help text\n"
        "/setimage - set start/help image (reply to a photo/sticker/document)\n"
        "/setchannel - add forced join channel\n"
        "/stats - show stats\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke a session\n"
        "/broadcast - reply to message to broadcast\n"
        "/backup_db - backup DB to DB channel(s) and Neon\n"
        "/restore_db - restore DB from pinned\n\n"
        f"Stats: Active(2d): {s['active_2d']}  Total users: {s['total_users']}  Files: {s['files']}  Sessions: {s['sessions']}"
    )
    await message.reply(txt, parse_mode=None)

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    s = sql_stats()
    await message.reply(f"Active(2d): {s['active_2d']}\nTotal users: {s['total_users']}\nTotal files: {s['files']}\nSessions: {s['sessions']}", parse_mode=None)

@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    rows = sql_list_sessions(200)
    if not rows:
        await message.reply("No sessions.", parse_mode=None)
        return
    out = []
    for r in rows:
        out.append(f"ID:{r['id']} created:{r['created_at']} protect:{r['protect']} auto_min:{r['auto_delete_minutes']} revoked:{r['revoked']} token:{r['deep_link']}")
    msg = "\n".join(out)
    if len(msg) > 4000:
        await message.reply("Too many sessions to display.", parse_mode=None)
    else:
        await message.reply(msg, parse_mode=None)

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /revoke <id>", parse_mode=None)
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id", parse_mode=None)
        return
    sql_set_session_revoked(sid, 1)
    await message.reply(f"Session {sid} revoked.", parse_mode=None)

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast.", parse_mode=None)
        return

    cur = db.cursor()
    cur.execute("SELECT id FROM users")
    users = [r["id"] for r in cur.fetchall()]
    if not users:
        await message.reply("No users to broadcast to.", parse_mode=None)
        return
    await message.reply(f"Starting broadcast to {len(users)} users.", parse_mode=None)
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

@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    sent = await backup_db_to_channel()
    if sent:
        await message.reply("DB backed up to channel(s) and Neon (if configured).", parse_mode=None)
    else:
        await message.reply("Backup failed.", parse_mode=None)

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    ok = await restore_db_from_pinned(force=True)
    if ok:
        await message.reply("DB restored.", parse_mode=None)
    else:
        await message.reply("Restore failed.", parse_mode=None)

@dp.message_handler(commands=["del_session"])
async def cmd_del_session(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.", parse_mode=None)
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /del_session <id>", parse_mode=None)
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id", parse_mode=None)
        return
    cur = db.cursor()
    cur.execute("DELETE FROM sessions WHERE id=?", (sid,))
    db.commit()
    mark_db_dirty()
    await message.reply("Session deleted.", parse_mode=None)

# -------------------------
# Callback retry handler
# -------------------------
@dp.callback_query_handler(cb_retry.filter())
async def cb_retry_handler(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    session_id = int(callback_data.get("session"))
    await call.message.answer("Please re-open the deep link you received (tap it in chat) to retry delivery. If channels are joined, delivery should proceed.", parse_mode=None)

# -------------------------
# Error handler
# -------------------------
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Update handling failed: %s", exception)
    return True

# -------------------------
# Catch-all uploader & user-lastseen handler (SAFE)
# -------------------------
def _upload_or_noncommand_filter(m: types.Message) -> bool:
    if m.from_user and m.from_user.id == OWNER_ID:
        return OWNER_ID in active_uploads
    if getattr(m, "text", None):
        return not m.text.startswith("/")
    return True

@dp.message_handler(_upload_or_noncommand_filter, content_types=types.ContentTypes.ANY)
async def catch_all_store_uploads(message: types.Message):
    try:
        if message.from_user.id != OWNER_ID:
            sql_update_user_lastseen(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "", message.from_user.last_name or "")
            return
        if OWNER_ID in active_uploads:
            if message.text and message.text.strip().startswith("/"):
                return
            if message.text and active_uploads[OWNER_ID].get("exclude_text"):
                pass
            else:
                append_upload_message(OWNER_ID, message)
                try:
                    await message.reply("Stored in upload session.", parse_mode=None)
                except Exception:
                    pass
    except Exception:
        logger.exception("Error in catch_all_store_uploads")

# -------------------------
# Debounced backup job + periodic backup
# -------------------------
async def debounced_backup_job():
    """
    Runs periodically (every DEBOUNCE_BACKUP_MINUTES). If DB_DIRTY is set, perform a backup.
    """
    global DB_DIRTY
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
    # Initialize Neon pool if configured
    try:
        await init_neon_pool()
    except Exception:
        logger.exception("init_neon_pool failed on startup")

    # Attempt restore from channel backups if local DB missing or corrupted
    try:
        await restore_db_from_pinned(force=True)
    except Exception:
        logger.exception("restore_db_from_pinned error on startup")

    # start scheduler
    try:
        scheduler.start()
    except Exception:
        logger.exception("Scheduler start error")

    # restore delete jobs and schedule them
    try:
        await restore_pending_jobs_and_schedule()
    except Exception:
        logger.exception("restore_pending_jobs_and_schedule error")

    # schedule periodic debounced backups and periodic safety backups
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

    # start health endpoint (aiohttp) - we run it separately in main(), but keep this as safety if needed
    try:
        asyncio.create_task(start_web_server())
    except Exception:
        logger.exception("Failed to start health app task")

    # basic checks: upload & DB channels
    try:
        await bot.get_chat(UPLOAD_CHANNEL_ID)
    except ChatNotFound:
        logger.error("Upload channel not found. Please add the bot to the upload channel.")
    except Exception:
        logger.exception("Error checking upload channel")
    if UPLOAD_CHANNEL_ID2 and UPLOAD_CHANNEL_ID2 != 0:
        try:
            await bot.get_chat(UPLOAD_CHANNEL_ID2)
        except ChatNotFound:
            logger.error("Upload channel 2 not found.")
        except Exception:
            logger.exception("Error checking upload channel 2")
    try:
        await bot.get_chat(DB_CHANNEL_ID)
    except ChatNotFound:
        logger.error("DB channel not found. Please add the bot to the DB channel.")
    except Exception:
        logger.exception("Error checking DB channel")
    if DB_CHANNEL_ID2 and DB_CHANNEL_ID2 != 0:
        try:
            await bot.get_chat(DB_CHANNEL_ID2)
        except ChatNotFound:
            logger.error("DB channel 2 not found.")
        except Exception:
            logger.exception("Error checking DB channel 2")

    # store bot username
    me = await bot.get_me()
    db_set("bot_username", me.username or "")

    # initialize start/help values if missing
    if db_get("start_text") is None:
        db_set("start_text", "Welcome, {first_name}!")
    if db_get("help_text") is None:
        db_set("help_text", "This bot delivers sessions.")

    # set bot commands: default users only see /start and /help
    try:
        default_commands = [
            BotCommand("start", "Start / open deep link"),
            BotCommand("help", "Show help"),
        ]
        await bot.set_my_commands(default_commands)
        # set admin-scoped commands for OWNER_ID (private chat scope)
        try:
            admin_commands = [
                BotCommand("start", "Start / open deep link"),
                BotCommand("help", "Show help"),
                BotCommand("adminp", "Owner panel"),
                BotCommand("stats", "Stats (owner)")
            ]
            # scope to owner's private chat so only owner sees these extra commands
            await bot.set_my_commands(admin_commands, scope=types.BotCommandScopeChat(OWNER_ID))
        except Exception:
            logger.exception("Failed setting admin-scoped commands")
    except Exception:
        logger.exception("Couldn't set bot commands")

    logger.info("on_startup complete")

async def on_shutdown(dispatcher):
    logger.info("Shutting down")
    try:
        # final backup if dirty
        if DB_DIRTY:
            logger.info("Final backup on shutdown (DB dirty).")
            await backup_db_to_channel()
    except Exception:
        logger.exception("Final backup failed")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    # close Neon pool
    try:
        await close_neon_pool()
    except Exception:
        logger.exception("Failed closing Neon pool")
    # stop web server
    try:
        await stop_web_server()
    except Exception:
        logger.exception("Failed stopping web server")
    await bot.close()

# -------------------------
# Run (async main combining aiogram polling + aiohttp)
# -------------------------
async def main_async():
    # start web server first
    await start_web_server()

    # start polling (this will block until shutdown)
    try:
        await dp.start_polling(on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)
    except asyncio.CancelledError:
        logger.info("Polling cancelled")
    except Exception:
        logger.exception("Fatal error in polling")

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopped by user")
    except Exception:
        logger.exception("Fatal error")