# -*- coding: utf-8 -*-
import os
import threading
import asyncio
import logging  # Logging import zaroori hai
import random
import json
import requests
import signal
import sys
import re
# import anthropic  # Agar zaroorat ho toh uncomment karein
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse, quote
from collections import defaultdict
from telegram.error import RetryAfter, TelegramError
from typing import Optional
from psycopg2 import pool
from PIL import Image, ImageFilter, ImageOps
from io import BytesIO

# ==================== 1. LOGGING SETUP (SABSE PEHLE YEH AAYEGA) ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Change from INFO to DEBUG if needed
)
logger = logging.getLogger(__name__)

# ==================== 2. AB IMDB CHECK KAREIN (AB YE SAFE HAI) ====================
try:
    from imdb import Cinemagoer
    ia = Cinemagoer()
except ImportError:
    # Ab logger define ho chuka hai, toh yeh error nahi dega
    logger.warning("imdb (cinemagoer) module not found. Run: pip install cinemagoer")
    ia = None

# ==================== 3. BAAKI IMPORTS ====================
# Third-party imports
from bs4 import BeautifulSoup
import telegram
import psycopg2
from flask import Flask, request, session, g
import google.generativeai as genai
from googleapiclient.discovery import build
from fuzzywuzzy import process, fuzz
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler
)

# Local imports
import admin_views as admin_views_module

# Try to import db_utils
try:
    import db_utils
    FIXED_DATABASE_URL = getattr(db_utils, "FIXED_DATABASE_URL", None)
except Exception:
    FIXED_DATABASE_URL = None

# ==================== GLOBAL VARIABLES ====================
background_tasks = set()
PENDING_DRAFTS = {}  # यह "Temporary Memory" है जहाँ फाइलें जमा होंगी
# ==================== CONVERSATION STATES (YEH MISSING HAI) ====================
WAITING_FOR_NAME, CONFIRMATION = range(2)
SEARCHING, REQUESTING, MAIN_MENU, REQUESTING_FROM_BUTTON = range(2, 6)
# ================= CONFIGURATION =================
FORUM_GROUP_ID = -1003696437312  # Apne Group ki ID

# Yahan Topic ID aur uske Keywords set karo
# Bot in shabdon ko Genre ya Description me dhundega
TOPIC_MAPPING = {
    # Format:  Topic_ID:  ['keyword1', 'keyword2', 'keyword3']
    
    20: ['south', 'telugu', 'tamil', 'kannada', 'malayalam', 'allu arjun'], # South Topic (ID: 12)
    32: ['hollywood', 'english', 'marvel', 'dc', 'disney'],                 # Hollywood Topic (ID: 34)
    16: ['bollywood', 'hindi', 'khan', 'kapoor'],                           # Bollywood Topic (ID: 56)
    18: ['series', 'season', 'episode', 'netflix', 'amazon'],               # Web Series Topic (ID: 78)
    22: ['anime', 'cartoon', 'animation'],                                  # Anime Topic (ID: 90)
    
    # Default Topic (Agar kuch match na ho to yahan jayega)
    100: ['default'] 
}
# =================================================

def get_auto_topic_id(genre, description):
    """
    Ye function movie ke data ko padhkar sahi Topic ID batata hai.
    """
    text_to_check = (str(genre) + " " + str(description)).lower()
    
    for topic_id, keywords in TOPIC_MAPPING.items():
        for word in keywords:
            if word in text_to_check:
                return topic_id
    
    return 100 # Agar kuch samajh na aaye to Default Topic ID

async def post_to_topic_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Fixed Version: Deep Link format ab 'movie_' use karega jo start handler me hai.
    """
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID: return

    # 1. MOVIE SEARCH
    movie_search_name = " ".join(context.args).strip()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT id, title, year, rating, genre, poster_url, description, category FROM movies"
    
    if movie_search_name:
        cursor.execute(f"{query} WHERE title ILIKE %s", (f"%{movie_search_name}%",))
    elif BATCH_SESSION.get('active'):
        cursor.execute(f"{query} WHERE id = %s", (BATCH_SESSION['movie_id'],))
    else:
        await update.message.reply_text("❌ Naam batao! Example: `/post Pushpa`")
        close_db_connection(conn)
        return

    movie_data = cursor.fetchone()
    cursor.close()
    close_db_connection(conn)

    if not movie_data:
        await update.message.reply_text("❌ Movie nahi mili.")
        return

    # Unpack 8 values
    movie_id, title, year, rating, genre, poster_url, description, category = movie_data

    # 2. TOPIC SELECTION
    topic_id = 100 # Default
    cat_lower = str(category).lower()

    for tid, keywords in TOPIC_MAPPING.items():
        if cat_lower in [k.lower() for k in keywords]: 
            topic_id = tid
            break
            
    if topic_id == 100:
        if "south" in cat_lower: topic_id = 20
        elif "hollywood" in cat_lower: topic_id = 1
        elif "bollywood" in cat_lower: topic_id = 16
        elif "anime" in cat_lower: topic_id = 22
        elif "series" in cat_lower: topic_id = 18

    # 3. HANDLE MISSING DATA
    final_photo = poster_url if poster_url and poster_url != 'N/A' else DEFAULT_POSTER
    short_desc = (description[:150] + "...") if description else "Plot details unavailable."

    # 4. CAPTION
    caption = (
        f"🎬 **{title} ({year})**\n\n"
        f"⭐️ **Rating:** {rating}/10\n"
        f"🎭 **Genre:** {genre}\n"
        f"🏷 **Category:** {category}\n\n"
        f"📜 **Story:** {short_desc}\n\n"
        f"👇 **Click Below to Download** 👇"
    )

    # 5. 🔗 CORRECTED DEEP LINKS (movie_ prefix)
    link_param = f"movie_{movie_id}"  # <--- YEH CHANGE KIYA HAI (Ab Start handler pakad lega)
    
    bot1_username = "FlimfyBox_SearchBot"
    bot2_username = "urmoviebot"
    bot3_username = "FlimfyBox_Bot"

    link1 = f"https://t.me/{bot1_username}?start={link_param}"
    link2 = f"https://t.me/{bot2_username}?start={link_param}"
    link3 = f"https://t.me/{bot3_username}?start={link_param}"

    keyboard = [
        [
            InlineKeyboardButton("📥 Download Server 1", url=link1),
            InlineKeyboardButton("📥 Download Server 2", url=link2)
        ],
        [
            InlineKeyboardButton("⚡ Fast Download (Server 3)", url=link3)
        ]
    ]

    try:
        await context.bot.send_photo(
            chat_id=FORUM_GROUP_ID,
            message_thread_id=topic_id,
            photo=final_photo,
            caption=caption,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        await update.message.reply_text(f"✅ Posted **{title}** in Topic `{topic_id}` with Working Links!")
    except Exception as e:
        logger.error(f"Post failed: {e}")
        await update.message.reply_text(f"❌ Error: {e}")
# ==================== ENVIRONMENT VARIABLES ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get('DATABASE_URL')
    # 👇👇👇 START COPY HERE 👇👇👇
db_pool = None
try:
    # Pool create kar rahe hain taki baar baar connection na banana pade
    pool_url = FIXED_DATABASE_URL or DATABASE_URL
    if pool_url:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20, # Min 1, Max 20 connections
            dsn=pool_url
        )
        logger.info("✅ Database Connection Pool Created!")
except Exception as e:
    logger.error(f"❌ Error creating pool: {e}")
# 👆👆👆 END COPY HERE 👆👆👆
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
_admin_id = os.environ.get('ADMIN_USER_ID', '0')
ADMIN_USER_ID = int(_admin_id) if _admin_id.isdigit() else 0
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')
REQUIRED_CHANNEL_ID = os.environ.get('REQUIRED_CHANNEL_ID', '-1003330141433')
REQUIRED_GROUP_ID = os.environ.get('REQUIRED_GROUP_ID', '-1003460387180')
FILMFYBOX_GROUP_URL = 'https://t.me/FlimfyBox'
FILMFYBOX_CHANNEL_URL = 'https://t.me/FilmFyBoxMoviesHD'  # Yahan apna Channel Link dalein
REQUEST_CHANNEL_ID = os.environ.get('REQUEST_CHANNEL_ID', '-1003078990647')
DUMP_CHANNEL_ID = os.environ.get('DUMP_CHANNEL_ID', '-1002683355160')
FORCE_JOIN_ENABLED = True

# ✅ NEW ENVIRONMENT VARIABLES FOR MULTI-CHANNEL & AI
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")  # ✅ NEW: Claude API Key
STORAGE_CHANNELS = os.environ.get("STORAGE_CHANNELS", "")  # ✅ NEW: Backup Channels List

# Verified users cache (Taaki baar baar API call na ho)
verified_users = {}
VERIFICATION_CACHE_TIME = 3600  # 1 Hour

# --- Random GIF IDs for Search Failure ---
SEARCH_ERROR_GIFS = [
    'https://media.giphy.com/media/26hkhKd2Cp5WMWU1O/giphy.gif',
    'https://media.giphy.com/media/3o7aTskHEUdgCQAXde/giphy.gif',
    'https://media.giphy.com/media/l2JhkHg5y5tW3wO3u/giphy.gif',
    'https://media.giphy.com/media/14uQ3cOFteDaU/giphy.gif',
    'https://media.giphy.com/media/xT9IgG50Fb7Mi0prBC/giphy.gif',
    'https://media.giphy.com/media/3o7abB06u9bNzA8lu8/giphy.gif',
    'https://media.giphy.com/media/3o7qDP7gNY08v4wYLy/giphy.gif',
]

# Rate limiting dictionary
user_last_request = defaultdict(lambda: datetime.min)

# ===== Configurable rate-limiting and fuzzy settings =====
REQUEST_COOLDOWN_MINUTES = int(os.environ.get('REQUEST_COOLDOWN_MINUTES', '10'))
SIMILARITY_THRESHOLD = int(os.environ.get('SIMILARITY_THRESHOLD', '80'))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '10'))

# Auto-delete tracking
messages_to_auto_delete = defaultdict(list)

# ✅ NEW GLOBAL VARIABLES FOR BATCH SESSION
BATCH_SESSION = {'active': False, 'movie_id': None, 'movie_title': None, 'file_count': 0, 'admin_id': None}

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")


# 👇👇👇 START COPY HERE (Line 290 ke aas-paas paste karein) 👇👇👇
import functools

async def run_async(func, *args, **kwargs):
    """
    Ye function blocking code (jaise Database/Fuzzy search) ko
    background thread me chalata hai taaki bot hang na ho.
    """
    func_partial = functools.partial(func, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(None, func_partial)
# 👆👆👆 END COPY HERE 👆👆👆

# ==================== UTILITY FUNCTIONS ====================
def preprocess_query(query):
    """Clean and normalize user query"""
    query = re.sub(r'[^\w\s-]', '', query)
    query = ' '.join(query.split())
    stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free']
    words = query.lower().split()
    words = [w for w in words if w not in stop_words]
    return ' '.join(words).strip()

async def check_rate_limit(user_id):
    """Check if user is rate limited"""
    now = datetime.now()
    last_request = user_last_request[user_id]

    if now - last_request < timedelta(seconds=2):
        return False

    user_last_request[user_id] = now
    return True

# ==================== MEMBERSHIP CHECK LOGIC ====================
async def is_user_member(context, user_id: int, force_fresh: bool = False):
    """Check if user is member of channel and group (Matching Reference Code)"""
    
    # Agar FSub disabled hai to turant pass kar do
    if not FORCE_JOIN_ENABLED:
        return {'is_member': True, 'channel': True, 'group': True, 'error': None}
    
    current_time = datetime.now()
    
    # 1. Cache Check
    # Agar force_fresh FALSE hai, tabhi cache check karo
    if not force_fresh and user_id in verified_users:
        last_checked, cached = verified_users[user_id]
        # Agar 1 ghante (3600s) se kam hua hai to cache use karo
        if (current_time - last_checked).total_seconds() < VERIFICATION_CACHE_TIME:
            return cached
    
    result = {
        'is_member': False,
        'channel': False,
        'group': False,
        'error': None
    }
    
    # Valid statuses jo member maane jayenge
    VALID_STATUSES = ['member', 'administrator', 'creator']
    
    # 2. Check Channel
    try:
        channel_member = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        if channel_member.status in VALID_STATUSES:
            result['channel'] = True
    except Exception as e:
        logger.error(f"Channel Check Error: {e}")
        # Agar bot admin nahi hai ya error hai, to assume karo join hai (Safe Fallback)
        result['channel'] = False 

    # 3. Check Group
    try:
        group_member = await context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        if group_member.status in VALID_STATUSES:
            result['group'] = True
    except Exception as e:
        logger.error(f"Group Check Error: {e}")
        result['group'] = False

    # 4. Final Result
    result['is_member'] = result['channel'] and result['group']
    
    # 5. Update Cache (Cache me naya status save karo)
    verified_users[user_id] = (current_time, result)
    
    return result

def get_join_keyboard():
    """Join buttons keyboard"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL),
            InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)
        ],
        [InlineKeyboardButton("✅ Joined Both - Verify", callback_data="verify")]
    ])

def get_join_message(channel_status, group_status):
    """Generate message based on what is missing"""
    if not channel_status and not group_status:
        missing = "Channel and Group both"
    elif not channel_status:
        missing = "Channel"
    else:
        missing = "Group"
    
    return (
        f"📂 **Your File is Ready!**\n\n"
        f"🚫 **But Access Denied**\n\n"
        f"You haven't joined {missing}!\n\n"
        f"📢 Channel: {'✅' if channel_status else '❌'}\n"
        f"💬 Group: {'✅' if group_status else '❌'}\n\n"
        f"Join both, then click **Verify** button 👇"
    )

def is_valid_url(url):
    """Check if a URL is valid"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def normalize_url(url):
    """Normalize and clean URLs"""
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        if 'blogspot.com' in url and 'import-urlhttpsfonts' in url:
            url = url.replace('import-urlhttpsfonts', 'import-url-https-fonts')

        if '#' in url:
            base, anchor = url.split('#', 1)
            parsed = urlparse(base)
            normalized_base = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                ''
            ))
            url = f"{normalized_base}#{anchor}"
        else:
            parsed = urlparse(url)
            url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))

        return url
    except:
        return url

def _normalize_title_for_match(title: str) -> str:
    """Normalize title for fuzzy matching"""
    if not title:
        return ""
    t = re.sub(r'[^\w\s]', ' ', title)
    t = re.sub(r'\s+', ' ', t).strip()
    return t.lower()

# NEW: Function to safely escape characters for Admin Notification
def escape_markdown_v2(text: str) -> str:
    """Escapes special characters for Markdown V2 formatting."""
    # Use the simplest escape for characters that commonly break parsing
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

def get_last_similar_request_for_user(user_id: int, title: str, minutes_window: int = REQUEST_COOLDOWN_MINUTES):
    """Look up the user's most recent request that is sufficiently similar to title"""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT movie_title, requested_at
            FROM user_requests
            WHERE user_id = %s
            ORDER BY requested_at DESC
            LIMIT 200
        """, (user_id,))
        rows = cur.fetchall()
        cur.close()
        close_db_connection(conn)

        if not rows:
            return None

        now = datetime.now()
        cutoff = now - timedelta(minutes=minutes_window)
        norm_target = _normalize_title_for_match(title)

        for stored_title, requested_at in rows:
            if not stored_title or not requested_at:
                continue
            try:
                if isinstance(requested_at, datetime):
                    requested_time = requested_at
                else:
                    requested_time = datetime.strptime(str(requested_at), '%Y-%m-%d %H:%M:%S')
            except Exception:
                requested_time = requested_at

            if requested_time < cutoff:
                break

            norm_stored = _normalize_title_for_match(stored_title)
            score = fuzz.token_sort_ratio(norm_target, norm_stored)
            if score >= SIMILARITY_THRESHOLD:
                return {
                    "stored_title": stored_title,
                    "requested_at": requested_time,
                    "score": score
                }

        return None
    except Exception as e:
        logger.error(f"Error checking last similar request for user {user_id}: {e}")
        try:
            close_db_connection(conn)
        except:
            pass
        return None

def user_burst_count(user_id: int, window_seconds: int = 60):
    """Count how many requests this user made in the last window_seconds"""
    conn = get_db_connection()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        since = datetime.now() - timedelta(seconds=window_seconds)
        cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s AND requested_at >= %s", (user_id, since))
        
        result = cur.fetchone()
        cnt = result[0] if result else 0 
        
        cur.close()
        close_db_connection(conn)
        return cnt
    except Exception as e:
        logger.error(f"Error counting burst requests for user {user_id}: {e}")
        try:
            close_db_connection(conn)
        except:
            pass
        return 0

# ==================== FIXED AUTO-DELETE FUNCTIONS ====================

async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Delete messages after specified delay using Background Tasks"""
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                logger.info(f"🗑️ Deleted message {msg_id}")
            except Exception:
                pass # Message shayad pehle hi delete ho gaya ho
    except Exception as e:
        logger.error(f"Error in delete task: {e}")

def track_message_for_deletion(context, chat_id, message_id, delay=60):
    """
    Schedules a message for deletion using asyncio tasks.
    IMPORTANT: Requires 'context' as the first argument.
    """
    if not message_id: return
    
    # Task create karein
    task = asyncio.create_task(
        delete_messages_after_delay(context, chat_id, [message_id], delay)
    )
    # Global set me add karein taki task beech me na ruke
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)

# ==================== DATABASE FUNCTIONS ====================

def setup_database():
    """Setup database tables and indexes - UPDATED with IMDb columns"""
    try:
        conn_str = FIXED_DATABASE_URL or DATABASE_URL
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')

        # ✅ UPDATED TABLE: Added imdb_id, poster_url, year, genre columns
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL,
                file_id TEXT,
                is_unreleased BOOLEAN DEFAULT FALSE,
                imdb_id TEXT,
                poster_url TEXT,
                year INTEGER,
                genre TEXT,
                rating TEXT  -- 👈 Added Rating
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS movie_files (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                quality TEXT NOT NULL,
                url TEXT,
                file_id TEXT,
                file_size TEXT,
                UNIQUE(movie_id, quality)
            )
        ''')

        cur.execute('CREATE TABLE IF NOT EXISTS sync_info (id SERIAL PRIMARY KEY, last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP);')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS user_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                movie_title TEXT NOT NULL,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notified BOOLEAN DEFAULT FALSE,
                group_id BIGINT,
                message_id BIGINT
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS movie_aliases (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                alias TEXT NOT NULL,
                UNIQUE(movie_id, alias)
            )
        ''')

        cur.execute('''
            DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'user_requests_unique_constraint') THEN
                ALTER TABLE user_requests ADD CONSTRAINT user_requests_unique_constraint UNIQUE (user_id, movie_title);
            END IF;
            END $$;
        ''')

        # Add columns if they don't exist (for existing databases)
        try:
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS file_id TEXT;")
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS is_unreleased BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS imdb_id TEXT;")
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS poster_url TEXT;")
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS year INTEGER;")
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS genre TEXT;")
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS rating TEXT;") # 👈 Fix missing column
            cur.execute("ALTER TABLE user_requests ADD COLUMN IF NOT EXISTS message_id BIGINT;")
        except Exception as e:
            logger.info(f"Column addition note: {e}")

        # ✅ Add backup_map column for Multi-Channel support
        try:
            cur.execute("ALTER TABLE movie_files ADD COLUMN IF NOT EXISTS backup_map JSONB DEFAULT '{}'::jsonb;")
        except Exception as e:
            logger.info(f"Column backup_map note: {e}")

        # Create indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_imdb_id ON movies (imdb_id);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_year ON movies (year);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_movie_title ON user_requests (movie_title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_user_id ON user_requests (user_id);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movie_aliases_alias ON movie_aliases (alias);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movie_files_movie_id ON movie_files (movie_id);')

        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        logger.info("Continuing without database setup...")


def migrate_add_imdb_columns():
    """One-time migration to add imdb_id column if not exists"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS imdb_id TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS poster_url TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS year INTEGER;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS genre TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS rating TEXT;") # 👈 Important
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_imdb_id ON movies (imdb_id);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_year ON movies (year);')
        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info("✅ DB Migration: IMDb columns added successfully")
        return True
    except Exception as e:
        logger.error(f"Migration Error: {e}")
        if conn: 
            close_db_connection(conn)
        return False


# 👇👇👇 START COPY HERE (New Function) 👇👇👇
def get_db_connection():
    """Pool se connection lene wala naya function"""
    if not db_pool:
        logger.error("Database pool is not ready.")
        return None
    try:
        conn = db_pool.getconn()
        return conn
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
        return None

def close_db_connection(conn):
    """Connection ko wapas pool me dalne ke liye helper"""
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception:
            pass
# 👆👆👆 END COPY HERE 👆👆👆

def update_movies_in_db():
    """Update movies from Blogger API"""
    logger.info("Starting movie update process...")
    setup_database()

    conn = None
    cur = None
    new_movies_added = 0

    try:
        conn = get_db_connection()
        if not conn:
            return "Database connection failed"

        cur = conn.cursor()

        cur.execute("SELECT last_sync FROM sync_info ORDER BY id DESC LIMIT 1;")
        last_sync_result = cur.fetchone()
        last_sync_time = last_sync_result[0] if last_sync_result else None

        cur.execute("SELECT title FROM movies;")
        existing_movies = {row[0] for row in cur.fetchall()}

        if not BLOGGER_API_KEY or not BLOG_ID:
            return "Blogger API keys not configured"

        service = build('blogger', 'v3', developerKey=BLOGGER_API_KEY)
        all_items = []

        posts_request = service.posts().list(blogId=BLOG_ID, maxResults=500)
        while posts_request is not None:
            posts_response = posts_request.execute()
            all_items.extend(posts_response.get('items', []))
            posts_request = service.posts().list_next(posts_request, posts_response)

        pages_request = service.pages().list(blogId=BLOG_ID)
        pages_response = pages_request.execute()
        all_items.extend(pages_response.get('items', []))

        unique_titles = set()
        for item in all_items:
            title = item.get('title')
            url = item.get('url')

            if last_sync_time and 'published' in item:
                try:
                    published_time = datetime.strptime(item['published'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    if published_time < last_sync_time:
                        continue
                except:
                    pass

            if title and url and title.strip() not in existing_movies and title.strip() not in unique_titles:
                try:
                    cur.execute("INSERT INTO movies (title, url) VALUES (%s, %s);", (title.strip(), url.strip()))
                    new_movies_added += 1
                    unique_titles.add(title.strip())
                except psycopg2.Error as e:
                    logger.error(f"Error inserting movie {title}: {e}")
                    conn.rollback()
                    continue

        cur.execute("INSERT INTO sync_info (last_sync) VALUES (CURRENT_TIMESTAMP);")

        conn.commit()
        return f"Update complete. Added {new_movies_added} new items."

    except Exception as e:
        logger.error(f"Error during movie update: {e}")
        if conn:
            conn.rollback()
        return f"An error occurred during update: {e}"

    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)


def get_movies_from_db(user_query, limit=10):
    """Search for MULTIPLE movies in database with fuzzy matching"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor()

        logger.info(f"Searching for: '{user_query}'")

        # ✅ Updated to include new columns
        cur.execute(
            """SELECT id, title, url, file_id, imdb_id, poster_url, year, genre 
               FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s""",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()

        if exact_matches:
            logger.info(f"Found {len(exact_matches)} exact matches")
            cur.close()
            close_db_connection(conn)
            return exact_matches

        cur.execute("""
            SELECT DISTINCT m.id, m.title, m.url, m.file_id, m.imdb_id, m.poster_url, m.year, m.genre
            FROM movies m
            JOIN movie_aliases ma ON m.id = ma.movie_id
            WHERE LOWER(ma.alias) LIKE LOWER(%s)
            ORDER BY m.title
            LIMIT %s
        """, (f'%{user_query}%', limit))
        alias_matches = cur.fetchall()

        if alias_matches:
            logger.info(f"Found {len(alias_matches)} alias matches")
            cur.close()
            close_db_connection(conn)
            return alias_matches

        cur.execute("SELECT id, title, url, file_id, imdb_id, poster_url, year, genre FROM movies")
        all_movies = cur.fetchall()

        if not all_movies:
            cur.close()
            close_db_connection(conn)
            return []

        movie_titles = [movie[1] for movie in all_movies]
        movie_dict = {movie[1]: movie for movie in all_movies}

        matches = process.extract(user_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=limit)

        filtered_movies = [movie_dict[title] for title, score, index in matches if score >= 65]

        logger.info(f"Found {len(filtered_movies)} fuzzy matches")

        cur.close()
        close_db_connection(conn)
        return filtered_movies[:limit]

    except Exception as e:
        logger.error(f"Database query error: {e}")
        return []
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass


def get_movies_fast_sql(query: str, limit: int = 5):
    """
    Smart SQL Search: Fast like SQL + Smart like FuzzyWuzzy.
    Handles typos using PostgreSQL 'pg_trgm' (Similarity).
    """
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor()
        
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        
        # ✅ Updated to include new columns
        sql = """
            SELECT m.id, m.title, m.url, m.file_id, m.imdb_id, m.poster_url, m.year, m.genre,
                   SIMILARITY(m.title, %s) as sim_score
            FROM movies m
            WHERE SIMILARITY(m.title, %s) > 0.3
            ORDER BY sim_score DESC
            LIMIT %s
        """
        
        cur.execute(sql, (query, query, limit))
        results = cur.fetchall()
        
        # Format results (remove score from tuple)
        final_results = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in results]
        
        cur.close()
        return final_results

    except Exception as e:
        logger.error(f"Smart SQL Search Error: {e}")
        return []
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass


def get_movie_by_imdb_id(imdb_id: str):
    """Get movie from database by IMDb ID"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor()
        cur.execute(
            """SELECT id, title, url, file_id, imdb_id, poster_url, year, genre 
               FROM movies WHERE imdb_id = %s LIMIT 1""",
            (imdb_id,)
        )
        result = cur.fetchone()
        cur.close()
        close_db_connection(conn)
        return result

    except Exception as e:
        logger.error(f"Error fetching movie by IMDb ID: {e}")
        return None
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass


def update_movie_metadata(movie_id: int, imdb_id: str = None, poster_url: str = None, year: int = None, genre: str = None):
    """Update movie metadata in database"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return False

        cur = conn.cursor()
        
        updates = []
        values = []
        
        if imdb_id:
            updates.append("imdb_id = %s")
            values.append(imdb_id)
        if poster_url:
            updates.append("poster_url = %s")
            values.append(poster_url)
        if year:
            updates.append("year = %s")
            values.append(year)
        if genre:
            updates.append("genre = %s")
            values.append(genre)
        
        if not updates:
            return False
        
        values.append(movie_id)
        query = f"UPDATE movies SET {', '.join(updates)} WHERE id = %s"
        
        cur.execute(query, values)
        conn.commit()
        cur.close()
        close_db_connection(conn)
        
        logger.info(f"✅ Updated metadata for movie ID: {movie_id}")
        return True

    except Exception as e:
        logger.error(f"Error updating movie metadata: {e}")
        return False
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass


def store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
    """Store user request in database"""
    try:
        conn = get_db_connection()
        if not conn:
            return False

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT user_requests_unique_constraint DO UPDATE
                SET requested_at = EXCLUDED.requested_at
        """, (user_id, username, first_name, movie_title, group_id, message_id))
        conn.commit()
        cur.close()
        close_db_connection(conn)
        return True
    except Exception as e:
        logger.error(f"Error storing user request: {e}")
        try:
            conn.rollback()
            close_db_connection(conn)
        except:
            pass
        return False


# ==================== METADATA FUNCTIONS ====================

def is_valid_imdb_id(imdb_id: str) -> bool:
    """Validate IMDb ID format (tt1234567 or tt12345678)"""
    if not imdb_id:
        return False
    return bool(re.match(r'^tt\d{7,12}$', imdb_id.strip()))

def auto_fetch_and_update_metadata(movie_id: int, movie_title: str):
    """Automatically fetch and update metadata for a movie"""
    try:
        metadata = fetch_movie_metadata(movie_title)
        if metadata:
            title, year, poster_url, genre, imdb_id, rating = metadata  # 6 values unpack
            update_movie_metadata(
                movie_id=movie_id,
                imdb_id=imdb_id if imdb_id else None,
                poster_url=poster_url if poster_url else None,
                year=year if year else None,
                genre=genre if genre else None,
                rating=rating if rating and rating != 'N/A' else None  # Rating add करें
            )
            return True
        return False
    except Exception as e:
        logger.error(f"Error in auto_fetch_and_update_metadata: {e}")
        return False

# ==================== NEW METADATA HELPER FUNCTIONS ====================

def fetch_movie_metadata(query: str):
    """
    Smart Metadata Fetcher:
    - Fetches Plot (Description) & Poster
    - Decides Category based on Country/Language
    Returns: (title, year, poster, genre, imdb_id, rating, plot, category)
    """
    if not ia: return query, 0, '', '', '', 'N/A', '', 'Unknown'

    # --- Helper: Region Decide Karne Ka Logic ---
    def get_smart_category(country, language, genre):
        c = str(country).lower()
        l = str(language).lower()
        g = str(genre).lower()

        if 'japan' in c or 'animation' in g: return "Anime"
        if 'series' in g or 'episode' in g: return "Series"
        if 'india' not in c: return "Hollywood"
        
        # India Logic
        if any(x in l for x in ['telugu', 'tamil', 'kannada', 'malayalam', 'marathi']):
            return "South"
        
        return "Bollywood" # Default Hindi/India

    try:
        # 📝 1. OMDb Strategy (First Priority)
        omdb_api_key = os.environ.get("OMDB_API_KEY")
        if omdb_api_key:
            try:
                url = f"https://www.omdbapi.com/?t={quote(query)}&apikey={omdb_api_key}"
                if re.match(r'^tt\d{7,12}$', query.strip()): # IMDb ID support
                    url = f"https://www.omdbapi.com/?i={query.strip()}&apikey={omdb_api_key}"

                response = requests.get(url, timeout=10)
                data = response.json()
                
                if data.get("Response") == "True":
                    # Data Extraction
                    title = data['Title']
                    year = int(data.get('Year', 0).split('–')[0]) if data.get('Year') else 0
                    poster = data.get('Poster', '') if data.get('Poster') != 'N/A' else ''
                    genre = data.get('Genre', '')
                    imdb_id = data.get('imdbID', '')
                    rating = data.get('imdbRating', 'N/A')
                    plot = data.get('Plot', '') # Description
                    
                    # Smart Category Decision
                    category = get_smart_category(data.get('Country', ''), data.get('Language', ''), genre)
                    
                    return title, year, poster, genre, imdb_id, rating, plot, category
            except Exception as e:
                logger.warning(f"OMDb Error: {e}")

        # 🔍 2. Cinemagoer Strategy (Fallback)
        if re.match(r'^tt\d{7,12}$', query.strip()):
            movie = ia.get_movie(query.strip()[2:])
        else:
            movies = ia.search_movie(query)
            if not movies: return None
            movie = movies[0]
            ia.update(movie)

        title = movie.get('title', 'Unknown')
        year = movie.get('year', 0)
        poster = movie.get('full-size cover url', '')
        genres = ', '.join(movie.get('genres', [])[:3])
        imdb_id = f"tt{movie.movieID}"
        rating = movie.get('rating', 'N/A')
        # Description Fix
        plot = movie.get('plot outline') or (movie.get('plot')[0] if movie.get('plot') else '')
        
        # Smart Category
        countries = movie.get('countries', [])
        languages = movie.get('languages', [])
        category = get_smart_category(countries, languages, genres)

        return title, year, poster, genres, imdb_id, rating, plot, category

    except Exception as e:
        logger.error(f"Metadata Error: {e}")
        return None
# ==================== AI INTENT ANALYSIS ====================
# 👇👇👇 START COPY HERE 👇👇👇
async def analyze_intent(message_text):
    """
    Bina AI (Gemini) ke message analyze karna.
    Isse API limit waste nahi hogi!
    """
    try:
        text_lower = message_text.lower().strip()
        
        # 1. Agar message bahut lamba hai ya usme Link hai, toh reject kar do
        if len(text_lower) > 60 or "http" in text_lower or "t.me" in text_lower:
            return {"is_request": False, "content_title": None}

        # 2. Agar chota message hai, toh usko direct Movie ka naam maan lo
        # Faltu words hatane ki koshish (Optional)
        words_to_remove = ["please", "plz", "bhai", "movie", "series", "chahiye", "give", "me"]
        clean_name = text_lower
        for word in words_to_remove:
            clean_name = clean_name.replace(word, "").strip()

        if len(clean_name) < 2:
            return {"is_request": False, "content_title": None}

        return {"is_request": True, "content_title": message_text.strip()}

    except Exception as e:
        logger.error(f"Error in intent analysis: {e}")
        return {"is_request": True, "content_title": message_text.strip()}
# 👆👆👆 END COPY HERE 👆👆👆

# ==================== NOTIFICATION FUNCTIONS ====================
async def send_admin_notification(context, user, movie_title, group_info=None):
    """Send notification to admin channel about a new request"""
    if not REQUEST_CHANNEL_ID:
        return

    try:
        # ESCAPE the movie title and username BEFORE putting it into the message string
        safe_movie_title = movie_title.replace('<', '&lt;').replace('>', '&gt;')
        safe_username = user.username if user.username else 'N/A'
        safe_first_name = (user.first_name or 'Unknown').replace('<', '&lt;').replace('>', '&gt;')

        user_info = f"User: {safe_first_name}"
        if user.username:
            user_info += f" (@{safe_username})"
        user_info += f" (ID: {user.id})"

        group_info_text = f"From Group: {group_info}" if group_info else "Via Private Message"

        message = f"""
🎬 New Movie Request! 🎬

Movie: <b>{safe_movie_title}</b>
{user_info}
{group_info_text}
Time: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}
"""

        await context.bot.send_message(
            chat_id=REQUEST_CHANNEL_ID, 
            text=message, 
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error sending admin notification: {e}")

async def notify_users_for_movie(context: ContextTypes.DEFAULT_TYPE, movie_title, movie_url_or_file_id):
    """Notify users who requested a movie"""
    logger.info(f"Attempting to notify users for movie: {movie_title}")
    conn = None
    cur = None
    notified_count = 0

    caption_text = (
    f"🎬 <b>{movie_title}</b>\n\n"
    "➖➖➖➖➖➖➖➖➖➖\n"
    "🔹 <b>Please drop the movie name, and I'll find it for you as soon as possible. 🎬✨👇</b>\n"
    "➖➖➖➖➖➖➖➖➖➖\n"
    "🔹 <b>Support group:</b> https://t.me/+2hFeRL4DYfBjZDQ1\n"
)
    join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

    try:
        conn = get_db_connection()
        if not conn:
            return 0

        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, username, first_name FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
            (f'%{movie_title}%',)
        )
        users_to_notify = cur.fetchall()

        for user_id, username, first_name in users_to_notify:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"🎉 Hey {first_name or username}! Your requested movie '{movie_title}' is now available!"
                )
            except Exception as e:
                # This ensures if the notification fails, the code continues
                print(f"Failed to send movie notification: {e}") 

            try:
                warning_msg = await context.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=-1002683355160,
                    message_id=1773
                )
            except Exception:
                warning_msg = None
                
                if isinstance(movie_url_or_file_id, str) and any(movie_url_or_file_id.startswith(prefix) for prefix in ["BQAC", "BAAC", "CAAC", "AQAC"]):
                    sent_msg = await context.bot.send_document(
                        chat_id=user_id,
                        document=movie_url_or_file_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )
                elif isinstance(movie_url_or_file_id, str) and movie_url_or_file_id.startswith("https://t.me/c/"):
                    parts = movie_url_or_file_id.split('/')
                    from_chat_id = int("-100" + parts[-2])
                    msg_id = int(parts[-1])
                    sent_msg = await context.bot.copy_message(
                        chat_id=user_id,
                        from_chat_id=from_chat_id,
                        message_id=msg_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )
                elif isinstance(movie_url_or_file_id, str) and movie_url_or_file_id.startswith("http"):
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"🎬 {movie_title} is now available!\n\n{caption_text}",
                        reply_markup=get_movie_options_keyboard(movie_title, movie_url_or_file_id),
                        parse_mode='HTML'
                    )
                else:
                    sent_msg = await context.bot.send_document(
                        chat_id=user_id,
                        document=movie_url_or_file_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )

                if sent_msg:
                    asyncio.create_task(
                        delete_messages_after_delay(
                            context,
                            user_id,
                            [sent_msg.message_id, warning_msg.message_id],
                            60
                        )
                    )

                cur.execute(
                    "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                    (user_id, f'%{movie_title}%')
                )
                conn.commit()
                notified_count += 1
                await asyncio.sleep(0.1)

            except telegram.error.Forbidden:
                logger.error(f"User {user_id} blocked the bot")
                continue
            except Exception as e:
                logger.error(f"Error notifying user {user_id}: {e}")
                continue

        return notified_count
    except Exception as e:
        logger.error(f"Error in notify_users_for_movie: {e}")
        return 0
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)

async def notify_in_group(context: ContextTypes.DEFAULT_TYPE, movie_title):
    """Notify users in group when a requested movie becomes available"""
    logger.info(f"Attempting to notify users in group for movie: {movie_title}")
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        if not conn:
            return

        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, username, first_name, group_id, message_id FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
            (f'%{movie_title}%',)
        )
        users_to_notify = cur.fetchall()

        if not users_to_notify:
            return

        groups_to_notify = defaultdict(list)
        for user_id, username, first_name, group_id, message_id in users_to_notify:
            if group_id:
                groups_to_notify[group_id].append((user_id, username, first_name, message_id))

        for group_id, users in groups_to_notify.items():
            try:
                notification_text = "Hey! आपकी requested movie अब आ गई है! 🥳\n\n"
                notified_users_ids = []
                user_mentions = []
                for user_id, username, first_name, message_id in users:
                    mention = f"[{first_name or username}](tg://user?id={user_id})"
                    user_mentions.append(mention)
                    notified_users_ids.append(user_id)

                notification_text += ", ".join(user_mentions)
                notification_text += f"\n\nआपकी फिल्म '{movie_title}' अब उपलब्ध है! इसे पाने के लिए, कृपया मुझे private [...]"

                await context.bot.send_message(
                    chat_id=group_id,
                    text=notification_text,
                    parse_mode='Markdown'
                )

                for user_id in notified_users_ids:
                    cur.execute(
                        "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                        (user_id, f'%{movie_title}%')
                    )
                conn.commit()

            except Exception as e:
                logger.error(f"Failed to send message to group {group_id}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in notify_in_group: {e}")
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)

# ==================== NEW GENRE FUNCTIONS ====================

def get_all_genres_from_db():
    """Fetch all unique genres from database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT genre FROM movies WHERE genre IS NOT NULL AND genre != ''")
        results = cur.fetchall()
        
        # Parse comma-separated genres and flatten
        all_genres = []
        for row in results:
            genre_str = row[0]
            if genre_str:
                # Split by comma and strip spaces
                genres = [g.strip() for g in genre_str.split(',')]
                all_genres.extend(genres)
        
        # Remove duplicates and return sorted list
        unique_genres = sorted(set(all_genres))
        cur.close()
        close_db_connection(conn)
        return unique_genres
        
    except Exception as e:
        logger.error(f"Error fetching genres: {e}")
        return []
    finally:
        if conn:
            close_db_connection(conn)


def create_genre_selection_keyboard():
    """Create inline keyboard with genre selection buttons"""
    genres = get_all_genres_from_db()
    
    if not genres:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ No Genres Found", callback_data="cancel_genre")]])
    
    keyboard = []
    row = []
    
    for idx, genre in enumerate(genres):
        row.append(InlineKeyboardButton(
            f"📂 {genre}",
            callback_data=f"genre_{genre}"
        ))
        
        # 2 buttons per row
        if (idx + 1) % 2 == 0:
            keyboard.append(row)
            row = []
    
    # Add remaining buttons
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_genre")])
    return InlineKeyboardMarkup(keyboard)


def get_movies_by_genre(genre: str, limit: int = 10):
    """Fetch movies filtered by genre"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        # Use ILIKE for case-insensitive search within genre string
        cur.execute("""
            SELECT id, title, url, file_id, poster_url, year 
            FROM movies 
            WHERE genre ILIKE %s
            ORDER BY year DESC NULLS LAST
            LIMIT %s
        """, (f'%{genre}%', limit))
        
        results = cur.fetchall()
        cur.close()
        close_db_connection(conn)
        return results
        
    except Exception as e:
        logger.error(f"Error fetching movies by genre: {e}")
        return []
    finally:
        if conn:
            close_db_connection(conn)


async def show_genre_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle 'Browse by Genre' button click"""
    if update.message:
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # FSub check
        check = await is_user_member(context, user_id)
        if not check['is_member']:
            msg = await update.message.reply_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            track_message_for_deletion(context, chat_id, msg.message_id, 120)
            return
        
        # Show genre selection
        keyboard = create_genre_selection_keyboard()
        msg = await update.message.reply_text(
            "📂 **Select a genre to browse movies:**",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        track_message_for_deletion(context, chat_id, msg.message_id, 180)


async def handle_genre_selection(update: Update, context:  ContextTypes.DEFAULT_TYPE):
    """Handle genre selection callback"""
    query = update.callback_query
    await query.answer()
    
    data = query. data
    
    if data == "cancel_genre":
        await query.edit_message_text("❌ Genre browsing cancelled.")
        return
    
    if data.startswith("genre_"):
        genre = data.replace("genre_", "")
        
        # Fetch movies for this genre
        movies = get_movies_by_genre(genre, limit=15)
        
        if not movies:
            await query.edit_message_text(
                f"😕 No movies found for genre: **{genre}**\n\n"
                "Try another genre or use 🔍 Search.",
                parse_mode='Markdown'
            )
            return
        
        # Create movie selection keyboard
        context.user_data['search_results'] = movies
        context.user_data['search_query'] = genre
        
        keyboard = create_movie_selection_keyboard(movies, page=0)  # ✅ Now handles 6-tuple
        
        await query.edit_message_text(
            f"🎬 **Found {len(movies)} movies in '{genre}' genre**\n\n"
            "👇 Select a movie:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
# ==================== KEYBOARD MARKUPS ====================
def get_main_keyboard():
    """Get the main menu keyboard - UPDATED with Genre"""
    keyboard = [
        ['🔍 Search Movies'],
        ['📂 Browse by Genre', '🙋 Request Movie'],
        ['📊 My Stats', '❓ Help']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_admin_request_keyboard(user_id, movie_title):
    """Inline keyboard for admin actions on a user request"""
    sanitized_title = movie_title[:30]

    keyboard = [
        [InlineKeyboardButton("✅ FULFILL MOVIE", callback_data=f"admin_fulfill_{user_id}_{sanitized_title}")],
        [InlineKeyboardButton("❌ IGNORE/DELETE", callback_data=f"admin_delete_{user_id}_{sanitized_title}")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_movie_options_keyboard(movie_title, url, movie_id, file_info):
    # 'scan_info_' ke saath movie_id bhejo taaki data fetch kar sakein
    keyboard = [
        [InlineKeyboardButton("ℹ️ SCAN INFO : AUDIO & SUBS", callback_data=f"scan_{movie_id}")],
        [InlineKeyboardButton("🎬 Watch Now", url=url)],
        [InlineKeyboardButton("📥 Download", callback_data=f"download_{movie_title[:50]}")],
        [InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_movie_selection_keyboard(movies, page=0, movies_per_page=5):
    """Create inline keyboard with movie selection buttons - FIXED for all sources"""
    start_idx = page * movies_per_page
    end_idx = start_idx + movies_per_page
    current_movies = movies[start_idx:end_idx]

    keyboard = []

    for movie in current_movies:
        # ✅ FIXED: Handle both 4-tuple and 6-tuple returns
        if len(movie) >= 6:
            # From get_movies_by_genre (6 values:  id, title, url, file_id, poster_url, year)
            movie_id, title, url, file_id, poster_url, year = movie[: 6]
        elif len(movie) >= 8:
            # From get_movies_from_db (8 values: id, title, url, file_id, imdb_id, poster_url, year, genre)
            movie_id, title, url, file_id, imdb_id, poster_url, year, genre = movie[:8]
        else:
            # Fallback for 4-tuple (id, title, url, file_id)
            movie_id, title = movie[0], movie[1]
        
        button_text = title if len(title) <= 40 else title[:37] + "..."
        keyboard.append([InlineKeyboardButton(
            f"🎬 {button_text}",
            callback_data=f"movie_{movie_id}"
        )])

    nav_buttons = []
    total_pages = (len(movies) + movies_per_page - 1) // movies_per_page

    if page > 0:
        nav_buttons. append(InlineKeyboardButton("◀️ Previous", callback_data=f"page_{page-1}"))

    if end_idx < len(movies):
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{page+1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)

def get_all_movie_qualities(movie_id):
    """Fetch all available qualities and their SIZES for a given movie ID"""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        cur = conn.cursor()
        # Update: Added file_size to the SELECT statement
        cur.execute("""
            SELECT quality, url, file_id, file_size
            FROM movie_files
            WHERE movie_id = %s AND (url IS NOT NULL OR file_id IS NOT NULL)
            ORDER BY CASE quality
                WHEN '4K' THEN 1
                WHEN 'HD Quality' THEN 2
                WHEN 'Standart Quality'  THEN 3
                WHEN 'Low Quality'  THEN 4
                ELSE 5
            END DESC
        """, (movie_id,))
        results = cur.fetchall()
        cur.close()
        return results
    except Exception as e:
        logger.error(f"Error fetching movie qualities for {movie_id}: {e}")
        return []
    finally:
        if conn:
            close_db_connection(conn)

# create_quality_selection_keyboard function ko isse replace karein ya modify karein:

def create_quality_selection_keyboard(movie_id, title, qualities, page=0):
    """
    Create inline keyboard with quality selection.
    Features: 
    1. Grid Layout (2 buttons per row)
    2. 'Send All' button at top
    3. Pagination
    """
    limit = 6 # Grid view ke liye limit badha di (even number best hai)
    start_idx = page * limit
    end_idx = start_idx + limit
    
    current_qualities = qualities[start_idx:end_idx]
    keyboard = []

    # --- 1. SEND ALL BUTTON (Top) ---
    keyboard.append([InlineKeyboardButton("🚀 SEND ALL FILES", callback_data=f"sendall_{movie_id}")])

    # --- 2. GRID LAYOUT (2 Buttons per Row) ---
    row = []
    for quality, url, file_id, file_size in current_qualities:
        callback_data = f"quality_{movie_id}_{quality}"
        
        # Button Text Formatting (Compact)
        # Example: "📁 720p [1.2GB]"
        size_str = f"[{file_size}]" if file_size else ""
        icon = "📁" if file_id else "🔗"
        
        button_text = f"{icon} {quality} {size_str}"
        
        row.append(InlineKeyboardButton(button_text, callback_data=callback_data))

        # Agar row mein 2 button ho gaye, to keyboard mein daal do
        if len(row) == 2:
            keyboard.append(row)
            row = []

    # Agar koi akela button bacha hai (odd number), to use bhi add karo
    if row:
        keyboard.append(row)

    # --- 3. PAGINATION BUTTONS ---
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Back", callback_data=f"qualpage_{movie_id}_{page-1}"))
    
    if end_idx < len(qualities):
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"qualpage_{movie_id}_{page+1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    # Cancel Button
    keyboard.append([InlineKeyboardButton("❌ Cancel Selection", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)

# ==================== HELPER FUNCTION ====================
async def send_movie_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE, movie_id: int, title: str, url: Optional[str] = None, file_id: Optional[str] = None, send_warning: bool = True):
    """Sends the movie file/link to the user with THUMBNAIL PROTECTION - UPDATED"""
    chat_id = update.effective_chat.id

    # --- 1. Fetch movie details including genre & year ---
    conn = get_db_connection()
    genre = ""
    year = ""
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT genre, year FROM movies WHERE id = %s", (movie_id,))
            result = cur.fetchone()
            if result:
                db_genre, db_year = result
                if db_genre:
                    genre = f"🎭 <b>Genre:</b> {db_genre}\n"
                if db_year and db_year > 0:
                    year = f"📅 <b>Year:</b> {db_year}\n"
            cur.close()
        except Exception as e:
            logger.error(f"Error fetching genre: {e}")
        finally:
            close_db_connection(conn)
    # ---------------------------------------------------

    # --- 2. Language Detection from FILES (Database) ---
    all_qualities = get_all_movie_qualities(movie_id)
    
    combined_file_text = " ".join([q[0].lower() for q in all_qualities])
    
    langs = []
    if "hind" in combined_file_text: langs.append("Hindi")
    if "eng" in combined_file_text: langs.append("English")
    if "tam" in combined_file_text: langs.append("Tamil")
    if "tel" in combined_file_text: langs.append("Telugu")
    if "kan" in combined_file_text: langs.append("Kannada")
    if "mal" in combined_file_text: langs.append("Malayalam")
    if "dual" in combined_file_text: langs.append("Dual Audio")
    if "multi" in combined_file_text: langs.append("Multi Audio")
    
    lang_display = ""
    if langs:
        lang_display = f"🔊 <b>Language:</b> {', '.join(sorted(set(langs)))}\n"
    # ---------------------------------------------------

    # 1. Multi-Quality Check (Agar direct link/file nahi hai)
    if not url and not file_id:
        if all_qualities:
            context.user_data['selected_movie_data'] = {'id': movie_id, 'title': title, 'qualities': all_qualities}
            
            selection_text = f"✅ We found **{title}** in multiple qualities.\n\n⬇️ **Please choose the file quality:**"
            keyboard = create_quality_selection_keyboard(movie_id, title, all_qualities)
            
            msg = await context.bot.send_message(chat_id=chat_id, text=selection_text, reply_markup=keyboard, parse_mode='Markdown')
            track_message_for_deletion(context, chat_id, msg.message_id, 60)
            return

    try:
        # =========================================================
        # 🔥 MODIFIED: REPLACED TEXT WARNING WITH FILE FORWARD 🔥
        # =========================================================
        warning_msg = None
        if send_warning:
            try:
                # Sirf File Copy Karega (Text Message Nahi)
                warning_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=-1002683355160,  # Channel ID
                    message_id=1773              # File Message ID
                )
            except Exception as e:
                logger.error(f"Warning file send failed: {e}")
        
        # --- CAPTION UPDATE WITH GENRE, YEAR & LANGUAGE ---
        caption_text = (
            f"🎬 <b>{title}</b>\n"
            f"{year}"        # Year added
            f"{genre}"       # Genre added
            f"{lang_display}"  # Language
            f"\n🔗 <b>JOIN »</b> <a href='{FILMFYBOX_CHANNEL_URL}'>FilmfyBox</a>\n\n"
            f"🔹 <b>Please drop the movie name, and I'll find it for you as soon as possible. 🎬✨👇</b>\n"
            f"🔹 <b><a href='https://t.me/+2hFeRL4DYfBjZDQ1'>FlimfyBox Chat</a></b>"
        )
        # ---------------------------------------------------
        
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url=FILMFYBOX_CHANNEL_URL)]])


        # ==================================================================
        # 🚀 PRIORITY 1: TRY COPYING FROM CHANNEL LINK (Best for Thumbnails)
        # ==================================================================
        if url and ("t.me/c/" in url or "t.me/" in url) and "http" in url:
            try:
                clean_url = url.strip()
                parts = clean_url.rstrip('/').split('/')
                msg_id = int(parts[-1])
                
                if "t.me/c/" in clean_url:
                    from_chat_id = int("-100" + parts[-2])
                else:
                    from_chat_id = f"@{parts[-2]}"

                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=msg_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_keyboard
                )
            except Exception as e:
                logger.error(f"Copy link failed: {e}")

        # ==================================================================
        # ⚠️ PRIORITY 2: TRY SENDING BY FILE ID (Fallback)
        # ==================================================================
        if not sent_msg and file_id:
            clean_file_id = str(file_id).strip()
            try:
                sent_msg = await context.bot.send_video(
                    chat_id=chat_id,
                    video=clean_file_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_keyboard
                )
            except telegram.error.BadRequest:
                try:
                    sent_msg = await context.bot.send_document(
                        chat_id=chat_id,
                        document=clean_file_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )
                except Exception as e:
                    logger.error(f"Send Document failed: {e}")

        # ==================================================================
        # 🌐 PRIORITY 3: EXTERNAL LINK (If everything else fails)
        # ==================================================================
        if not sent_msg and url and "http" in url and "t.me" not in url:
             sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 <b>{title}</b>\n\n🔗 <b>Watch/Download:</b> {url}",
                parse_mode='HTML',
                reply_markup=join_keyboard
            )

        # Final Cleanup (Auto Delete Logic)
        messages_to_delete = []
        if sent_msg:
            messages_to_delete.append(sent_msg.message_id)
        if warning_msg:
            messages_to_delete.append(warning_msg.message_id)

        if messages_to_delete:
            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    chat_id,
                    messages_to_delete,
                    60
                )
            )
        elif not sent_msg:
            await context.bot.send_message(chat_id=chat_id, text="❌ Error: File not found or Bot needs Admin rights in Source Channel.")

    except Exception as e:
        logger.error(f"Critical Error in send_movie: {e}")
        try: await context.bot.send_message(chat_id=chat_id, text="❌ System Error.")
        except: pass

# ==================== TELEGRAM BOT HANDLERS ====================
# ============================================================================
# NEW BACKGROUND SEARCH & START LOGIC
# ============================================================================

async def background_search_and_send(update: Update, context: ContextTypes.DEFAULT_TYPE, query_text: str, status_msg):
    """
    Runs database search in background to prevent blocking the bot.
    """
    chat_id = update.effective_chat.id
    try:
        # 1. PEHLE EXACT MATCH CHECK KAREIN (Ye FAST hai - 0.1 sec)
        # This saves resources if the user clicked a precise link
        conn = get_db_connection()
        exact_movie = None
        if conn:
            try:
                cur = conn.cursor()
                # Use ILIKE for case-insensitive exact match
                cur.execute("SELECT id, title, url, file_id FROM movies WHERE title ILIKE %s LIMIT 1", (query_text.strip(),))
                exact_movie = cur.fetchone()
            except Exception as db_e:
                logger.error(f"Database error in exact match: {db_e}")
            finally:
                if conn: close_db_connection(conn)

        movies_found = []
        if exact_movie:
            movies_found = [exact_movie] # Exact match found, skip fuzzy search
        else:
            # Agar exact nahi mila to hi Fuzzy Search karein (Slower process)
            # Assuming get_movies_from_db is your existing function
            movies_found = await run_async(get_movies_from_db, query_text, limit=1)

        # 2. Result Handle karein
        if not movies_found:
            # Delete loading msg
            try: await status_msg.delete() 
            except: pass
            
            # Create request button
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🙋 Request This Movie", callback_data=f"request_{query_text[:40]}")]
            ])
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"😕 Sorry, '{query_text}' not found.\nWould you like to request it?",
                reply_markup=keyboard
            )
            return

        # 3. Movie Mil gayi - Send karein
        movie_id, title, url, file_id = movies_found[0]
        
        # Loading msg delete karein
        try: await status_msg.delete() 
        except: pass

        # Send the movie using your existing helper function
        await send_movie_to_user(update, context, movie_id, title, url, file_id)

    except Exception as e:
        logger.error(f"Background Search Error: {e}")
        try: 
            await status_msg.edit_text("❌ Error fetching movie. Please try again.")
        except: 
            pass

# ==================== CLEAN LOADING FUNCTION (FIXED) ====================
async def deliver_movie_on_start(update: Update, context: ContextTypes.DEFAULT_TYPE, movie_id: int):
    """
    Fetches and sends a movie with a clean 'Loading' animation.
    No technical details shown to the user.
    """
    chat_id = update.effective_chat.id
    
    # 1. Loading Effect
    status_msg = None
    try:
        status_msg = await context.bot.send_message(chat_id, "⏳ <b>Please wait...</b>", parse_mode='HTML')
        
        # Backup Auto-delete
        track_message_for_deletion(context, chat_id, status_msg.message_id, 60)
    except:
        pass

    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            # User ko technical error mat dikhao, bas chupchap delete kar do
            if status_msg: 
                try: 
                    await status_msg.delete() 
                except: 
                    pass
            return

        cur = conn.cursor()
        cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
        movie_data = cur.fetchone()
        cur.close()
        close_db_connection(conn)

        # 2. Movie milne ke baad turant Loading Msg delete karo
        if status_msg:
            try: 
                await status_msg.delete()
            except: 
                pass

        if movie_data:
            title, url, file_id = movie_data
            # Movie bhejo
            await send_movie_to_user(update, context, movie_id, title, url, file_id)
        else:
            # Agar movie nahi mili
            fail_msg = await context.bot.send_message(chat_id, "❌ <b>Movie not found or deleted.</b>", parse_mode='HTML')
            track_message_for_deletion(context, chat_id, fail_msg.message_id, 10)

    except Exception as e:
        logger.error(f"Error in deliver_movie: {e}")
        if status_msg:
            try: 
                await status_msg.delete()
            except: 
                pass
        if movie_data:
            title, url, file_id = movie_data
            await send_movie_to_user(update, context, movie_id, title, url, file_id)
        else:
            await context.bot.send_message(
                chat_id=chat_id, 
                text="❌ Movie not found. It may have been removed from our database."
            )

    except Exception as e:
        logger.error(f"CRITICAL ERROR in deliver_movie: {e}", exc_info=True)
        error_msg = "❌ Failed to retrieve movie. Please try again or use search."
        if status_msg:
            try:
                await status_msg.edit_text(error_msg)
            except:
                pass
        else:
            await context.bot.send_message(chat_id=chat_id, text=error_msg)
            
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass

# Add this at the top level
from asyncio import Lock
from collections import defaultdict

user_processing_locks = defaultdict(Lock)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # ✅ FIX 1: Message ko safe tarike se nikalein (Button aur Text dono ke liye)
    message = update.effective_message 

    # === FSub Check (Smart Logic) ===
    force_check = True if context.args else False
    
    check = await is_user_member(context, user_id, force_fresh=force_check)
    
    if not check['is_member']:
        # Agar deep link (args) hain to unhe save kar lo
        if context.args:
            context.user_data['pending_start_args'] = context.args

        # ✅ FIX 2: send_message use karein (reply_text fail ho sakta hai button par)
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=get_join_message(check['channel'], check['group']),
            reply_markup=get_join_keyboard(),
            parse_mode='Markdown'
        )
        track_message_for_deletion(context, chat_id, msg.message_id, 120)
        return
    # ==================

    logger.info(f"START called by user {user_id} with args: {context.args}")

    # Purani states clear karein
    context.user_data.clear()
    if hasattr(context, 'conversation') and context.conversation:
        context.conversation = None

    # === DEEP LINK PROCESSING ===
    if context.args and len(context.args) > 0:
        payload = context.args[0]
        
        # Check lock (taaki user spam na kare)
        if user_processing_locks[user_id].locked():
            await context.bot.send_message(
                chat_id=chat_id, 
                text="⏳ Please wait! Your previous request is still processing..."
            )
            return

        async with user_processing_locks[user_id]:
            
            # --- CASE 1: DIRECT MOVIE ID (movie_123) ---
            if payload.startswith("movie_"):
                try:
                    movie_id = int(payload.split('_')[1])
                    
                    # ✅ FIX 3: send_message use karein
                    status_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🎬 Deep link detected!\nMovie ID: {movie_id}\nFetching... Please wait ⏳"
                    )
                    
                    try:
                        await deliver_movie_on_start(update, context, movie_id)
                        
                        # Success hone par status msg delete karein
                        try: await status_msg.delete() 
                        except: pass
                        
                        logger.info(f"✅ Deep link SUCCESS for user {user_id}, movie {movie_id}")
                        
                    except Exception as e:
                        logger.error(f"❌ Deep link FAILED: {e}")
                        await status_msg.edit_text(f"❌ Error fetching movie: {e}")
                    
                    return # Movie mil gayi, Welcome msg mat dikhao

                except Exception as e:
                    logger.error(f"Invalid movie link: {e}")
                    await context.bot.send_message(chat_id=chat_id, text="❌ Invalid Link Format")
                    return

            # --- CASE 2: AUTO SEARCH (q_kalki) ---
            # ✅ RESTORED: Ye logic maine wapas add kar di hai
            elif payload.startswith("q_"):
                try:
                    query_text = payload[2:].replace("_", " ").strip()
                    
                    # ✅ FIX 4: send_message use karein
                    status_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🔎 Deep link search detected!\nQuery: '{query_text}'\nSearching... Please wait ⏳"
                    )
                    
                    try:
                        # Background search function call karein
                        await background_search_and_send(update, context, query_text, status_msg)
                        logger.info(f"✅ Deep link SEARCH SUCCESS for user {user_id}, query: {query_text}")
                        
                    except Exception as e:
                        logger.error(f"❌ Deep link SEARCH FAILED: {e}")
                        error_text = f"❌ Search failed for '{query_text}'.\nTry searching manually."
                        try: await status_msg.edit_text(error_text)
                        except: await context.bot.send_message(chat_id=chat_id, text=error_text)
                    
                    return # Search ho gaya, Welcome msg mat dikhao
                    
                except Exception as e:
                    logger.error(f"Deep link search error: {e}")
                    await context.bot.send_message(chat_id=chat_id, text="❌ Error processing search link.")
                    return

    # --- NORMAL WELCOME MESSAGE ---
    welcome_text = """
📨 Send Movie Or Series Name And Year As Per Google Spelling..!! 👍

🎬 <b>FlimfyBox Bot</b> is ready to serve you!

👇 Use the buttons below to get started:
"""
    # ✅ FIX 5: Final Welcome Msg bhi safe tarike se bhejen
    msg = await context.bot.send_message(
        chat_id=chat_id, 
        text=welcome_text, 
        reply_markup=get_main_keyboard(), 
        parse_mode='HTML'
    )
    track_message_for_deletion(context, chat_id, msg.message_id, delay=300)
    return
    
    # ✅ Just return (No state needed for main menu)
    return
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu options"""
    try:
        query = update.message.text

        if query == '🔍 Search Movies':
            msg = await update.message.reply_text("Great! Tell me the name of the movie you want to search for.")
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
            return SEARCHING

        elif query == '🙋 Request Movie':
            msg = await update.message.reply_text("Okay, you've chosen to request a new movie. Please tell me the name of the movie you want me to add.")
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
            return REQUESTING

        elif query == '📊 My Stats':
            user_id = update.effective_user.id
            conn = None
            try:
                conn = get_db_connection()
                if conn:
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s", (user_id,))
                    request_count = cur.fetchone()

                    cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s AND notified = TRUE", (user_id,))
                    fulfilled_count = cur.fetchone()

                    stats_text = f"""
📊 Your Stats:
- Total Requests: {request_count}
- Fulfilled Requests: {fulfilled_count}
"""
                    msg = await update.message.reply_text(stats_text)
                    track_message_for_deletion(update.effective_chat.id, msg.message_id, 180)
                else:
                    await update.message.reply_text("Sorry, database connection failed.")
            except Exception as e:
                logger.error(f"Error getting stats: {e}")
                await update.message.reply_text("Sorry, couldn't retrieve your stats at the moment.")
            finally:
                if conn: close_db_connection(conn)

            return MAIN_MENU

        elif query == '❓ Help':
            help_text = """
🤖 How to use FlimfyBox Bot:

🔍 Search Movies: Find movies in our collection
🙋 Request Movie: Request a new movie to be added
📊 My Stats: View your request statistics

Just use the buttons below to navigate!
            """
            msg = await update.message.reply_text(help_text)
            track_message_for_deletion(update.effective_chat.id, msg.message_id, 180)
            return MAIN_MENU
        else:
            return await search_movies(update, context)

    except Exception as e:
        logger.error(f"Error in main menu: {e}")
        return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search for movies in the database"""
    try:
        # Agar ye button click se aya hai (cancel/back)
        if update.callback_query:
            query = update.callback_query
            await query.answer()
            # Yahan hum kuch return nahi kar rahe, bas message bhej rahe hain
            return

        # Agar message text nahi hai
        if not update.message or not update.message.text:
            return 

        query = update.message.text.strip()
        
        # Safety check
        if query in ['🔍 Search Movies', '📊 My Stats', '❓ Help']:
             return await main_menu_or_search(update, context)

        # 1. Search DB
        movies = await run_async(get_movies_from_db, query, limit=10)
        
        # 2. Not Found
        if not movies:
            if SEARCH_ERROR_GIFS:
                try:
                    gif = random.choice(SEARCH_ERROR_GIFS)
                    msg_gif = await update.message.reply_animation(animation=gif)
                    track_message_for_deletion(context, update.effective_chat.id, msg_gif.message_id, 60)
                except:
                    pass

            not_found_text = (
                "माफ़ करें, मुझे कोई मिलती-जुलती फ़िल्म नहीं मिली\n\n"
                "<b><a href='https://www.google.com/'>𝗚𝗼𝗼𝗴𝗹𝗲</a></b> ☜ सर्च करें..!!\n\n"
                "मूवी की स्पेलिंग गूगल पर सर्च करके, कॉपी करे, उसके बाद यहां टाइप करें।✔️\n\n"
                "बस मूवी का नाम + वर्ष:::: लिखें, उसके आगे पीछे कुछ भी ना लिखे..।♻️\n\n"
                "✐ᝰ𝗘𝘅𝗮𝗺𝗽𝗹𝗲\n\n"
                "सही है.!‼️    \n"
                "─────────────────────\n"
                "𝑲𝒈𝒇 𝟐✔️ | 𝑲𝒈𝒇 𝟐 𝑴𝒐𝒗𝒊𝒆 ❌\n"
                "─────────────────────\n"
                "𝑨𝒔𝒖𝒓 𝑺𝟎𝟏 𝑬𝟎𝟑✔️ | 𝑨𝒔𝒖𝒓 𝑺𝒆𝒂𝒔𝒐𝒏𝟑❌\n"
                "─────────────────────\n\n"
                "अगर फिर भी न मिले तो नीचे Request करे."
            )

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🙋 Request This Movie", callback_data=f"request_{query[:20]}")]
            ])
            
            msg = await update.message.reply_text(
                text=not_found_text,
                reply_markup=keyboard,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            # Auto Delete Not Found Msg
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
            return # <--- YAHAN SE MAIN_MENU HATA DIYA HAI

        # 3. Found
        context.user_data['search_results'] = movies
        context.user_data['search_query'] = query

        keyboard = create_movie_selection_keyboard(movies, page=0)
        
        msg = await update.message.reply_text(
            f"🎬 **Found {len(movies)} results for '{query}'**\n\n"
            "👇 Select your movie below:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
        return # <--- YAHAN SE BHI MAIN_MENU HATA DIYA HAI

    except Exception as e:
        logger.error(f"Error in search_movies: {e}")
        # await update.message.reply_text("An error occurred during search.") <--- ERROR MSG HATA DIYA TAKI USER DISTURB NA HO
        return

async def request_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie requests with duplicate detection, fuzzy matching and cooldowns"""
    try:
        user_message = (update.message.text or "").strip()
        user = update.effective_user

        if not user_message:
            await update.message.reply_text("कृपया मूवी का नाम भेजें।")
            return REQUESTING

        burst = user_burst_count(user.id, window_seconds=60)
        if burst >= MAX_REQUESTS_PER_MINUTE:
            msg = await update.message.reply_text(
                "🛑 तुम बहुत जल्दी-जल्दी requests भेज रहे हो। कुछ देर रोकें (कुछ मिनट) और फिर कोशिश करें।\n"
                "बार‑बार भेजने से फ़ायदा नहीं होगा।"
            )
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
            return REQUESTING

        intent = await analyze_intent(user_message)
        if not intent["is_request"]:
            msg = await update.message.reply_text("यह एक मूवी/सीरीज़ का नाम नहीं लग रहा है। कृपया सही नाम भेजें।")
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
            return REQUESTING

        movie_title = intent["content_title"] or user_message

        similar = get_last_similar_request_for_user(user.id, movie_title, minutes_window=REQUEST_COOLDOWN_MINUTES)
        if similar:
            last_time = similar.get("requested_at")
            elapsed = datetime.now() - last_time
            minutes_passed = int(elapsed.total_seconds() / 60)
            minutes_left = max(0, REQUEST_COOLDOWN_MINUTES - minutes_passed)
            if minutes_left > 0:
                strict_text = (
                    "🛑 Ruk jao! Aapne ye request abhi bheji thi.\n\n"
                    "Baar‑baar request karne se movie jaldi nahi aayegi.\n\n"
                    f"Similar previous request: \"{similar.get('stored_title')}\" ({similar.get('score')}% match)\n"
                    f"Kripya {minutes_left} minute baad dobara koshish karein. 🙏"
                )
                msg = await update.message.reply_text(strict_text)
                track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
                return REQUESTING

        stored = store_user_request(
            user.id,
            user.username,
            user.first_name,
            movie_title,
            update.effective_chat.id if update.effective_chat.type != "private" else None,
            update.message.message_id
        )
        if not stored:
            logger.error("Failed to store user request in DB.")
            await update.message.reply_text("Sorry, आपका request store नहीं हो पाया। बाद में कोशिश करें।")
            return REQUESTING

        group_info = update.effective_chat.title if update.effective_chat.type != "private" else None
        await send_admin_notification(context, user, movie_title, group_info)

        msg = await update.message.reply_text(
            f"✅ Got it! Your request for '{movie_title}' has been sent. I'll let you know when it's available.",
            reply_markup=get_main_keyboard()
        )
        track_message_for_deletion(update.effective_chat.id, msg.message_id, 180)

        return MAIN_MENU

    except Exception as e:
        logger.error(f"Error in request_movie: {e}")
        await update.message.reply_text("Sorry, an error occurred while processing your request.")
        return REQUESTING

async def request_movie_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie request after user sends movie name following button click"""
    try:
        user_message = (update.message.text or "").strip()
        
        # Check for Main Menu Buttons (Emergency Exit)
        menu_buttons = ['🔍 Search Movies', '🙋 Request Movie', '📊 My Stats', '❓ Help', '/start']
        if user_message in menu_buttons:
            if 'awaiting_request' in context.user_data:
                del context.user_data['awaiting_request']
            if 'pending_request' in context.user_data:
                del context.user_data['pending_request']
            return await main_menu(update, context)

        if not user_message:
            await update.message.reply_text("कृपया मूवी का नाम भेजें।")
            return REQUESTING_FROM_BUTTON

        # Store movie name
        context.user_data['pending_request'] = user_message
        
        confirm_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📽️ Confirm 🎬", callback_data=f"confirm_request_{user_message[:40]}")]
        ])
        
        msg = await update.message.reply_text(
            f"✅ आपने '<b>{user_message}</b>' को रिक्वेस्ट करना चाहते हैं?\n\n"
            f"<b>💫 अब बस अपनी मूवी या वेब-सीरीज़ का मूल नाम भेजें और कन्फर्म बटन पर क्लिक करें!</b>\n\n"
            f"कृपया कन्फर्म बटन पर क्लिक करें 👇",
            reply_markup=confirm_keyboard,
            parse_mode='HTML'
        )
        track_message_for_deletion(update.effective_chat.id, msg.message_id, 180)
        
        return MAIN_MENU

    except Exception as e:
        logger.error(f"Error in request_movie_from_button: {e}")
        return MAIN_MENU

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat.id
    data = query.data

    # --- DRAFT SYSTEM HANDLERS ---
    if data == "edit_draft_name":
        await query.answer()
        await query.message.reply_text("✏️ **Send New Name:**\nAb agla message jo aap bhejenge wo movie ka naam mana jayega.")
        context.user_data['awaiting_draft_rename'] = True
        return

    elif data == "set_draft_imdb":
        await query.answer()
        await query.message.reply_text(
            "🔗 **Send IMDb ID:**\n"
            "Example: `tt15462578`\n\n"
            "Ab agla message IMDb ID mana jayega."
        )
        context.user_data['awaiting_draft_imdb'] = True
        return
    
    elif data == "clear_draft":
        if user_id in PENDING_DRAFTS:
            del PENDING_DRAFTS[user_id]
        await query.message.delete()
        await query.message.reply_text("🗑️ **Draft Cleared!** Start fresh.")
        return

    elif data == "process_draft":
        if user_id not in PENDING_DRAFTS:
            await query.answer("❌ Draft expire ho gaya!", show_alert=True)
            return

        # --- PROCESS START ---
        draft = PENDING_DRAFTS[user_id]
        movie_title = draft['suggested_name'] # यह "कचरा" नाम हो सकता है
        manual_id = draft.get('manual_imdb_id')
        files = draft['files']
        
        await query.edit_message_text(f"🚀 **Processing...**\n⏳ Generating Landscape Thumbnails & Uploading...")

        # 1. Fetch Metadata (Metadata Check)
        if manual_id:
            metadata = await run_async(fetch_movie_metadata, manual_id)
        else:
            metadata = await run_async(fetch_movie_metadata, movie_title)
        
        # Default Variables
        poster_url = None
        genre = "Unknown"
        year = 0
        imdb_id = None
        rating = "N/A"
        category = "Mixed"
        description = "Uploaded via Bot"

        if metadata:
            try:
                # Unpack Metadata
                m_title, m_year, m_poster, m_genre, m_imdb, m_rating, m_plot, m_cat = metadata
                poster_url = m_poster
                year = m_year
                genre = m_genre
                imdb_id = m_imdb
                rating = m_rating
                description = m_plot
                category = m_cat
                
                # ✅ FIX 1: Agar IMDb mila hai, to Naam REPLACE kar do
                if m_title:
                    movie_title = f"{m_title} ({m_year})" if m_year else m_title
                    
            except Exception as e:
                logger.error(f"Metadata Error: {e}")
        else:
            # ✅ FIX 2: Agar IMDb NAHI mila, to bhi naam Clean karo (Fallback)
            # Faltu words hatane ka Regex
            clean_fallback = re.sub(r'\b(480p|720p|1080p|2160p|hevc|x264|x265|10bit|hindi|dubbed|dual|audio|eng|sub|web-dl|webrip|hdr|remux|truehd|atmos|amzn|nf|dsnp|s\d+e\d+|complete|season|s\d+)\b', '', movie_title, flags=re.IGNORECASE)
            clean_fallback = re.sub(r'[._-]', ' ', clean_fallback)
            clean_fallback = re.sub(r'\s+', ' ', clean_fallback).strip()
            # Agar naam 2 letter se bada hai, to use update kar do
            if len(clean_fallback) > 2:
                movie_title = clean_fallback

        # 🔥 STEP 1.1: Landscape Thumbnail Generate Karo 🔥
        landscape_thumb = None
        if poster_url:
            landscape_thumb = await run_async(get_smart_thumbnail, poster_url)

        # 2. Database Entry (Movies Table)
        conn = get_db_connection()
        if not conn:
            await query.message.reply_text("❌ DB Connection Failed!")
            return

        cur = conn.cursor()
        try:
            # 3. Insert or Update Movie
            cur.execute(
                """
                INSERT INTO movies (title, url, imdb_id, poster_url, year, genre, rating, description, category) 
                VALUES (%s, '', %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (title) DO UPDATE 
                SET imdb_id = COALESCE(EXCLUDED.imdb_id, movies.imdb_id),
                    poster_url = COALESCE(EXCLUDED.poster_url, movies.poster_url)
                RETURNING id
                """,
                (movie_title, imdb_id, poster_url, year, genre, rating, description, category)
            )
            movie_id = cur.fetchone()[0]
            conn.commit()

            # 4. Upload Loop (With Landscape Thumbnail)
            channels = get_storage_channels()
            success_count = 0
            
            for file_data in files:
                msg_obj = file_data['message_obj']
                f_name = file_data['file_name']
                f_size = file_data['file_size']
                f_size_str = get_readable_file_size(f_size)
                
                # File ID selection
                file_id_to_send = msg_obj.document.file_id if msg_obj.document else msg_obj.video.file_id

                # Branding Name (Clean Title Use karega)
                clean_title_for_file = re.sub(r'[^\w\s-]', '', movie_title).strip()
                new_filename = f"[@FilmFyBox] {clean_title_for_file}.mkv"
                
                quality_label = generate_quality_label(f_name, f_size_str)
                backup_map = {}
                main_url = ""

                # Upload to Backups
                for ch_id in channels:
                    try:
                        # Reset Thumb Pointer
                        if landscape_thumb:
                            landscape_thumb.seek(0)

                        # 🔥 Send with Landscape Thumbnail 🔥
                        sent = await context.bot.send_document(
                            chat_id=ch_id,
                            document=file_id_to_send,
                            filename=new_filename,
                            thumbnail=landscape_thumb, 
                            caption=f"🎬 {movie_title}\n✨ @FilmFyBoxMoviesHD",
                            parse_mode='Markdown'
                        )
                        backup_map[str(ch_id)] = sent.message_id
                        
                        if not main_url:
                            clean_id = str(ch_id).replace("-100", "")
                            main_url = f"https://t.me/c/{clean_id}/{sent.message_id}"
                            
                    except Exception as e:
                        logger.error(f"Upload failed for {ch_id}: {e}")

                # Save File to DB
                if main_url:
                    cur.execute(
                        """
                        INSERT INTO movie_files (movie_id, quality, file_size, url, backup_map) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (movie_id, quality) 
                        DO UPDATE SET url = EXCLUDED.url, backup_map = EXCLUDED.backup_map
                        """,
                        (movie_id, quality_label, f_size_str, main_url, json.dumps(backup_map))
                    )
                    success_count += 1

            conn.commit()

            # --- 5. AI ALIAS GENERATION ---
            alias_msg = ""
            try:
                await query.edit_message_text(f"🧠 **Generating AI Aliases...**\n🎬 Movie: {movie_title}")
                aliases = await run_async(generate_aliases_gemini, movie_title)
                alias_count = 0
                if aliases:
                    for alias in aliases:
                        if len(alias) > 255: continue
                        try:
                            cur.execute("SAVEPOINT sp_alias")
                            cur.execute(
                                "INSERT INTO movie_aliases (movie_id, alias) VALUES (%s, %s) ON CONFLICT (movie_id, alias) DO NOTHING",
                                (movie_id, alias)
                            )
                            cur.execute("RELEASE SAVEPOINT sp_alias")
                            alias_count += 1
                        except Exception:
                            cur.execute("ROLLBACK TO SAVEPOINT sp_alias")
                    conn.commit()
                    alias_msg = f"🤖 **Aliases:** {alias_count} Added"
                else:
                    alias_msg = "🤖 **Aliases:** API Failed/No Data"
            except Exception as e:
                logger.error(f"Alias Gen Error: {e}")
                alias_msg = "🤖 **Aliases:** Error"

            # 6. Final Output
            final_code = f"/post_query {movie_title}"
            report = (
                f"✅ **Batch Completed!**\n"
                f"🎬 Movie: `{movie_title}`\n"
                f"📂 Files Saved: {success_count}\n"
                f"ℹ️ IMDb: {'✅ Found' if poster_url else '❌ Not Found (Cleaned)'}\n"
                f"{alias_msg}\n\n"
                f"👇 **Copy & Post This Code:**"
            )

            if poster_url and "http" in poster_url:
                await context.bot.send_photo(chat_id=user_id, photo=poster_url, caption=f"{report}\n`{final_code}`", parse_mode='Markdown')
            else:
                await context.bot.send_message(chat_id=user_id, text=f"{report}\n`{final_code}`", parse_mode='Markdown')

            del PENDING_DRAFTS[user_id]

        except Exception as e:
            logger.error(f"Process Draft Error: {e}")
            await query.message.reply_text(f"❌ Error: {e}")
            conn.rollback()
        finally:
            cur.close()
            close_db_connection(conn)
        return
    
    # === NEW: GENRE CALLBACK HANDLER ===
    if data.startswith(("genre_", "cancel_genre")):
        await handle_genre_selection(update, context)
        return
    
    # === SEND ALL FILES LOGIC ===
    if query.data.startswith("sendall_"):
        movie_id = int(query.data.split("_")[1])
        chat_id = update.effective_chat.id

        # Movie Info Fetch
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
        res = cur.fetchone()
        title = res[0] if res else "Movie"
        cur.close()
        close_db_connection(conn)

        qualities = get_all_movie_qualities(movie_id)
        if not qualities:
            await query.answer("❌ No files found!", show_alert=True)
            return

        await query.answer(f"🚀 Sending {len(qualities)} files...")
        status_msg = await query.message.reply_text(f"🚀 **Sending {len(qualities)} files...**", parse_mode='Markdown')
        
        # 1. LOOP: FILES BHEJO (Warning = False)
        # Yahan hum 'send_warning=False' pass kar rahe hain taaki har file ke sath warning na aye
        count = 0
        for quality, url, file_id, file_size in qualities:
            try:
                await send_movie_to_user(
                    update, context, movie_id, title, url, file_id, 
                    send_warning=False  # 👈 IMPORTANT: Loop me warning mat bhejo
                )
                await asyncio.sleep(1.5) 
                count += 1
            except Exception as e:
                logger.error(f"Send All Error: {e}")

        # 2. END: WARNING FILE BHEJO (Ek hi baar)
        try:
            warning_msg = await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=-1002683355160, # Apka Channel ID
                message_id=1773              # Warning File Message ID
            )
            # Is file ko bhi delete list me daalo
            track_message_for_deletion(context, chat_id, warning_msg.message_id, 60)
        except Exception as e:
            logger.error(f"Failed to send final warning file: {e}")

        await status_msg.edit_text(f"✅ **Sent {count} Files!**", parse_mode='Markdown')
        track_message_for_deletion(context, chat_id, status_msg.message_id, 30)
        return
    
    # === NEW: SCAN INFO POPUP ===
    if data.startswith("scan_"):
        m_id = int(data.split("_")[1])
        
        # Database se details nikalo
        conn = get_db_connection()
        cur = conn.cursor()
        # Maan lo tumhare DB mein 'language' aur 'subtitle' column hain, ya tum file name se guess karoge
        cur.execute("SELECT title, year, genre FROM movies WHERE id = %s", (m_id,))
        res = cur.fetchone()
        cur.close()
        close_db_connection(conn)

        if res:
            title, year, genre = res
            # Ye wo text hai jo Popup mein dikhega
            popup_text = (
                f"📂 File Info:\n"
                f"🎬 Movie: {title}\n"
                f"📅 Year: {year}\n"
                f"🎭 Genre: {genre}\n"
                f"🔊 Audio: Hindi, English (Dual)\n" # Ise DB se dynamic bana sakte ho
                f"📝 Subs: English, Hindi"
            )
            # show_alert=True ka matlab hai Screen par bada popup aayega!
            await query.answer(popup_text, show_alert=True)
        else:
            await query.answer("❌ Info not found", show_alert=True)
        return
    
    # ===================================
    
    # 👇👇👇 YE NAYA CODE ADD KARO 👇👇👇
    if query.data.startswith("clearfiles_"):
        if update.effective_user.id != ADMIN_USER_ID:
            await query.answer("❌ Sirf Admin ke liye!", show_alert=True)
            return

        movie_id = int(query.data.split("_")[1])
        
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM movie_files WHERE movie_id = %s", (movie_id,))
                deleted_count = cur.rowcount # Kitni delete hui
                conn.commit()
                cur.close()
                close_db_connection(conn)
                
                await query.answer(f"✅ {deleted_count} purani files delete ho gayi!", show_alert=True)
                await query.edit_message_text(
                    f"🗑️ **Deleted {deleted_count} old files.**\n\n"
                    f"✅ **Clean Slate!** Ab nayi files upload karo.",
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Delete Error: {e}")
                await query.answer("❌ Error deleting files", show_alert=True)
        return
    # 👆👆👆 YAHAN TAK 👆👆👆
    
    
    # === 1. VERIFY BUTTON LOGIC (UPDATED) ===
    if data == "verify":
        await query.answer("🔍 Checking membership...", show_alert=False) # Alert False rakha taki user disturb na ho
        
        # Force Fresh Check
        check = await is_user_member(context, user_id, force_fresh=True)
        
        if check['is_member']:
            # ✅ SCENARIO 1: Agar koi Deep Link pending tha (e.g. start=movie_123)
            if 'pending_start_args' in context.user_data:
                saved_args = context.user_data.pop('pending_start_args')
                
                # "Verified" wala msg delete kar do taaki clean lage
                try: await query.message.delete()
                except: pass
                
                # Start function ko manually call karo saved args ke saath
                context.args = saved_args
                await start(update, context)
                return

            # ✅ SCENARIO 2: Agar koi Text Search pending tha (e.g. "Kalki")
            elif 'pending_search_query' in context.user_data:
                saved_query = context.user_data.pop('pending_search_query')
                
                # "Verified" wala msg delete kar do
                try: await query.message.delete()
                except: pass
                
                # Search Movies ko call karne ke liye update object ko modify karein
                # Hum current query message ko use karenge par text replace kar denge
                update.message = query.message 
                update.message.text = saved_query
                
                # User ko feedback do ki search shuru ho gaya
                await search_movies(update, context)
                return

            # ✅ SCENARIO 3: Agar koi pending request nahi thi (Normal Verify)
            else:
                await query.edit_message_text(
                    "✅ **Verified Successfully!**\n\n"
                    "You can now use the bot! 🎬\n"
                    "Click /start or search any movie.",
                    parse_mode='Markdown'
                )
                track_message_for_deletion(context, chat_id, query.message.message_id, 10)
        else:
            # Agar abhi bhi join nahi kiya
            try:
                await query.edit_message_text(
                    get_join_message(check['channel'], check['group']),
                    reply_markup=get_join_keyboard(),
                    parse_mode='Markdown'
                )
            except telegram.error.BadRequest:
                await query.answer("❌ You haven't joined yet!", show_alert=True)
        return
    # ==============================

    # === 2. OTHER BUTTONS PROTECTION (Optional but Recommended) ===
    # Agar user 'download', 'movie', 'request' dabaye to bhi check karo
    if data.startswith(("movie_", "download_", "quality_", "request_")):
        check = await is_user_member(context, user_id) # Cache use karega
        if not check['is_member']:
            await query.answer("❌ Please join channels first!", show_alert=True)
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            return
    # ==============================================================

    try:
        # ==================== MOVIE SELECTION ====================
        if query.data.startswith("movie_"):
            movie_id = int(query.data.replace("movie_", ""))

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM movies WHERE id = %s", (movie_id,))
            movie = cur.fetchone()
            cur.close()
            close_db_connection(conn)

            if not movie:
                await query.edit_message_text("❌ Movie not found in database.")
                return

            movie_id, title = movie
            qualities = get_all_movie_qualities(movie_id)

            if not qualities:
                await query.edit_message_text(f"✅ You selected: **{title}**\n\nSending movie.. .", parse_mode='Markdown')
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT url, file_id FROM movies WHERE id = %s", (movie_id,))
                result = cur.fetchone()
                url, file_id = result if result else (None, None)  # ✅ Added safety check
                cur.close()
                close_db_connection(conn)

                await send_movie_to_user(update, context, movie_id, title, url, file_id)
                return

            context.user_data['selected_movie_data'] = {
                'id':  movie_id,
                'title': title,
                'qualities':  qualities
            }

            selection_text = f"✅ You selected: **{title}**\n\n⬇️ **Please choose the file quality:**"
            keyboard = create_quality_selection_keyboard(movie_id, title, qualities)

            await query.edit_message_text(
                selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            
            track_message_for_deletion(context, update. effective_chat. id, query.message.message_id, 60)


        # ==================== ADMIN ACTIONS ====================
        
        # ==================== QUALITY PAGINATION (NEXT/BACK) ====================
        elif query.data.startswith("qualpage_"):
            parts = query.data.split('_')
            movie_id = int(parts[1])
            page = int(parts[2])

            # Try fetching data from user_data first (Fast)
            movie_data = context.user_data.get('selected_movie_data')
            
            # Agar data expire ho gaya ho ya ID match na kare, to DB se nikalo
            if not movie_data or movie_data.get('id') != movie_id:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                res = cur.fetchone()
                cur.close()
                close_db_connection(conn)
                
                title = res[0] if res else "Movie"
                qualities = get_all_movie_qualities(movie_id)
                
                # Context update karo
                context.user_data['selected_movie_data'] = {
                    'id': movie_id,
                    'title': title,
                    'qualities': qualities
                }
            else:
                title = movie_data['title']
                qualities = movie_data['qualities']

            # New Keyboard with updated page
            keyboard = create_quality_selection_keyboard(movie_id, title, qualities, page=page)
            
            # Sirf buttons update karein (Text same rahega)
            await query.edit_message_reply_markup(reply_markup=keyboard)
        
        elif query.data.startswith("admin_fulfill_"):
            parts = query.data.split('_', 3)
            user_id = int(parts[2])
            movie_title = parts[3]

            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT id, url, file_id FROM movies WHERE title = %s LIMIT 1", (movie_title,))
                movie_data = cur.fetchone()

                if movie_data:
                    movie_id, url, file_id = movie_data
                    value_to_send = file_id if file_id else url
                    num_notified = await notify_users_for_movie(context, movie_title, value_to_send)

                    await query.edit_message_text(
                        f"✅ FULFILLED: Movie '{movie_title}' updated and user (ID: {user_id}) notified ({num_notified} total users).",
                        parse_mode='Markdown'
                    )
                else:
                    await query.edit_message_text(f"❌ ERROR: Movie '{movie_title}' not found in the `movies` table. Please add it first.", parse_mode='Markdown')

                cur.close()
                close_db_connection(conn)
            else:
                await query.edit_message_text("❌ Database error during fulfillment.")

        elif query.data.startswith("admin_delete_"):
            parts = query.data.split('_', 3)
            user_id = int(parts[2])
            movie_title = parts[3]

            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM user_requests WHERE user_id = %s AND movie_title = %s", (user_id, movie_title))
                conn.commit()
                cur.close()
                close_db_connection(conn)
                await query.edit_message_text(f"❌ DELETED: Request for '{movie_title}' from User ID {user_id} removed.", parse_mode='Markdown')
            else:
                await query.edit_message_text("❌ Database error during deletion.")

        # ==================== QUALITY SELECTION ====================
        elif query.data.startswith("quality_"):
            parts = query.data.split('_')
            movie_id = int(parts[1])
            selected_quality = parts[2]

            movie_data = context.user_data.get('selected_movie_data')

            if not movie_data or movie_data.get('id') != movie_id:
                qualities = get_all_movie_qualities(movie_id)
                # Note: qualities now contains (quality, url, file_id, file_size)
                movie_data = {'id': movie_id, 'title': 'Movie', 'qualities': qualities}

            if not movie_data or 'qualities' not in movie_data:
                await query.edit_message_text("❌ Error: Could not retrieve movie data. Please search again.")
                return

            chosen_file = None
            
            # --- FIX IS BELOW THIS LINE ---
            # We added 'file_size' to the unpacking because the DB function returns 4 values now
            for quality, url, file_id, file_size in movie_data['qualities']:
                if quality == selected_quality:
                    chosen_file = {'url': url, 'file_id': file_id}
                    break
            # -----------------------------

            if not chosen_file:
                await query.edit_message_text("❌ Error fetching the file for that quality.")
                return

            title = movie_data['title']
            await query.edit_message_text(f"Sending **{title}**...", parse_mode='Markdown')

            await send_movie_to_user(
                update,
                context,
                movie_id,
                title,
                chosen_file['url'],
                chosen_file['file_id']
            )

            if 'selected_movie_data' in context.user_data:
                del context.user_data['selected_movie_data']

        # ==================== PAGINATION ====================
        elif query.data.startswith("page_"):
            page = int(query.data.replace("page_", ""))

            if 'search_results' not in context.user_data:
                await query.edit_message_text("❌ Search results expired. Please search again.")
                return

            movies = context.user_data['search_results']
            search_query = context.user_data.get('search_query', 'your search')

            selection_text = f"🎬 **Found {len(movies)} movies matching '{search_query}'**\n\nPlease select the movie you want:"
            keyboard = create_movie_selection_keyboard(movies, page=page)

            await query.edit_message_text(
                selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )

        elif query.data == "cancel_selection":
            await query.edit_message_text("❌ Selection cancelled.")
            keys_to_clear = ['search_results', 'search_query', 'selected_movie_data', 'awaiting_request', 'pending_request']
            for key in keys_to_clear:
                if key in context.user_data:
                    del context.user_data[key]

        
        # ==================== DOWNLOAD SHORTCUT ====================
        elif query.data.startswith("download_"):
            movie_title = query.data.replace("download_", "")

            conn = get_db_connection()
            if not conn:
                await query.answer("❌ Database connection failed.", show_alert=True)
                return

            cur = conn.cursor()
            cur.execute("SELECT id, title, url, file_id FROM movies WHERE title ILIKE %s LIMIT 1", (f'%{movie_title}%',))
            movie = cur.fetchone()
            cur.close()
            close_db_connection(conn)

            if movie:
                movie_id, title, url, file_id = movie
                await send_movie_to_user(update, context, movie_id, title, url, file_id)
            else:
                await query.answer("❌ Movie not found.", show_alert=True)

    except Exception as e:
        logger.error(f"Error in button callback: {e}")
        try:
            await query.answer(f"❌ Error: {str(e)}", show_alert=True)
        except:
            pass

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the current operation"""
    msg = await update.message.reply_text("Operation cancelled.", reply_markup=get_main_keyboard())
    track_message_for_deletion(update.effective_chat.id, msg.message_id, 60)
    return MAIN_MENU

# ==================== NEW MULTI-CHANNEL BACKUP FUNCTIONS ====================

def get_storage_channels():
    """Load channel list from .env"""
    channels_str = os.environ.get('STORAGE_CHANNELS', '')
    return [int(c.strip()) for c in channels_str.split(',') if c.strip()]

def generate_quality_label(file_name, file_size_str):
    """
    Generates a label like '720p [1.2GB]' or 'S01E01 - 1080p [500MB]'
    """
    name_lower = file_name.lower()
    quality = "HD" # Default
    
    # 1. Detect Quality
    if "4k" in name_lower or "2160p" in name_lower: quality = "4K"
    elif "1080p" in name_lower: quality = "1080p"
    elif "720p" in name_lower: quality = "720p"
    elif "480p" in name_lower: quality = "480p"
    elif "360p" in name_lower: quality = "360p"
    elif "cam" in name_lower or "rip" in name_lower: quality = "CamRip"
    
    # 2. Detect Series (S01E01)
    season_match = re.search(r'(s\d+e\d+|ep\s?\d+|season\s?\d+)', name_lower)
    if season_match:
        episode_tag = season_match.group(0).upper()
        # Returns: S01E01 - 720p [1.2GB]
        return f"{episode_tag} - {quality} [{file_size_str}]"
        
    # 3. Default Movie Format: 720p [1.2GB]
    return f"{quality} [{file_size_str}]"

def get_readable_file_size(size_in_bytes):
    """Converts bytes to readable format (MB, GB)"""
    try:
        if not size_in_bytes: return "N/A"
        size = int(size_in_bytes)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
    except Exception:
        return "Unknown"
    return "Unknown"

def generate_aliases_gemini(movie_title):
    """
    Generates 50 SEO aliases using a detailed Search Query Analyst prompt.
    """
    print(f"\n🚀 [DEBUG 1] Alias generation start hui: '{movie_title}' ke liye")
    
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("❌ [DEBUG 2] ERROR: GEMINI_API_KEY .env file me nahi mili!")
        return []
        
    print("✅ [DEBUG 3] API Key mil gayi. Model load kar rahe hain...")

    try:
        # Configure Gemini
        genai.configure(api_key=api_key)
        
        # ✅ Using Gemini 2.0 Flash (as per current available stable naming, 
        # change to 'gemini-1.5-flash' if 2.0 is not in your region yet)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        prompt = f"""
        Generate 20 relevant search keywords/aliases for the movie: "{movie_title}".
        Include typos, Hinglish spellings, and short forms.
        Output ONLY comma-separated text. No numbering.
        Example: kalki, kalki 2898, kalki hindi, culki movie
        """

        response = model.generate_content(prompt)
        content = response.text
        
        # Clean and split by comma
        aliases = [x.strip().lower() for x in content.split(',') if x.strip()]
        
        logger.info(f"✅ Generated {len(aliases)} aliases for '{movie_title}'")
        return aliases

    except Exception as e:
        logger.error(f"❌ Gemini AI Error: {e}")
        return []
    try:
        # Configure Gemini
        genai.configure(api_key=api_key)
        
        # Use correct model name (gemini-2.5-flash is free & fast)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # Your Prompt
        prompt = f"""
Act as a Search Query Analyst, SEO Expert, and User Behavior Specialist.

I need an exhaustive list of "Aliases," "Keywords," and "Search Terms" for the movie/web series: "{movie_title}".

**CONTEXT (if available):**
- Release Year:
- Language:
- Platform (Netflix/Prime/Hotstar etc.):
- Director/Lead Actors:
- Genre:

Generate a list that covers ALL possible ways a human might type this query — including mistakes, regional variations, voice search patterns, and intent-based searches. Do NOT summarize; list every variation explicitly.

---

## OUTPUT CATEGORIES (8 Categories):

### 1. Official & Structural Variations:
- Full official title with punctuation
- Subtitle variations (with/without colon, dash)
- Season/Episode notations (Season 1, S01, S1, Part 1, Vol 1, Chapter 1)
- Year combinations (Movie Name 2025)
- Language-specific official titles (Hindi/Tamil/Telugu titles)
- Franchise naming (Part 1, Part 2, Sequel name)

### 2. Acronyms & Short Forms:
- First letter acronyms (DDLJ, GOT, BB3)
- Common fan abbreviations
- Lazy typing shortcuts
- Social media hashtag formats (#MovieName)

### 3. Phonetic & Spelling Mistakes (CRITICAL SECTION):
Focus on how someone would TYPE if they only HEARD the name:
- Hinglish/Regional phonetic errors
- Vowel swaps: a↔e, i↔e, u↔o, aa↔a
- Consonant confusion: ph↔f, sh↔s, th↔t, ch↔c, kh↔k, dh↔d
- Double letter errors (single→double, double→single)
- Silent letter omissions
- Regional accent-based spellings (South Indian, Punjabi, Bengali pronunciation)

### 4. Keyboard Slips & Fat-finger Errors:
- Adjacent QWERTY key mistakes (a↔s, i↔o, n↔m)
- Skipped letters (typing too fast)
- Double-pressed keys
- Mobile swipe keyboard errors
- Spacebar errors (no space, extra space, wrong space position)

### 5. Platform & Intent-Based Searches:
- "[Movie] watch online"
- "[Movie] download"
- "[Movie] Netflix/Prime/Hotstar"
- "[Movie] full movie"
- "[Movie] Hindi dubbed"
- "[Movie] trailer"
- "[Movie] review"

### 6. Voice Search & Conversational Queries:
- "Play [Movie name]"
- "Show me [Movie name]"
- "I want to watch [Movie name]"

### 7. Cast & Crew Combinations:
- "[Actor name] new movie"
- "[Director] latest film"

### 8. Regional & Transliteration Variations:
- Devanagari to Roman transliteration errors
- Tamil/Telugu/Kannada phonetic spellings

---

## OUTPUT FORMAT:
Provide exactly 50 comma-separated aliases only. No explanations, no categories, no numbering. Just plain comma-separated text.

Example output format:
alias1, alias2, alias3, alias4, alias5...
"""

        print("⏳ [DEBUG 4] Google AI ko detailed request bhej rahe hain...")
        response = model.generate_content(prompt)
        
        print("📥 [DEBUG 5] AI se response aa gaya!")
        
        try:
            ai_text = response.text
            # Print only first 100 chars to keep logs clean
            print(f"🤖 [DEBUG 6] AI ne response diya (length: {len(ai_text)})")
        except ValueError:
            print(f"❌ [DEBUG 6.1] Safety Blocked! AI ne response block kar diya.")
            return []

        if not ai_text:
            print("❌ [DEBUG 7] AI ka text khali hai.")
            return []

        # Clean and split by comma
        aliases = [x.strip().lower() for x in ai_text.split(',') if x.strip()]
        
        # Agar list bahut badi ho jaye (kabhi AI extra bhej deta hai), toh limit to 50
        aliases = aliases[:50]
        
        print(f"✅ [DEBUG 8] Total {len(aliases)} Aliases tayyar hain.")
        return aliases

    except Exception as e:
        print(f"❌ [DEBUG 9] CRITICAL ERROR Gemini me: {e}")
        return []

# ==================== NEW BATCH COMMAND WITH MULTI-CHANNEL UPLOAD ====================

async def batch_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Direct IMDb ID batch command"""
    if update.effective_user.id != ADMIN_USER_ID:
        return
        
    if not context.args:
        await update.message.reply_text("❌ Usage: `/batchid tt1234567`", parse_mode='Markdown')
        return
        
    imdb_id = context.args[0].strip()
    
    # Check if helper function exists, otherwise simple regex check
    if 'is_valid_imdb_id' in globals():
        if not is_valid_imdb_id(imdb_id):
            await update.message.reply_text("❌ Invalid IMDb ID format. Must be tt + 7-8 digits (e.g., tt15462578)")
            return
    elif not re.match(r'^tt\d{7,12}$', imdb_id):
         await update.message.reply_text("❌ Invalid IMDb ID format. Must be tt + 7-8 digits (e.g., tt15462578)")
         return
        
    # Reuse batch_add_command logic
    context.args = [imdb_id]
    await batch_add_command(update, context)

async def batch_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID: return

    if not context.args: 
        await update.message.reply_text("❌ Usage: `/batch Name`")
        return

    # 1. Loading Message
    status_msg = await update.message.reply_text("⏳ Checking database & metadata...", parse_mode='Markdown')
    
    query = " ".join(context.args).strip()
    
    # --- YAHAN CHANGE KIYA HAI (Smart Logic) ---
    # Pehle metadata dhundo
    metadata = await run_async(fetch_movie_metadata, query)
    
    if metadata:
        # Agar IMDb/OMDb par mil gayi, to Original Data use karo
        title, year, poster_url, genre, imdb_id, rating, plot, category = metadata
    else:
        # Agar IMDb par NAHI mili, to Error mat do.
        # Jo naam user ne diya hai, wahi use karo (Manual Mode)
        title = query
        year = 0
        poster_url = None
        genre = "Unknown"
        imdb_id = None
        rating = "N/A"
        plot = "Custom Added"
        category = "Custom"
    # -------------------------------------------
    
    conn = get_db_connection()
    if not conn: return
    
    try:
        cur = conn.cursor()
        
        # 2. Movie Data Insert/Update (Ye Logic same rahega)
        # Agar movie pehle se hai (ON CONFLICT), to bas update karega, naya nahi banayega
        cur.execute(
            """
            INSERT INTO movies (title, url, imdb_id, poster_url, year, genre, rating, description, category) 
            VALUES (%s, '', %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (title) DO UPDATE 
            SET imdb_id = COALESCE(EXCLUDED.imdb_id, movies.imdb_id), -- Purana ID safe rakho agar naya NULL hai
                poster_url = COALESCE(EXCLUDED.poster_url, movies.poster_url),
                year = CASE WHEN movies.year = 0 THEN EXCLUDED.year ELSE movies.year END
            RETURNING id
            """,
            (title, imdb_id, poster_url, year, genre, rating, plot, category)
        )
        movie_id = cur.fetchone()[0]
        conn.commit()

        # 3. Check for OLD FILES
        cur.execute("SELECT COUNT(*) FROM movie_files WHERE movie_id = %s", (movie_id,))
        file_count = cur.fetchone()[0]
        
        cur.close()
        close_db_connection(conn)

        # 4. Activate Batch Session
        BATCH_SESSION.update({
            'active': True,
            'movie_id': movie_id,
            'movie_title': title,
            'file_count': 0,
            'admin_id': user_id
        })

        # 5. Message Prepare Karo
        msg_text = (
            f"🎬 **Batch Started:** {title}\n"
            f"📂 **Existing Files in DB:** {file_count}\n"
        )
        
        if not metadata:
            msg_text += "⚠️ *Metadata not found (Custom Mode ON)*\n"

        msg_text += "\n🚀 **Send NEW files now!** (Bot is listening...)"

        # 6. Delete Button Logic
        reply_markup = None
        if file_count > 0:
            msg_text += "\n\n⚠️ *Purani files mili hain. Delete button niche hai:* 👇"
            keyboard = [[InlineKeyboardButton("🗑️ Delete OLD Files (Clean Slate)", callback_data=f"clearfiles_{movie_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

        # 7. Final Message Send
        await status_msg.edit_text(msg_text, parse_mode='Markdown', reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Batch Error: {e}")
        await status_msg.edit_text(f"❌ DB Error: {e}")
        if conn: close_db_connection(conn)

def get_smart_thumbnail(poster_url):
    """
    Magic Function: 
    1. Downloads Poster.
    2. Creates a 16:9 Landscape Canvas (320x180).
    3. Adds Blurred Background + Centered Poster.
    """
    try:
        if not poster_url: return None
        
        # 1. Image Download
        response = requests.get(poster_url, timeout=5)
        if response.status_code != 200: return None
        
        img = Image.open(BytesIO(response.content)).convert("RGB")
        
        # --- LANDSCAPE LOGIC (16:9) ---
        # Target Dimensions (Telegram Thumb Limit: 320px width)
        # 16:9 Ratio of 320 width is approx 180 height
        target_w, target_h = 320, 180
        
        # A. Background Create karo (Blur wala)
        # Image ko itna bada karo ki width fit ho jaye
        bg = img.copy()
        bg_ratio = target_w / bg.width
        bg_new_h = int(bg.height * bg_ratio)
        bg = bg.resize((target_w, bg_new_h), Image.Resampling.LANCZOS)
        
        # Center Crop karo (taaki height 180 hi rahe)
        left = 0
        top = (bg_new_h - target_h) / 2
        right = target_w
        bottom = (bg_new_h + target_h) / 2
        bg = bg.crop((left, top, right, bottom))
        
        # Blur Effect Lagao
        bg = bg.filter(ImageFilter.GaussianBlur(radius=3)) # 3-5 is good blur
        
        # B. Foreground (Main Poster)
        # Isko height ke hisab se resize karo (taaki wo 180px height me fit aye)
        fg = img.copy()
        fg.thumbnail((target_w, target_h - 10)) # Thoda margin (padding) choda
        
        # C. Paste karo (Center mein)
        # Background par Foreground chipkao
        fg_x = (target_w - fg.width) // 2
        fg_y = (target_h - fg.height) // 2
        bg.paste(fg, (fg_x, fg_y))
        
        # 4. Save to Bytes
        output = BytesIO()
        bg.save(output, format='JPEG', quality=90)
        output.seek(0)
        return output
        
    except Exception as e:
        logger.error(f"Landscape Thumb Error: {e}")
        return None

# 👆👆👆 YAHAN KHATAM 👆👆👆

async def pm_file_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin se file receive karne wala function (Cleaner Logic)"""
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID: return

    # Rename Mode Check
    if context.user_data.get('awaiting_draft_rename'):
        return await handle_draft_rename(update, context)

    message = update.effective_message
    if not (message.document or message.video): return

    file_name = message.document.file_name if message.document else (message.video.file_name or "Unknown")
    file_size = message.document.file_size if message.document else message.video.file_size
    
    # --- 🔥 NAME CLEANING (Ye "Backchodi" hatane ke liye hai) 🔥 ---
    clean_name = file_name
    
    # 1. Sabse pehle brackets [...] hatao (Jisme channel name hota hai)
    # Example: "[@ClipmateZone] Movie.mkv" -> " Movie.mkv"
    clean_name = re.sub(r'\[.*?\]', '', clean_name)
    
    # 2. Dots, Underscore hatao
    clean_name = re.sub(r'[._-]', ' ', clean_name)
    
    # 3. Year ke baad ka sab kuch uda do (Safe Game)
    # Example: "Movie Name 2024 Dual Audio..." -> "Movie Name"
    match = re.search(r'(.*?\b(19|20)\d{2}\b)', clean_name)
    if match:
        clean_name = match.group(1)

    # 4. Faltu words ki list (Regex)
    clean_name = re.sub(r'\b(480p|720p|1080p|2160p|hevc|x264|x265|10bit|hindi|dubbed|dual|audio|eng|sub|web-dl|webrip|web|hdrip|bluray|camrip|pre-dvdrip|s\d+e\d+|season|episode)\b', '', clean_name, flags=re.IGNORECASE)
    
    # 5. Extra spaces clean karo
    clean_name = re.sub(r'\s+', ' ', clean_name).strip()

    # --- DRAFT LOGIC ---
    if user_id not in PENDING_DRAFTS:
        PENDING_DRAFTS[user_id] = {'files': [], 'suggested_name': clean_name}
    
    PENDING_DRAFTS[user_id]['files'].append({
        'message_obj': message,
        'file_name': file_name,
        'file_size': file_size
    })

    count = len(PENDING_DRAFTS[user_id]['files'])
    # Agar ye pehli file hai to clean name set karo
    if count == 1:
        PENDING_DRAFTS[user_id]['suggested_name'] = clean_name
        
    current_name = PENDING_DRAFTS[user_id]['suggested_name']

    keyboard = [
        [InlineKeyboardButton(f"✅ PROCESS: {current_name}", callback_data="process_draft")],
        [InlineKeyboardButton("✏️ Rename Title", callback_data="edit_draft_name")],
        [InlineKeyboardButton("🔗 Set IMDb ID", callback_data="set_draft_imdb")],
        [InlineKeyboardButton("🗑️ Cancel All", callback_data="clear_draft")]
    ]

    await message.reply_text(
        f"📥 **Draft Staging ({count} Files)**\n🎬 Target: `{current_name}`\n👇 Aur files bhejo ya Process dabao.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

    sent_msg = await message.reply_text(status_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    PENDING_DRAFTS[user_id]['last_msg_id'] = sent_msg.message_id


async def handle_draft_rename(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Step 2: Agar admin ne naya naam bheja hai to update karo.
    """
    user_id = update.effective_user.id
    new_name = update.message.text.strip()
    
    if user_id in PENDING_DRAFTS:
        # Naam update karo
        PENDING_DRAFTS[user_id]['suggested_name'] = new_name
        
        # State clear karo
        context.user_data['awaiting_draft_rename'] = False
        
        # Menu wapas dikhao
        count = len(PENDING_DRAFTS[user_id]['files'])
        keyboard = [
            [InlineKeyboardButton(f"✅ PROCESS: {new_name}", callback_data="process_draft")],
            [InlineKeyboardButton("✏️ Rename Title", callback_data="edit_draft_name")],
            [InlineKeyboardButton("🗑️ Cancel All", callback_data="clear_draft")]
        ]
        
        await update.message.reply_text(
            f"✅ **Name Updated!**\n🎬 New Name: `{new_name}`\n🔢 Files: {count}\n\n👇 Ab Process dabayein:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("❌ Koi active draft nahi mila.")
        context.user_data['awaiting_draft_rename'] = False
    
async def handle_draft_imdb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """IMDb ID manually set karne ke liye"""
    user_id = update.effective_user.id
    imdb_id = update.message.text.strip()
    
    # Simple validation
    if not imdb_id.startswith("tt"):
        await update.message.reply_text("❌ Invalid ID! `tt` se shuru honi chahiye (e.g. tt1234567).")
        return

    if user_id in PENDING_DRAFTS:
        # Draft me ID save kar lo
        PENDING_DRAFTS[user_id]['manual_imdb_id'] = imdb_id
        
        # State clear
        context.user_data['awaiting_draft_imdb'] = False
        
        # --- BUTTON NAME CHANGE ---
        # Ab hum button par "Naam" nahi, "ID" dikhayenge taaki confirmation rahe
        keyboard = [
            [InlineKeyboardButton(f"✅ PROCESS NOW (Using ID: {imdb_id})", callback_data="process_draft")],
            [InlineKeyboardButton("✏️ Rename Title", callback_data="edit_draft_name")],
            [InlineKeyboardButton(f"🔗 ID Updated: {imdb_id}", callback_data="set_draft_imdb")],
            [InlineKeyboardButton("🗑️ Cancel All", callback_data="clear_draft")]
        ]
        
        await update.message.reply_text(
            f"✅ **IMDb ID Linked!**\n🆔 `{imdb_id}`\n\nAb Process dabane par bot **sidha IMDb se data uthayega**, purane naam ko ignore kar dega.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def batch_done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Finishes session -> Generates AI Aliases -> Shows Report
    """
    if not BATCH_SESSION['active']: return

    status_msg = await update.message.reply_text("🔄 **Generating AI Aliases... Please wait!** 🧠")

    # 1. AI Generation
    movie_title = BATCH_SESSION['movie_title']
    movie_id = BATCH_SESSION['movie_id']
    
    aliases = generate_aliases_gemini(movie_title)
    
    alias_count = 0
    conn = get_db_connection()
    
    if conn and aliases:
        try:
            cur = conn.cursor()
            for alias in aliases:
                # Ensure alias is not too long
                if len(alias) > 255: continue
                
                try:
                    # ✅ FIX: SAVEPOINT create karein taaki ek error se pura loop na ruke
                    cur.execute("SAVEPOINT sp_alias")
                    
                    cur.execute(
                        "INSERT INTO movie_aliases (movie_id, alias) VALUES (%s, %s) ON CONFLICT (movie_id, alias) DO NOTHING",
                        (movie_id, alias)
                    )
                    
                    # Agar success hua to savepoint release karein
                    cur.execute("RELEASE SAVEPOINT sp_alias")
                    alias_count += 1
                    
                except Exception as inner_e:
                    # ❌ FIX: Agar error aaye to sirf is alias ko rollback karein
                    cur.execute("ROLLBACK TO SAVEPOINT sp_alias")
                    logger.warning(f"Skipped alias '{alias}': {inner_e}")
                    
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error(f"Error saving aliases: {e}")
            if conn: conn.rollback()
        finally:
            close_db_connection(conn)

    # 2. Final Report
    report = (
        f"🎉 **Batch Completed Successfully!**\n\n"
        f"🎬 **Movie:** `{movie_title}`\n"
        f"📂 **Files Saved:** {BATCH_SESSION['file_count']}\n"
        f"🤖 **AI Aliases Added:** {alias_count}\n\n"
        f"✅ *All backups secured across {len(get_storage_channels())} channels.*"
    )

    await status_msg.edit_text(report, parse_mode='Markdown')
    
    # Reset Session
    BATCH_SESSION['active'] = False
    BATCH_SESSION['movie_id'] = None
    BATCH_SESSION['movie_title'] = None
    BATCH_SESSION['file_count'] = 0
    BATCH_SESSION['admin_id'] = None

# 👇👇👇 FIXED 3-BOT FUNCTION 👇👇👇

async def admin_post_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ FIXED: Smart Post Generator with proper error handling
    """
    try:
        user_id = update.effective_user.id
        if user_id != ADMIN_USER_ID:
            return

        message = update.message
        
        # 1. Check Media
        if not (message.photo or message.video):
            await message.reply_text("❌ Photo ya Video bhejo caption ke sath")
            return

        caption_text = message.caption or ""
        if not caption_text.startswith('/post_query'):
            return

        # 2. Extract Media
        file_id = None
        media_type = 'photo'

        if message.photo:
            file_id = message.photo[-1].file_id
            media_type = 'photo'
        elif message.video:
            file_id = message.video.file_id
            media_type = 'video'

        # 3. Parse Query
        raw_input = caption_text.replace('/post_query', '').strip()
        
        if ',' in raw_input:
            parts = raw_input.split(',', 1)
            query_text = parts[0].strip()
            custom_msg = parts[1].strip()
        else:
            query_text = raw_input
            custom_msg = ""

        if not query_text:
            await message.reply_text("❌ Movie name missing")
            return

        # 4. Find Movie in DB
        movie_id = None
        conn = get_db_connection()

        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id FROM movies WHERE title ILIKE %s LIMIT 1",
                    (f"%{query_text}%",)
                )
                row = cur.fetchone()
                movie_id = row[0] if row else None
                cur.close()
            except Exception as e:
                logger.error(f"DB Error: {e}")
            finally:
                close_db_connection(conn)

        # 5. Generate Links
        bot1 = "FlimfyBox_SearchBot"
        bot2 = "urmoviebot"
        bot3 = "FlimfyBox_Bot"
        
        link_param = f"movie_{movie_id}" if movie_id else f"q_{query_text.replace(' ', '_')}"

        link1 = f"https://t.me/{bot1}?start={link_param}"
        link2 = f"https://t.me/{bot2}?start={link_param}"
        link3 = f"https://t.me/{bot3}?start={link_param}"

        # 6. Build Keyboard
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Download Now", url=link1),
                InlineKeyboardButton("Download Now", url=link2),
            ],
            [InlineKeyboardButton("Download Now", url=link3)],
            [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
        ])

        # 7. Build Caption
        channel_caption = f"🎬 <b>{query_text}</b>\n"
        if custom_msg:
            channel_caption += f"✨ <b>{custom_msg}</b>\n\n"
        else:
            channel_caption += "\n"
        
        channel_caption += (
            "➖➖➖➖➖➖➖\n"
            f"<b>Support:</b> <a href='https://t.me/+2hFeRL4DYfBjZDQ1'>Join Chat</a>\n"
            "➖➖➖➖➖➖➖\n"
            "<b>👇 Download Below</b>"
        )

        # 8. Send to Channels
        channels_str = os.environ.get('BROADCAST_CHANNELS', '')
        target_channels = [ch.strip() for ch in channels_str.split(',') if ch.strip()]

        if not target_channels:
            await message.reply_text("❌ No BROADCAST_CHANNELS configured in .env")
            return

        sent_count = 0
        failed_list = []

        for chat_id_str in target_channels:
            try:
                # ✅ FIXED: Parse channel ID properly
                try:
                    chat_id = int(chat_id_str)
                except ValueError:
                    failed_list.append(f"Invalid ID: {chat_id_str}")
                    continue

                logger.info(f"📤 Sending to {chat_id}...")

                sent_msg = None

                if media_type == 'video':
                    sent_msg = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_id,
                        caption=channel_caption,
                        reply_markup=keyboard,
                        parse_mode='HTML'
                    )
                else:
                    sent_msg = await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=file_id,
                        caption=channel_caption,
                        reply_markup=keyboard,
                        parse_mode='HTML'
                    )

                if sent_msg:
                    logger.info(f"✅ Sent to {chat_id}, Message ID: {sent_msg.message_id}")
                    sent_count += 1

            except telegram.error.BadRequest as e:
                error = str(e)
                if "group is deactivated" in error or "not found" in error:
                    failed_list.append(f"{chat_id_str}: Channel inactive/deleted")
                else:
                    failed_list.append(f"{chat_id_str}: {error}")
                logger.error(f"BadRequest for {chat_id_str}: {e}")
                
            except telegram.error.Forbidden as e:
                failed_list.append(f"{chat_id_str}: Bot blocked/no access")
                logger.error(f"Forbidden for {chat_id_str}: {e}")
                
            except Exception as e:
                failed_list.append(f"{chat_id_str}: {str(e)[:30]}")
                logger.error(f"Error sending to {chat_id_str}: {e}")

        # 9. Final Report
        report = f"""✅ <b>Post Processed ({media_type.capitalize()})</b>

📤 <b>Sent:</b> {sent_count}/{len(target_channels)}
❌ <b>Failed:</b> {len(failed_list)}

🎬 <b>Movie:</b> {query_text}
📝 <b>Extra:</b> {custom_msg or 'None'}"""

        if failed_list:
            report += "\n\n<b>Errors:</b>\n"
            for err in failed_list[:3]:  # Show first 3 errors
                report += f"• {err}\n"

        await message.reply_text(report, parse_mode='HTML')

    except Exception as e:
        logger.error(f"Critical error in post_query: {e}", exc_info=True)
        await message.reply_text(f"❌ Error: {str(e)[:100]}")

# ==================== ADMIN COMMANDS ====================
async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to add a movie manually (Supports Unreleased)"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, sirf 𝑶𝒘𝒏𝒆𝒓 hi is command ka istemal kar sakte hain.")
        return

    conn = None
    try:
        parts = context.args
        if len(parts) < 2:
            await update.message.reply_text("Galat Format! Aise use karein:\n/addmovie MovieName Link/FileID/unreleased")
            return

        value = parts[-1]  # Last part is link/id/unreleased
        title = " ".join(parts[:-1]) # Rest is title

        logger.info(f"Adding movie: {title} with value: {value}")

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        # CASE 1: UNRELEASED MOVIE
        if value.strip().lower() == "unreleased":
            # is_unreleased = TRUE set karenge
            cur.execute(
                """
                INSERT INTO movies (title, url, file_id, is_unreleased) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (title) DO UPDATE SET 
                    is_unreleased = EXCLUDED.is_unreleased,
                    url = '', 
                    file_id = NULL
                """,
                (title.strip(), "", None, True)
            )
            message = f"✅ '{title}' ko successfully **Unreleased** mark kar diya gaya hai. (Cute message activate ho gaya ✨)"

        # CASE 2: TELEGRAM FILE ID
        elif any(value.startswith(prefix) for prefix in ["BQAC", "BAAC", "CAAC", "AQAC"]):
            cur.execute(
                """
                INSERT INTO movies (title, url, file_id, is_unreleased) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (title) DO UPDATE SET 
                    url = EXCLUDED.url, 
                    file_id = EXCLUDED.file_id,
                    is_unreleased = FALSE
                """,
                (title.strip(), "", value.strip(), False)
            )
            message = f"✅ '{title}' ko File ID ke sath add kar diya gaya hai."

        # CASE 3: URL LINK
        elif "http" in value or "." in value:
            normalized_url = value.strip()
            if not value.startswith(('http://', 'https://')):
                await update.message.reply_text("❌ Invalid URL format. URL must start with http:// or https://")
                return

            cur.execute(
                """
                INSERT INTO movies (title, url, file_id, is_unreleased) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (title) DO UPDATE SET 
                    url = EXCLUDED.url, 
                    file_id = NULL,
                    is_unreleased = FALSE
                """,
                (title.strip(), normalized_url, None, False)
            )
            message = f"✅ '{title}' ko URL ke sath add kar diya gaya hai."

        else:
            await update.message.reply_text("❌ Invalid format. Please provide valid File ID, URL, or type 'unreleased'.")
            return

        conn.commit()
        await update.message.reply_text(message)

        # Notify Users logic (Agar movie sach mein release hui hai to hi notify karein)
        if value.strip().lower() != "unreleased":
            cur.execute("SELECT id, title, url, file_id FROM movies WHERE title = %s", (title.strip(),))
            movie_found = cur.fetchone()

            if movie_found:
                movie_id, title, url, file_id = movie_found
                value_to_send = file_id if file_id else url

                num_notified = await notify_users_for_movie(context, title, value_to_send)
                # Group notification optional
                # await notify_in_group(context, title)
                await update.message.reply_text(f"📢 Notification: {num_notified} users notified.")

    except Exception as e:
        logger.error(f"Error in add_movie command: {e}")
        await update.message.reply_text(f"Ek error aaya: {e}")
    finally:
        if conn:
            close_db_connection(conn)

async def update_buttons_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (Authentication check wahi rahega) ...

    status_msg = await update.message.reply_text("🚀 **Safe Update Mode On...**\nStarting to fix buttons slowly to avoid ban.")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT movie_id, channel_id, message_id FROM channel_posts WHERE bot_username = %s", (old_bot,))
    posts = cur.fetchall()
    
    total = len(posts)
    success = 0
    
    for i, (m_id, ch_id, msg_id) in enumerate(posts):
        try:
            # 1. Logic to make new link (Same as before)
            link_param = f"movie_{m_id}"
            new_link = f"https://t.me/{new_bot}?start={link_param}"
            
            new_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Download Server 1", url=new_link)],
                [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
            ])

            # 2. EDIT COMMAND (With Safety)
            await context.bot.edit_message_reply_markup(
                chat_id=ch_id,
                message_id=msg_id,
                reply_markup=new_keyboard
            )
            
            success += 1
            
            # ✅ SAFETY DELAY 1: Har edit ke baad 3 second ruko
            # Isse Telegram ko lagega koi insaan dheere-dheere edit kar raha hai
            await asyncio.sleep(3) 

            # ✅ SAFETY DELAY 2: Har 50 posts ke baad thoda lamba break (10 seconds)
            if success % 50 == 0:
                await asyncio.sleep(10)
                await status_msg.edit_text(f"☕ **Taking a coffee break...**\nUpdated: {success}/{total}")

        except RetryAfter as e:
            # 🚨 MOST IMPORTANT: Agar Telegram bole "Ruko", to hum rukenge
            wait_time = e.retry_after + 5 # Jitna Telegram bole usse 5 sec zyada ruko
            logger.warning(f"⚠️ FloodWait triggered! Sleeping for {wait_time} seconds...")
            await status_msg.edit_text(f"⚠️ Telegram asked to wait. Sleeping for {wait_time}s...")
            
            await asyncio.sleep(wait_time)
            
            # Retry logic yahan laga sakte ho ya skip kar sakte ho
            continue 

        except TelegramError as e:
            if "Message to edit not found" in str(e):
                # Delete from DB if message doesn't exist
                cur.execute("DELETE FROM channel_posts WHERE channel_id = %s AND message_id = %s", (ch_id, msg_id))
                conn.commit()
            else:
                logger.error(f"Error editing {msg_id}: {e}")

    await status_msg.edit_text(f"✅ **Mission Accomplished!**\nUpdated {success} posts safely.")

async def bulk_add_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add multiple movies at once"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return

    try:
        full_text = update.message.text
        lines = full_text.split('\n')

        if len(lines) <= 1 and not context.args:
            await update.message.reply_text("""
गलत फॉर्मेट! ऐसे इस्तेमाल करें:

/bulkadd
Movie1 https://link1.com
Movie2 https://link2.com
Movie3 file_id_here
""")
            return

        success_count = 0
        failed_count = 0
        results = []

        for line in lines:
            line = line.strip()
            if not line or line.startswith('/bulkadd'):
                continue

            parts = line.split()
            if len(parts) < 2:
                failed_count += 1
                results.append(f"❌ Invalid line format: {line}")
                continue

            url_or_id = parts[-1]
            title = ' '.join(parts[:-1])

            try:
                conn = get_db_connection()
                if not conn:
                    failed_count += 1
                    results.append(f"❌ {title} - Database connection failed")
                    continue

                cur = conn.cursor()

                if any(url_or_id.startswith(prefix) for prefix in ["BQAC", "BAAC", "CAAC", "AQAC"]):
                    cur.execute(
                        "INSERT INTO movies (title, url, file_id) VALUES (%s, %s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url, file_id = EXCLUDED.file_id",
                        (title.strip(), "", url_or_id.strip())
                    )
                else:
                    normalized_url = normalize_url(url_or_id)
                    cur.execute(
                        "INSERT INTO movies (title, url, file_id) VALUES (%s, %s, NULL) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url, file_id = NULL",
                        (title.strip(), normalized_url.strip())
                    )

                conn.commit()
                close_db_connection(conn)

                success_count += 1
                results.append(f"✅ {title}")
            except Exception as e:
                failed_count += 1
                results.append(f"❌ {title} - Error: {str(e)}")

        result_message = f"""
📊 Bulk Add Results:

Successfully added: {success_count}
Failed: {failed_count}

Details:
""" + "\n".join(results[:10])

        if len(results) > 10:
            result_message += f"\n\n... और {len(results) - 10} more items"

        await update.message.reply_text(result_message)

    except Exception as e:
        logger.error(f"Error in bulk_add_movies: {e}")
        await update.message.reply_text(f"Bulk add में error: {e}")

async def add_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add an alias for an existing movie"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return

    conn = None
    try:
        if not context.args or len(context.args) < 2:
            await update.message.reply_text("गलत फॉर्मेट! ऐसे इस्तेमाल करें:\n/addalias मूवी_का_असली_नाम alias_name")
            return

        parts = context.args
        alias = parts[-1]
        movie_title = " ".join(parts[:-1])

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        cur.execute("SELECT id FROM movies WHERE title = %s", (movie_title,))
        movie = cur.fetchone()

        if not movie:
            await update.message.reply_text(f"❌ '{movie_title}' डेटाबेस में नहीं मिली। पहले मूवी को add करें।")
            return

        movie_id = movie

        cur.execute(
            "INSERT INTO movie_aliases (movie_id, alias) VALUES (%s, %s) ON CONFLICT (movie_id, alias) DO NOTHING",
            (movie_id, alias.lower())
        )

        conn.commit()
        await update.message.reply_text(f"✅ Alias '{alias}' successfully added for '{movie_title}'")

    except Exception as e:
        logger.error(f"Error adding alias: {e}")
        await update.message.reply_text(f"Error: {e}")
    finally:
        if conn:
            close_db_connection(conn)

async def list_aliases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all aliases for a movie"""
    conn = None
    try:
        if not context.args:
            await update.message.reply_text("कृपया मूवी का नाम दें:\n/aliases मूवी_का_नाम")
            return

        movie_title = " ".join(context.args)

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        cur.execute("""
            SELECT m.title, COALESCE(array_agg(ma.alias), '{}'::text[])
            FROM movies m
            LEFT JOIN movie_aliases ma ON m.id = ma.movie_id
            WHERE m.title = %s
            GROUP BY m.title
        """, (movie_title,))

        result = cur.fetchone()

        if not result:
            await update.message.reply_text(f"'{movie_title}' डेटाबेस में नहीं मिली।")
            return

        title, aliases = result
        aliases_list = "\n".join(f"- {alias}" for alias in aliases) if aliases else "कोई aliases नहीं हैं"

        await update.message.reply_text(f"🎬 **{title}**\n\n**Aliases:**\n{aliases_list}", parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error listing aliases: {e}")
        await update.message.reply_text(f"Error: {e}")
    finally:
        if conn:
            close_db_connection(conn)
async def bulk_add_aliases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add multiple aliases at once"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return

    conn = None
    try:
        full_text = update.message.text
        lines = full_text.split('\n')

        if len(lines) <= 1 and not context.args:
            await update.message.reply_text("""
गलत फॉर्मेट! ऐसे इस्तेमाल करें:

/aliasbulk
Movie1: alias1, alias2, alias3
Movie2: alias4, alias5
""")
            return

        success_count = 0
        failed_count = 0

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        for line in lines:
            line = line.strip()
            if not line or line.startswith('/aliasbulk'):
                continue

            if ':' not in line:
                continue

            movie_title, aliases_str = line.split(':', 1)
            movie_title = movie_title.strip()
            aliases = [alias.strip() for alias in aliases_str.split(',') if alias.strip()]

            cur.execute("SELECT id FROM movies WHERE title = %s", (movie_title,))
            movie = cur.fetchone()

            if not movie:
                failed_count += len(aliases)
                continue

            movie_id = movie

            for alias in aliases:
                try:
                    cur.execute(
                        "INSERT INTO movie_aliases (movie_id, alias) VALUES (%s, %s) ON CONFLICT (movie_id, alias) DO NOTHING",
                        (movie_id, alias.lower())
                    )
                    success_count += 1
                except:
                    failed_count += 1

        conn.commit()

        await update.message.reply_text(f"""
📊 Alias Bulk Add Results:

Successfully added: {success_count}
Failed: {failed_count}
""")

    except Exception as e:
        logger.error(f"Error in bulk alias add: {e}")
        await update.message.reply_text(f"Error: {e}")
    finally:
        if conn:
            close_db_connection(conn)

async def notify_manually(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually notify users about a movie"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return

    try:
        if not context.args:
            await update.message.reply_text("Usage: /notify <movie_title>")
            return

        movie_title = " ".join(context.args)

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute("SELECT id, title, url, file_id FROM movies WHERE title ILIKE %s LIMIT 1", (f'%{movie_title}%',))
        movie_found = cur.fetchone()
        cur.close()
        close_db_connection(conn)

        if movie_found:
            movie_id, title, url, file_id = movie_found
            value_to_send = file_id if file_id else url
            num_notified = await notify_users_for_movie(context, title, value_to_send)
            await notify_in_group(context, title)
            await update.message.reply_text(f"{num_notified} users को '{title}' के लिए notify किया गया है।")
        else:
            await update.message.reply_text(f"'{movie_title}' डेटाबेस में नहीं मिली।")
    except Exception as e:
        logger.error(f"Error in notify_manually: {e}")
        await update.message.reply_text(f"एक एरर आया: {e}")

async def notify_user_by_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send text notification to specific user"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        if not context.args or len(context.args) < 2:
            await update.message.reply_text("Usage: /notifyuser @username Your message here")
            return

        target_username = context.args[0].replace('@', '')
        message_text = ' '.join(context.args[1:])

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT user_id, first_name FROM user_requests WHERE username ILIKE %s LIMIT 1",
            (target_username,)
        )
        user = cur.fetchone()

        if not user:
            await update.message.reply_text(f"❌ User `@{target_username}` not found in database.", parse_mode='Markdown')
            cur.close()
            close_db_connection(conn)
            return

        user_id, first_name = user

        await context.bot.send_message(
            chat_id=user_id,
            text=message_text
        )

        await update.message.reply_text(f"✅ Message sent to `@{target_username}` ({first_name})", parse_mode='Markdown')

        cur.close()
        close_db_connection(conn)

    except telegram.error.Forbidden:
        await update.message.reply_text(f"❌ User blocked the bot.")
    except Exception as e:
        logger.error(f"Error in notify_user_by_username: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast HTML message to all users with formatting support"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        # Command ke baad wala pura text (Formatting ke sath)
        if not context.args:
            await update.message.reply_text("Usage: /broadcast <b>Message Title</b>\n\nYour formatted text here...")
            return

        # Pure message ko extract karein
        message_text = update.message.text.replace('/broadcast', '').strip()

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute("SELECT DISTINCT user_id FROM user_requests")
        all_users = cur.fetchall()

        if not all_users:
            await update.message.reply_text("No users found in database.")
            cur.close()
            close_db_connection(conn)
            return

        status_msg = await update.message.reply_text(f"📤 Broadcasting to {len(all_users)} users...\n⏳ Please wait...")

        success_count = 0
        failed_count = 0

        for user_id_tuple in all_users:
            user_id = user_id_tuple[0]
            try:
                # 📢 YAHAN PAR 'HTML' USE HOGA
                await context.bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode='HTML',  # Isse Enter aur Bold kaam karega
                    disable_web_page_preview=True
                )
                success_count += 1
                await asyncio.sleep(0.05) # Flood protection
            except telegram.error.Forbidden:
                failed_count += 1
            except Exception as e:
                failed_count += 1

        await status_msg.edit_text(
            f"📊 <b>Broadcast Complete</b>\n\n"
            f"✅ Sent: {success_count}\n"
            f"❌ Failed: {failed_count}",
            parse_mode='HTML'
        )

        cur.close()
        close_db_connection(conn)

    except Exception as e:
        logger.error(f"Error in broadcast_message: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def schedule_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Schedule a notification for later"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "Usage: /schedulenotify <minutes> <@username> <message>\n"
                "Example: /schedulenotify 30 @john New movie arriving soon!"
            )
            return

        delay_minutes = int(context.args[0])
        target_username = context.args[1].replace('@', '')
        message_text = ' '.join(context.args[2:])

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT user_id, first_name FROM user_requests WHERE username ILIKE %s LIMIT 1",
            (target_username,)
        )
        user = cur.fetchone()

        if not user:
            await update.message.reply_text(f"❌ User `@{target_username}` not found.", parse_mode='Markdown')
            cur.close()
            close_db_connection(conn)
            return

        user_id, first_name = user

        async def send_scheduled_notification():
            await asyncio.sleep(delay_minutes * 60)
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=message_text
                )
                logger.info(f"Scheduled notification sent to {user_id}")
            except Exception as e:
                logger.error(f"Failed to send scheduled notification to {user_id}: {e}")

        asyncio.create_task(send_scheduled_notification())

        await update.message.reply_text(
            f"⏰ Notification scheduled!\n\n"
            f"To: `@{target_username}` ({first_name})\n"
            f"Delay: {delay_minutes} minutes\n"
            f"Message: {message_text[:50]}...",
            parse_mode='Markdown'
        )

        cur.close()
        close_db_connection(conn)

    except ValueError:
        await update.message.reply_text("❌ Invalid delay. Please provide number of minutes.")
    except Exception as e:
        logger.error(f"Error in schedule_notification: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def notify_user_with_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Notify user with media by replying to a message"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        if not update.message.reply_to_message:
            await update.message.reply_text(
                "❌ Please reply to a message (file/video/audio/photo) with:\n"
                "/notifyuserwithmedia @username Optional message"
            )
            return

        if not context.args:
            await update.message.reply_text(
                "Usage: /notifyuserwithmedia @username [optional message]\n"
                "Example: /notifyuserwithmedia @amit002 Here's your requested movie!"
            )
            return

        target_username = context.args[0].replace('@', '')
        optional_message = ' '.join(context.args[1:]) if len(context.args) > 1 else None

        replied_message = update.message.reply_to_message

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT user_id, first_name FROM user_requests WHERE username ILIKE %s LIMIT 1",
            (target_username,)
        )
        user = cur.fetchone()

        if not user:
            await update.message.reply_text(f"❌ User `@{target_username}` not found in database.", parse_mode='Markdown')
            cur.close()
            close_db_connection(conn)
            return

        user_id, first_name = user

        notification_header = ""
        if optional_message:
            notification_header = optional_message

        warning_msg = await context.bot.send_message(
            chat_id=user_id,
            text="ᯓ➤This file automatically❕️deletes after 1 minute❕️so please forward it to another chat જ⁀➴",
            parse_mode='Markdown'
        )

        sent_msg = None
        media_type = "unknown"
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

        if replied_message.document:
            media_type = "file"
            sent_msg = await context.bot.send_document(
                chat_id=user_id,
                document=replied_message.document.file_id,
                caption=notification_header if notification_header else None,
                reply_markup=join_keyboard
            )
        elif replied_message.video:
            media_type = "video"
            sent_msg = await context.bot.send_video(
                chat_id=user_id,
                video=replied_message.video.file_id,
                caption=notification_header if notification_header else None,
                reply_markup=join_keyboard
            )
        elif replied_message.audio:
            media_type = "audio"
            sent_msg = await context.bot.send_audio(
                chat_id=user_id,
                audio=replied_message.audio.file_id,
                caption=notification_header if notification_header else None,
                reply_markup=join_keyboard
            )
        elif replied_message.photo:
            media_type = "photo"
            photo = replied_message.photo[-1]
            sent_msg = await context.bot.send_photo(
                chat_id=user_id,
                photo=photo.file_id,
                caption=notification_header if notification_header else None,
                reply_markup=join_keyboard
            )
        if sent_msg:
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                # Hum save kar rahe hain ki is movie ka post is channel me is ID par hai
                cur.execute(
                    "INSERT INTO channel_posts (movie_id, channel_id, message_id, bot_username) VALUES (%s, %s, %s, %s)",
                    (movie_id, chat_id, sent_msg.message_id, "FlimfyBox_Bot") # Current Main Bot Username
                )
                conn.commit()
                cur.close()
                close_db_connection(conn)
            except Exception as e:
                logger.error(f"Failed to save post ID: {e}")
        
        elif replied_message.text:
            media_type = "text"
            text_to_send = replied_message.text
            if optional_message:
                text_to_send = f"{optional_message}\n\n{text_to_send}"
            sent_msg = await context.bot.send_message(
                chat_id=user_id,
                text=text_to_send
            )
        else:
            await update.message.reply_text("❌ Unsupported media type.")
            cur.close()
            close_db_connection(conn)
            return

        if sent_msg and media_type != "text":
            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    user_id,
                    [sent_msg.message_id, warning_msg.message_id],
                    60
                )
            )

        confirmation = f"✅ **Notification Sent!**\n\n"
        confirmation += f"To: `@{target_username}` ({first_name})\n"
        confirmation += f"Media Type: {media_type.capitalize()}"

        await update.message.reply_text(confirmation, parse_mode='Markdown')

        cur.close()
        close_db_connection(conn)

    except telegram.error.Forbidden:
        await update.message.reply_text(f"❌ User blocked the bot.")
    except Exception as e:
        logger.error(f"Error in notify_user_with_media: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def broadcast_with_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast media to all users"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Please reply to a media message to broadcast it.")
        return

    try:
        optional_message = ' '.join(context.args) if context.args else None

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute("SELECT DISTINCT user_id, first_name, username FROM user_requests")
        all_users = cur.fetchall()

        if not all_users:
            await update.message.reply_text("No users found in database.")
            cur.close()
            close_db_connection(conn)
            return

        status_msg = await update.message.reply_text(
            f"📤 Broadcasting media to {len(all_users)} users...\n⏳ Please wait..."
        )

        success_count = 0
        failed_count = 0
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

        for user_id, first_name, username in all_users:
            try:
                if optional_message:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=optional_message
                    )

                if replied_message.document:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=replied_message.document.file_id,
                        reply_markup=join_keyboard
                    )
                elif replied_message.video:
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=replied_message.video.file_id,
                        reply_markup=join_keyboard
                    )
                elif replied_message.audio:
                    await context.bot.send_audio(
                        chat_id=user_id,
                        audio=replied_message.audio.file_id,
                        reply_markup=join_keyboard
                    )
                elif replied_message.photo:
                    photo = replied_message.photo[-1]
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=photo.file_id,
                        reply_markup=join_keyboard
                    )

                success_count += 1
                await asyncio.sleep(0.1)

            except telegram.error.Forbidden:
                failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.error(f"Failed broadcast to {user_id}: {e}")

        await status_msg.edit_text(
            f"📊 **Broadcast Complete**\n\n"
            f"✅ Sent: {success_count}\n"
            f"❌ Failed: {failed_count}\n"
            f"📝 Total: {len(all_users)}"
        )

        cur.close()
        close_db_connection(conn)

    except Exception as e:
        logger.error(f"Error in broadcast_with_media: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def quick_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Quick notify - sends media to specific requesters"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Reply to a media message first!")
        return

    if not context.args:
        await update.message.reply_text("Usage: /qnotify <@username | MovieTitle>")
        return

    try:
        query = ' '.join(context.args)

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        target_users = []

        if query.startswith('@'):
            username = query.replace('@', '')
            cur.execute(
                "SELECT DISTINCT user_id, first_name, username FROM user_requests WHERE username ILIKE %s",
                (username,)
            )
            target_users = cur.fetchall()
        else:
            cur.execute(
                "SELECT DISTINCT user_id, first_name, username FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
                (f'%{query}%',)
            )
            target_users = cur.fetchall()

        if not target_users:
            await update.message.reply_text(f"❌ No users found for '{query}'")
            cur.close()
            close_db_connection(conn)
            return

        success_count = 0
        failed_count = 0
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

        for user_id, first_name, username in target_users:
            try:
                caption = f"🎬 {query}" if not query.startswith('@') else None
                if replied_message.document:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=replied_message.document.file_id,
                        caption=caption,
                        reply_markup=join_keyboard
                    )
                elif replied_message.video:
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=replied_message.video.file_id,
                        caption=caption,
                        reply_markup=join_keyboard
                    )

                success_count += 1

                if not query.startswith('@'):
                    cur.execute(
                        "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                        (user_id, f'%{query}%')
                    )
                    conn.commit()

                await asyncio.sleep(0.1)

            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to send to {user_id}: {e}")

        await update.message.reply_text(
            f"✅ Sent to {success_count} user(s)\n"
            f"❌ Failed for {failed_count} user(s)\n"
            f"Query: {query}"
        )

        cur.close()
        close_db_connection(conn)

    except Exception as e:
        logger.error(f"Error in quick_notify: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def forward_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Forward message from channel to user"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Reply to a message first!")
        return

    if not context.args:
        await update.message.reply_text("Usage: /forwardto @username_or_userid")
        return

    try:
        target_username = context.args[0].replace('@', '')

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT user_id, first_name FROM user_requests WHERE username ILIKE %s LIMIT 1",
            (target_username,)
        )
        user = cur.fetchone()

        if not user:
            await update.message.reply_text(f"❌ User `@{target_username}` not found.", parse_mode='Markdown')
            cur.close()
            close_db_connection(conn)
            return

        user_id, first_name = user

        await replied_message.forward(chat_id=user_id)

        await update.message.reply_text(f"✅ Forwarded to `@{target_username}` ({first_name})", parse_mode='Markdown')

        cur.close()
        close_db_connection(conn)

    except Exception as e:
        logger.error(f"Error in forward_to_user: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def get_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get user information"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /userinfo @username")
        return

    try:
        target_username = context.args[0].replace('@', '')

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        cur.execute("""
            SELECT
                user_id,
                username,
                first_name,
                COUNT(*) as total_requests,
                SUM(CASE WHEN notified = TRUE THEN 1 ELSE 0 END) as fulfilled,
                MAX(requested_at) as last_request
            FROM user_requests
            WHERE username ILIKE %s
            GROUP BY user_id, username, first_name
        """, (target_username,))

        user_info = cur.fetchone()

        if not user_info:
            await update.message.reply_text(f"❌ No data found for `@{target_username}`", parse_mode='Markdown')
            cur.close()
            close_db_connection(conn)
            return

        user_id, username, first_name, total, fulfilled, last_request = user_info
        fulfilled = fulfilled or 0

        cur.execute("""
            SELECT movie_title, requested_at, notified
            FROM user_requests
            WHERE user_id = %s
            ORDER BY requested_at DESC
            LIMIT 5
        """, (user_id,))
        recent_requests = cur.fetchall()

        username_str = f"`@{username}`" if username else "N/A"

        info_text = f"""
👤 **User Information**

**Basic Info:**
• Name: {first_name}
• Username: {username_str}
• User ID: `{user_id}`

**Statistics:**
• Total Requests: {total}
• Fulfilled: {fulfilled}
• Pending: {total - fulfilled}
• Last Request: {last_request.strftime('%Y-%m-%d %H:%M') if last_request else 'N/A'}

**Recent Requests:**
"""

        if recent_requests:
            for movie, req_time, notified in recent_requests:
                status = "✅" if notified else "⏳"
                info_text += f"{status} {movie} - {req_time.strftime('%m/%d %H:%M')}\n"
        else:
            info_text += "No recent requests."

        await update.message.reply_text(info_text, parse_mode='Markdown')

        cur.close()
        close_db_connection(conn)

    except Exception as e:
        logger.error(f"Error in get_user_info: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def list_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all bot users with Accurate Count from Activity Log"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        page = 1
        if context.args and context.args[0].isdigit():
            page = int(context.args[0])

        per_page = 10
        offset = (page - 1) * per_page

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        # 1. ✅ REAL TOTAL COUNT (From user_activity table)
        # Ye un sabhi unique users ko ginega jinhone kabhi bhi bot use kiya hai
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_activity")
        result = cur.fetchone()
        total_users = result[0] if result else 0

        # 2. GET LIST (From user_requests table because it has Names)
        # Note: List mein shayad kam log dikhein (sirf wo jinhone request kiya hai), 
        # lekin uppar Total Count sahi dikhega.
        cur.execute("""
            SELECT 
                user_id, 
                username, 
                first_name, 
                COUNT(*) as requests, 
                MAX(requested_at) as last_seen
            FROM user_requests 
            GROUP BY user_id, username, first_name 
            ORDER BY MAX(requested_at) DESC 
            LIMIT %s OFFSET %s
        """, (per_page, offset))

        users = cur.fetchall()

        # Calculate pages based on the list available (user_requests)
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_requests")
        listable_users = cur.fetchone()[0]
        total_pages = (listable_users + per_page - 1) // per_page if listable_users > 0 else 1

        users_text = f"👥 **Bot Users** (Page {page}/{total_pages})\n"
        users_text += f"📊 **Total Unique Users: {total_users}**\n\n"

        if not users:
            users_text += "No active requesters found on this page."
        else:
            for idx, (user_id, username, first_name, req_count, last_seen) in enumerate(users, start=offset+1):
                username_str = f"`@{username}`" if username else "N/A"
                safe_name = (first_name or "Unknown").replace("<", "&lt;").replace(">", "&gt;")
                
                users_text += f"{idx}. <b>{safe_name}</b> ({username_str})\n"
                users_text += f"   🆔 `{user_id}` | 📥 Reqs: {req_count}\n"
                users_text += f"   🕒 {last_seen.strftime('%Y-%m-%d %H:%M')}\n\n"

        if total_users > listable_users:
            users_text += f"\n⚠️ *Note:* {total_users - listable_users} users ne bot use kiya hai par koi Request nahi bheji (isliye list me naam nahi hai)."

        await update.message.reply_text(users_text, parse_mode='HTML')

        cur.close()
        close_db_connection(conn)

    except Exception as e:
        logger.error(f"Error in list_all_users: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def get_bot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get comprehensive bot statistics"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    conn = None
    cur = None

    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM movies")
        total_movies = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_requests")
        total_users = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM user_requests")
        total_requests = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM user_requests WHERE notified = TRUE")
        fulfilled = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM user_requests WHERE DATE(requested_at) = CURRENT_DATE")
        today_requests = cur.fetchone()[0]

        cur.execute("""
            SELECT first_name, username, COUNT(*) as req_count
            FROM user_requests
            GROUP BY user_id, first_name, username
            ORDER BY req_count DESC
            LIMIT 5
        """)
        top_users = cur.fetchall()

        fulfillment_rate = (fulfilled / total_requests * 100) if total_requests > 0 else 0

        stats_text = f"""
📊 **Bot Statistics**

**Database:**
• Movies: {total_movies}
• Users: {total_users}
• Total Requests: {total_requests}
• Fulfilled: {fulfilled}
• Pending: {total_requests - fulfilled}

**Activity:**
• Today's Requests: {today_requests}
• Fulfillment Rate: {fulfillment_rate:.1f}%

**Top Requesters:**
"""
        if top_users:
            for name, username, count in top_users:
                username_str = f"`@{username}`" if username else "N/A"
                stats_text += f"• {name} ({username_str}): {count} requests\n"
        else:
            stats_text += "No user data available."
            
        await update.message.reply_text(stats_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in get_bot_stats: {e}")
        await update.message.reply_text(f"❌ Error while fetching stats: {e}")
        
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)

async def fix_missing_metadata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Magic Command: Finds movies with missing info and fixes them - UPDATED
    """
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ सिर्फ एडमिन के लिए!")
        return

    status_msg = await update.message.reply_text("⏳ **Scanning Database for incomplete movies...**", parse_mode='Markdown')

    conn = get_db_connection()
    if not conn:
        await status_msg.edit_text("❌ Database connection failed.")
        return

    try:
        cur = conn.cursor()
        # Find movies where ANY key info is missing (Genre, Poster, or Year)
        cur.execute("SELECT title FROM movies WHERE genre IS NULL OR poster_url IS NULL OR year IS NULL")
        movies_to_fix = cur.fetchall()
        
        if not movies_to_fix:
            await status_msg.edit_text("✅ **All Good!** Database mein sabhi movies ka metadata complete hai.")
            return

        total = len(movies_to_fix)
        await status_msg.edit_text(f"🧐 Found **{total}** movies to fix. Starting update process... (This may take time)")

        success_count = 0
        failed_count = 0

        for index, (title,) in enumerate(movies_to_fix):
            try:
                # Progress update every 10 movies
                if index % 10 == 0:
                    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

                # ✅ FETCH CORRECT METADATA (6 Values)
                metadata = fetch_movie_metadata(title)
                
                # Unpack carefully
                if metadata:
                    new_title, year, poster_url, genre, imdb_id, rating = metadata

                    # Only update if we found something useful
                    if genre or poster_url or year > 0:
                        # ✅ CORRECT SQL UPDATE QUERY (Order Matters!)
                        cur.execute("""
                            UPDATE movies 
                            SET genre = %s, 
                                poster_url = %s, 
                                year = %s, 
                                imdb_id = %s, 
                                rating = %s
                            WHERE title = %s
                        """, (genre, poster_url, year, imdb_id, rating, title))
                        
                        conn.commit()
                        success_count += 1
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
                
                # Sleep slightly to respect API limits
                await asyncio.sleep(0.5) 

            except Exception as e:
                # 🛑 ROLLBACK IS CRITICAL HERE
                if conn:
                    conn.rollback() 
                logger.error(f"Failed to fix {title}: {e}")
                failed_count += 1

        # Final Report
        await status_msg.edit_text(
            f"🎉 **Repair Complete!**\n\n"
            f"✅ Fixed: {success_count}\n"
            f"❌ Failed: {failed_count}\n"
            f"📊 Total Processed: {total}\n\n"
            f"Database updated successfully! 🚀",
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Error in fix_metadata: {e}")
        await status_msg.edit_text(f"❌ Error: {e}")
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)
async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show admin commands help"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    help_text = """
👑 **Admin Commands Guide**

**Media Notifications:**
• `/notifyuserwithmedia @user [msg]` - Reply to media + send to user
• `/qnotify <@user|MovieTitle>` - Quick notify (reply to media)
• `/forwardto @user` - Forward channel message (reply to msg)
• `/broadcastmedia [msg]` - Broadcast media to all (reply to media)

**Text Notifications:**
• `/notifyuser @user <msg>` - Send text message
• `/broadcast <msg>` - Text broadcast to all
• `/schedulenotify <min> @user <msg>` - Schedule notification

**User Management:**
• `/userinfo @username` - Get user stats
• `/listusers [page]` - List all users

**Movie Management:**
• `/addmovie <Title> <URL|FileID>` - Add movie
• `/bulkadd` - Bulk add movies (multi-line)
• `/addalias <Title> <alias>` - Add alias
• `/aliasbulk` - Bulk add aliases (multi-line)
• `/aliases <MovieTitle>` - List aliases
• `/notify <MovieTitle>` - Auto-notify requesters

**Stats & Help:**
• `/stats` - Bot statistics
• `/adminhelp` - This help message
"""

    await update.message.reply_text(help_text, parse_mode='Markdown')

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors and handle them gracefully"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

    if isinstance(update, Update) and update.effective_message:
        try:
            # ✅ IMPROVED: Show more helpful error message
            error_msg = str(context.error)
            if "too many values to unpack" in error_msg:
                await update.effective_message.reply_text(
                    "❌ Error:  Data format issue. Please try again.",
                    reply_markup=get_main_keyboard()
                )
            elif "unpacking" in error_msg:
                await update.effective_message.reply_text(
                    "❌ Error: Could not process your request. Please try again.",
                    reply_markup=get_main_keyboard()
                )
            else:
                await update.effective_message.reply_text(
                    "Sorry, something went wrong. Please try again later.",
                    reply_markup=get_main_keyboard()
                )
        except Exception as e:
            logger.error(f"Failed to send error message to user: {e}")

# ==================== FLASK APP ====================
flask_app = Flask('')

@flask_app.route('/')
def home():
    return "Bot is running!"

@flask_app.route('/health')
def health():
    return "OK", 200

@flask_app.route(f'/{UPDATE_SECRET_CODE}')
def trigger_update():
    result = update_movies_in_db()
    return result

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.secret_key = os.environ.get('FLASK_SECRET_KEY', None) or os.urandom(24)

    try:
        from admin_views import admin as admin_blueprint
        flask_app.register_blueprint(admin_blueprint)
        logger.info("Admin blueprint registered successfully.")
    except Exception as e:
        logger.error(f"Failed to register admin blueprint: {e}")

    flask_app.run(host='0.0.0.0', port=port)

# ==================== BATCH UPLOAD HANDLERS (OLD - TO BE REMOVED) ====================

# Note: Purane batch functions ko replace kar diya gaya hai naye multi-channel batch functions se
# Isliye ye functions delete kar diye gaye hain aur unki jagah naye functions upar add kiye gaye hain.

# ==================== NEW REQUEST SYSTEM (CONFIRMATION FLOW) ====================

async def start_request_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 1: User clicks 'Request This Movie' -> Show Short & Stylish Guidelines"""
    query = update.callback_query
    await query.answer()
    
    # --- NEW STYLISH & SHORT TEXT ---
    request_instruction_text = (
        "📝 𝗥𝗲𝗾𝘂𝗲𝘀𝘁 𝗥𝘂𝗹𝗲𝘀..!!\n\n"
        "बस मूवी/सीरीज़ का <b>असली नाम</b> लिखें।✔️\n\n"
        "फ़ालतू शब्द (Download, HD, Please) न लिखें।♻️\n\n"
        "<b><a href='https://www.google.com/'>𝗚𝗼𝗼𝗴𝗹𝗲</a></b> से सही स्पेलिंग चेक कर लें। ☜\n\n"
        "✐ᝰ𝗘𝘅𝗮𝗺𝗽𝗹𝗲\n\n"
        "सही है.!‼️    \n"
        "─────────────────────\n"
        "Animal ✔️ | Animal Movie Download ❌\n"
        "─────────────────────\n"
        "Mirzapur S03 ✔️ | Mirzapur New Season ❌\n"
        "─────────────────────\n\n"
        "👇 <b>अब नीचे मूवी का नाम भेजें:</b>"
    )
    
    # Message Edit karein
    await query.edit_message_text(
        text=request_instruction_text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )
    
    # Is instruction message ko bhi delete list me daal dein (2 min baad)
    track_message_for_deletion(context, update.effective_chat.id, query.message.message_id, 120)
    
    # State change -> Ab Bot sirf Name ka wait karega
    return WAITING_FOR_NAME

async def handle_request_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 2: User sends name -> Bot asks for Confirmation (Not saved yet)"""
    user_name_input = update.message.text.strip()
    chat_id = update.effective_chat.id
    
    # User ka message delete karne ke liye (Clean Chat)
    track_message_for_deletion(context, chat_id, update.message.message_id, 120)

    # Safety: Check if user tried to send a command or menu button
    if user_name_input.startswith('/') or user_name_input in ['🔍 Search Movies', '📊 My Stats', '❓ Help']:
        msg = await update.message.reply_text("❌ Request Process Cancelled. Back to menu.")
        track_message_for_deletion(context, chat_id, msg.message_id, 60)
        return ConversationHandler.END

    # Name ko temporary memory me rakho
    context.user_data['temp_request_name'] = user_name_input
    
    # Confirmation Keyboard (Yes/No)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Confirm", callback_data="confirm_yes"),
            InlineKeyboardButton("❌ No, Cancel", callback_data="confirm_no")
        ]
    ])
    
    msg = await update.message.reply_text(
        f"🔔 <b>Confirmation Required</b>\n\n"
        f"क्या आप <b>'{user_name_input}'</b> को रिक्वेस्ट करना चाहते हैं?\n\n"
        f"नाम सही है तो <b>Yes</b> दबाएं, नहीं तो <b>No</b> दबाकर दोबारा कोशिश करें।",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    # ⚡ Ye Confirmation message 60 seconds me delete ho jayega
    track_message_for_deletion(context, chat_id, msg.message_id, 60)
    
    return CONFIRMATION

async def handle_confirmation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 3: Handle Yes/No buttons"""
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    
    choice = query.data
    user = query.from_user
    
    if choice == "confirm_no":
        await query.edit_message_text("❌ Request Cancelled. आप दोबारा सर्च या रिक्वेस्ट कर सकते हैं।")
        # Cancel message auto delete in 10 seconds
        track_message_for_deletion(context, chat_id, query.message.message_id, 10)
        context.user_data.pop('temp_request_name', None)
        return ConversationHandler.END
        
    elif choice == "confirm_yes":
        movie_title = context.user_data.get('temp_request_name')
        
        # --- FINAL SAVE TO DATABASE ---
        stored = store_user_request(
            user.id,
            user.username,
            user.first_name,
            movie_title,
            query.message.chat.id if query.message.chat.type != "private" else None,
            query.message.message_id
        )
        
        if stored:
            # Notify Admin
            group_info = query.message.chat.title if query.message.chat.type != "private" else None
            await send_admin_notification(context, user, movie_title, group_info)
            
            success_text = f"""
✅ <b>Request Sent to Admin!</b>

🎬 Movie: <b>{movie_title}</b>

📝 आपकी रिक्वेस्ट 𝑶𝒘𝒏𝒆𝒓 <b>@ownermahi</b> / <b>@ownermahima</b> को मिली गई है।
⏳ जैसे ही मूवी उपलब्ध होगी, वो खुद आपको यहाँ सूचित (Notify) कर देंगे।

<i>हमसे जुड़े रहने के लिए धन्यवाद! 🙏</i>
            """
            await query.edit_message_text(success_text, parse_mode='HTML')
        else:
            await query.edit_message_text("❌ Error: Request save नहीं हो पाई। शायद यह पहले से पेंडिंग है।")
            
        # ⚡ Success Message Auto Delete (60 Seconds)
        track_message_for_deletion(context, chat_id, query.message.message_id, 60)
            
        context.user_data.pop('temp_request_name', None)
        return ConversationHandler.END

async def timeout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """2 Minute Timeout Handler"""
    if update.effective_message:
        msg = await update.effective_message.reply_text("⏳ <b>Session Expired:</b> रिक्वेस्ट का समय समाप्त हो गया।", parse_mode='HTML')
        track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 30)
    return ConversationHandler.END

async def main_menu_or_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    
    # Rename check
    if context.user_data.get('awaiting_draft_rename'):
        return await handle_draft_rename(update, context)
        
    # 👇👇👇 IMDb ID Check 👇👇👇
    if context.user_data.get('awaiting_draft_imdb'):
        return await handle_draft_imdb(update, context)
    # 👆👆👆
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # === 1. FSub Check (Only in Private Chat) ===
    if update.effective_chat.type == "private":
        check = await is_user_member(context, user_id)
        if not check['is_member']:
            # ✅ NEW: User ne jo search kiya (e.g. "Kalki"), use SAVE kar lo
            if update.message and update.message.text:
                context.user_data['pending_search_query'] = update.message.text.strip()

            msg = await update.message.reply_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            track_message_for_deletion(context, chat_id, msg.message_id, 120)
            return
    # ============================================

    if not update.message or not update.message.text:
        return

    query_text = update.message.text.strip()
    
    # === 2. Menu Button Logic ===
    
    # Handle 'Search Movies' button (Guidance)
    if query_text == '🔍 Search Movies':
        msg = await update.message.reply_text("Great! Just type the name of the movie you want to search for.")
        track_message_for_deletion(context, chat_id, msg.message_id, 60)
        return

    # Handle 'Request Movie' button (Guidance)
    elif query_text == '🙋 Request Movie':
        msg = await update.message.reply_text("Click the button below to request:", reply_markup=get_main_keyboard())
        track_message_for_deletion(context, chat_id, msg.message_id, 60)
        return

    # === NEW: Handle 'Browse by Genre' button ===
    elif query_text == '📂 Browse by Genre':
        await show_genre_selection(update, context)
        return
    # ============================================

    # Handle 'My Stats' button
    elif query_text == '📊 My Stats':
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s", (user_id,))
                req = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s AND notified = TRUE", (user_id,))
                ful = cur.fetchone()[0]
                
                stats_msg = await update.message.reply_text(
                    f"📊 **Your Stats**\n\n"
                    f"📝 Total Requests: {req}\n"
                    f"✅ Fulfilled: {ful}",
                    parse_mode='Markdown'
                )
                track_message_for_deletion(context, chat_id, stats_msg.message_id, 120)
            except Exception as e:
                logger.error(f"Stats Error: {e}")
            finally:
                close_db_connection(conn)
        return

    # Handle 'Help' button
    elif query_text == '❓ Help':
        help_text = (
            "🤖 **How to use:**\n\n"
            "1. **Search:** Just type any movie name (e.g., 'Avengers').\n"
            "2. **Request:** If not found, use the Request button.\n"
            "3. **Download:** Click the buttons provided."
        )
        msg = await update.message.reply_text(help_text, parse_mode='Markdown')
        track_message_for_deletion(context, chat_id, msg.message_id, 120)
        return

    # === 3. If no button matched, Search for the Movie ===
    await search_movies(update, context)

# 👇👇👇 IS FUNCTION KO REPLACE KARO (Line ~1665) 👇👇👇

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle messages in groups using FAST SQL Search.
    Agar movie database me hai to reply karega, nahi to chup rahega.
    """
    if not update.message or not update.message.text:
        return
    
    text = update.message.text.strip()
    
    # 1. Commands ignore karo
    if text.startswith('/'):
        return
    
    # 2. Bahut chote words ignore karo
    if len(text) < 2:
        return

    # 3. 🚀 FAST SEARCH CALL (Sirf SQL Check)
    # Hum 5 results maang rahe hain taaki agar typos ho to best match mile
    movies = await run_async(get_movies_fast_sql, text, limit=5)

    if not movies:
        # 🤫 Agar movie nahi mili, to YAHIN RUK JAO.
        # Bot kuch reply nahi karega, group me shanti rahegi.
        return

    # 4. Results mil gaye, ab show karo
    context.user_data['search_results'] = movies
    context.user_data['search_query'] = text

    keyboard = create_movie_selection_keyboard(movies, page=0)
    
    # Reply to user
    msg = await update.message.reply_text(
        f"🎬 **Found {len(movies)} results for '{text}'**\n👇 Select movie:",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )
    
    # Auto-delete (Optional - 2 min)
    track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)

# ==================== MULTI-BOT SETUP (REPLACES OLD MAIN) ====================

def register_handlers(application: Application):
    """
    यह फंक्शन हर बॉट पर लॉजिक (Handlers) सेट करेगा।
    ताकि तीनों बॉट्स सेम काम करें।
    """
    # -----------------------------------------------------------
    # 1. NEW REQUEST SYSTEM HANDLER (With 2 Min Timeout)
    # -----------------------------------------------------------
    # नोट: ConversationHandler को हर बार नया बनाना जरूरी है
    request_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_request_flow, pattern="^request_")],
        states={
            WAITING_FOR_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_request_name_input)
            ],
            CONFIRMATION: [
                CallbackQueryHandler(handle_confirmation_callback, pattern="^confirm_")
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('start', start)
        ],
        conversation_timeout=120,
    )
    application.add_handler(request_conv_handler)

    # -----------------------------------------------------------
    # 2. GLOBAL HANDLERS
    # -----------------------------------------------------------
    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, main_menu_or_search))
    
    # Button Callback
    application.add_handler(CallbackQueryHandler(button_callback))

    # -----------------------------------------------------------
    # 3. ADMIN & BATCH COMMANDS
    # -----------------------------------------------------------
    application.add_handler(CommandHandler("addmovie", add_movie))
    application.add_handler(CommandHandler("bulkadd", bulk_add_movies))
    application.add_handler(CommandHandler("notify", notify_manually))
    application.add_handler(CommandHandler("addalias", add_alias))
    application.add_handler(CommandHandler("aliases", list_aliases))
    application.add_handler(CommandHandler("aliasbulk", bulk_add_aliases))
    application.add_handler(MessageHandler((filters.PHOTO | filters.VIDEO) & filters.CaptionRegex(r'^/post_query'), admin_post_query))
    application.add_handler(CommandHandler("fixbuttons", update_buttons_command))

    # Batch Commands
    application.add_handler(CommandHandler("batch", batch_add_command))
    application.add_handler(CommandHandler("done", batch_done_command))
    application.add_handler(CommandHandler("batchid", batch_id_command))
    application.add_handler(CommandHandler("fixdata", fix_missing_metadata))
    application.add_handler(CommandHandler("post", post_to_topic_command))

    # Listener handler (Documents/Video ke liye)
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.Document.ALL | filters.VIDEO | filters.TEXT), pm_file_listener))
    
    # PM Listener for Batch
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.Document.ALL | filters.VIDEO), pm_file_listener))

    # -----------------------------------------------------------
    # 4. GENRE & GROUP HANDLERS
    # -----------------------------------------------------------
    application.add_handler(CommandHandler("genres", show_genre_selection))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, handle_group_message))
      
    # -----------------------------------------------------------
    # 5. NOTIFICATION & STATS
    # -----------------------------------------------------------
    application.add_handler(CommandHandler("notifyuser", notify_user_by_username))
    application.add_handler(CommandHandler("broadcast", broadcast_message))
    application.add_handler(CommandHandler("schedulenotify", schedule_notification))
    application.add_handler(CommandHandler("notifyuserwithmedia", notify_user_with_media))
    application.add_handler(CommandHandler("qnotify", quick_notify))
    application.add_handler(CommandHandler("forwardto", forward_to_user))
    application.add_handler(CommandHandler("broadcastmedia", broadcast_with_media))

    application.add_handler(CommandHandler("userinfo", get_user_info))
    application.add_handler(CommandHandler("listusers", list_all_users))
    application.add_handler(CommandHandler("adminhelp", admin_help))
    application.add_handler(CommandHandler("stats", get_bot_stats))

    # Error Handler
    application.add_error_handler(error_handler)


async def main():
    """Main function to run MULTIPLE bots concurrently"""
    logger.info("🚀 Starting Multi-Bot System...")

    # =================================================================
    # 1. Start Flask Server FIRST (To satisfy Render Port Requirement)
    # =================================================================
    # इसे सबसे ऊपर ले जाएं ताकि Render को तुरंत Port मिल जाए और Timeout न हो
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    logger.info("🌐 Flask server started immediately to prevent Timeout.")

    # =================================================================
    # 2. Database Setup (Ab ye araam se run ho sakta hai)
    # =================================================================
    try:
        setup_database()
        migrate_add_imdb_columns()
    except Exception as e:
        logger.error(f"DB Setup Error: {e}")

    # =================================================================
    # 3. Get Tokens from ENV
    # =================================================================
    tokens = [
        os.environ.get("TELEGRAM_BOT_TOKEN"),  # Bot 1
        os.environ.get("BOT_TOKEN_2"),         # Bot 2
        os.environ.get("BOT_TOKEN_3")          # Bot 3
    ]
    
    # Khali tokens filter karo
    tokens = [t for t in tokens if t]

    if not tokens:
        logger.error("❌ No tokens found! Check Environment Variables.")
        return

    # =================================================================
    # 4. Initialize & Start All Bots
    # =================================================================
    apps = []
    logger.info(f"🤖 Found {len(tokens)} tokens. Initializing bots...")

    for i, token in enumerate(tokens):
        try:
            logger.info(f"🔹 Initializing Bot {i+1}...")
            
            # Application Build
            app = Application.builder().token(token).read_timeout(30).write_timeout(30).build()
            
            # Handlers Register karo
            register_handlers(app)
            
            # Initialize & Start
            await app.initialize()
            await app.start()
            
            # Non-blocking Polling Start karo
            await app.updater.start_polling(drop_pending_updates=True)
            
            apps.append(app)
            
            bot_info = await app.bot.get_me()
            logger.info(f"✅ Bot {i+1} Started: @{bot_info.username}")
            
        except Exception as e:
            logger.error(f"❌ Failed to start Bot {i+1}: {e}")

    if not apps:
        logger.error("❌ No bots could be started.")
        return

    # =================================================================
    # 5. Keep Script Alive
    # =================================================================
    stop_signal = asyncio.Event()
    await stop_signal.wait()

    # Cleanup (Jab script band ho)
    for app in apps:
        await app.stop()
        await app.shutdown()

if __name__ == '__main__':
    try:
        # Async loop start
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Critical Error: {e}")
