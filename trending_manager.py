import asyncio
import logging
import os
import socket
from datetime import datetime, timedelta
import pytz

import requests
import psycopg2
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# -------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# ENVIRONMENT VARIABLES
# -------------------------------------------------------------
TMDB_API_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"
CHANNEL_ID = int(os.environ.get('CHANNEL_ID', '-1002555232489'))
BOT_INSTANCE_ID = os.environ.get('BOT_INSTANCE_ID', socket.gethostname())
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', '6946322342'))

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    logger.error("❌ DATABASE_URL not set!")

def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"❌ DB conn error: {e}")
        return None

def close_db_connection(conn):
    if conn:
        try:
            conn.close()
        except:
            pass

# -------------------------------------------------------------
# TMDB API
# -------------------------------------------------------------
def fetch_trending(time_window="day"):
    try:
        url = f"https://api.themoviedb.org/3/trending/all/{time_window}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get('results', [])[:15]
    except Exception as e:
        logger.error(f"fetch_trending error: {e}")
        return []

def fetch_extra_details(tmdb_id, media_type):
    try:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=10)
        return resp.json()
    except Exception as e:
        logger.error(f"fetch_extra_details error: {e}")
        return {}

# -------------------------------------------------------------
# NEW MESSAGE FORMAT (as requested)
# -------------------------------------------------------------
def build_premium_alert(item, extra):
    title = item.get('title') or item.get('name') or "Unknown"
    media_type = item.get('media_type', 'movie')
    tmdb_id = item.get('id')
    overview = item.get('overview', '')
    vote_avg = item.get('vote_average', 0)
    release_date = item.get('release_date') or item.get('first_air_date') or "N/A"
    
    # Genre from extra
    genre_ids = extra.get('genres', [])
    genre_names = [g['name'] for g in genre_ids if 'name' in g]
    genre_str = " | ".join(genre_names) if genre_names else "Unknown"
    
    # Cast (top 3)
    cast_list = []
    credits = extra.get('credits', {})
    for cast in credits.get('cast', [])[:3]:
        cast_list.append(cast.get('name', ''))
    cast_str = ", ".join(cast_list) if cast_list else "Not available"
    
    # Runtime
    runtime = extra.get('runtime') or extra.get('episode_run_time', [None])[0]
    runtime_str = f"{runtime} min" if runtime else "N/A"
    
    # Stars rating (⭐)
    stars = "⭐" * min(int(round(vote_avg / 2)), 5) if vote_avg else "☆☆☆☆☆"
    
    # Language (default)
    lang = "Hindi + English (Dual Audio)"
    
    # Quality (dynamic - we'll set placeholder)
    dynamic_res = "1080p | 720p | 480p"
    
    # Safe HTML escape
    safe_title = str(title).replace('<', '&lt;').replace('>', '&gt;')
    safe_overview = str(overview).replace('<', '&lt;').replace('>', '&gt;')
    if len(safe_overview) > 200:
        safe_overview = safe_overview[:197] + "..."
    
    # New format (like Bhabiji example)
    text = (
        f"<b>{safe_title}</b>\n"
        f"📅 <b>Date:</b> {release_date}\n"
        f"⭐ <b>iMDB Rating:</b> {vote_avg}/10\n"
        f"🎭 <b>Genre:</b> {genre_str}\n"
        f"🌟 <b>Stars:</b> {cast_str}\n"
        f"🔊 <b>Language:</b> {lang}\n"
        f"💿 <b>Quality:</b> V2 HQ-HDTC {dynamic_res}\n"
        f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
        f"👇 <b>Download Below</b> 👇\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Yeh movie database mein nahi hai!</i>\n"
        f"📥 <i>Add kar le before users search karein.</i>"
    )
    
    admin_buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 View on TMDB", url=f"https://www.themoviedb.org/{media_type}/{tmdb_id}"),
            InlineKeyboardButton("🔍 Search Google", url=f"https://www.google.com/search?q={safe_title.replace(' ', '+')}+download")
        ]
    ])
    
    image_url = f"https://image.tmdb.org/t/p/w780{item.get('backdrop_path')}" if item.get('backdrop_path') else None
    return text, admin_buttons, image_url

def build_summary_message(new_count, total_checked, skipped_in_db, skipped_already):
    now = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%d %b %Y, %I:%M %p IST")
    text = (
        f"📊 <b>TRENDING SUMMARY</b>\n\n"
        f"🤖 Bot: <code>{BOT_INSTANCE_ID}</code>\n"
        f"🕐 Time: <code>{now}</code>\n"
        f"🔍 Checked: <code>{total_checked}</code> items\n"
        f"🚀 Auto-Posted: <code>{skipped_in_db}</code>\n"
        f"🔁 Already Alerted: <code>{skipped_already}</code>\n"
        f"🆕 New Alerts: <code>{new_count}</code>\n\n"
    )
    if new_count == 0:
        text += "💤 <i>Sab kuch covered hai.</i>"
    else:
        text += "🔥 <i>Naya content aaya hai!</i>"
    return text

# -------------------------------------------------------------
# DATABASE SETUP (with missing columns)
# -------------------------------------------------------------
def setup_trending_db():
    if not DATABASE_URL:
        return False
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_history (
                tmdb_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                media_type TEXT DEFAULT 'movie',
                popularity REAL DEFAULT 0,
                vote_average REAL DEFAULT 0,
                alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                posted_by TEXT DEFAULT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_meta (
                id INTEGER PRIMARY KEY,
                last_check TIMESTAMP,
                locked_by TEXT DEFAULT NULL,
                lock_time TIMESTAMP DEFAULT NULL
            )
        """)
        
        # Add missing columns if any
        for col in ['posted_by']:
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='trending_history' AND column_name='{col}'
            """)
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE trending_history ADD COLUMN {col} TEXT DEFAULT NULL")
        
        for col in ['locked_by', 'lock_time']:
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='trending_meta' AND column_name='{col}'
            """)
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE trending_meta ADD COLUMN {col} TEXT DEFAULT NULL")
        
        cur.execute("INSERT INTO trending_meta (id, last_check) VALUES (1, '2000-01-01') ON CONFLICT (id) DO NOTHING")
        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info("✅ Trending DB ready")
        return True
    except Exception as e:
        logger.error(f"setup_trending_db error: {e}")
        return False

def get_last_check_time():
    try:
        conn = get_db_connection()
        if not conn:
            return datetime(2000, 1, 1)
        cur = conn.cursor()
        cur.execute("SELECT last_check FROM trending_meta WHERE id = 1")
        row = cur.fetchone()
        cur.close()
        close_db_connection(conn)
        if row and row[0]:
            return row[0]
        return datetime(2000, 1, 1)
    except:
        return datetime(2000, 1, 1)

def update_last_check():
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("UPDATE trending_meta SET last_check = CURRENT_TIMESTAMP WHERE id = 1")
        conn.commit()
        cur.close()
        close_db_connection(conn)
    except Exception as e:
        logger.error(f"update_last_check error: {e}")

def acquire_lock():
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute("SELECT locked_by, lock_time FROM trending_meta WHERE id = 1")
        row = cur.fetchone()
        if row and row[0]:
            locked_by, lock_time = row
            if lock_time and (datetime.utcnow() - lock_time).total_seconds() < 600:
                if locked_by != BOT_INSTANCE_ID:
                    cur.close()
                    close_db_connection(conn)
                    return False
        cur.execute("UPDATE trending_meta SET locked_by = %s, lock_time = CURRENT_TIMESTAMP WHERE id = 1", (BOT_INSTANCE_ID,))
        conn.commit()
        cur.close()
        close_db_connection(conn)
        return True
    except Exception as e:
        logger.error(f"acquire_lock error: {e}")
        return False

def release_lock():
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("UPDATE trending_meta SET locked_by = NULL, lock_time = NULL WHERE id = 1 AND locked_by = %s", (BOT_INSTANCE_ID,))
        conn.commit()
        cur.close()
        close_db_connection(conn)
    except Exception as e:
        logger.error(f"release_lock error: {e}")

# -------------------------------------------------------------
# MAIN TRENDING CHECK
# -------------------------------------------------------------
async def check_and_alert_trending(app, admin_id):
    if not acquire_lock():
        return 0
    
    logger.info(f"🔍 Checking trending...")
    new_alerts = 0
    auto_posted = 0
    skipped_already = 0
    skipped_in_db = 0
    
    conn = None
    try:
        items = fetch_trending("day")
        if not items:
            release_lock()
            return 0
        
        conn = get_db_connection()
        if not conn:
            release_lock()
            return 0
        
        cur = conn.cursor()
        total = len(items)
        
        for item in items:
            tmdb_id = int(item.get('id', 0))
            title = item.get('title') or item.get('name')
            media_type = item.get('media_type', 'movie')
            if not title or not tmdb_id:
                continue
            
            # Check if already alerted
            cur.execute("SELECT tmdb_id FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
            if cur.fetchone():
                skipped_already += 1
                continue
            
            # Insert into history
            try:
                cur.execute("""
                    INSERT INTO trending_history (tmdb_id, title, media_type, popularity, vote_average, posted_by)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (tmdb_id, title, media_type, item.get('popularity', 0), item.get('vote_average', 0), BOT_INSTANCE_ID))
                conn.commit()
            except Exception as e:
                logger.error(f"Insert error: {e}")
                conn.rollback()
                continue
            
            # Fetch extra details for message
            extra = fetch_extra_details(tmdb_id, media_type)
            text, admin_buttons, image_url = build_premium_alert(item, extra)
            
            # Check if movie exists in main 'movies' table
            cur.execute("SELECT id FROM movies WHERE title ILIKE %s LIMIT 1", (f"%{title}%",))
            movie_row = cur.fetchone()
            
            if movie_row:
                # Auto-post to channel
                try:
                    watch_link = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_row[0]}"
                    channel_buttons = InlineKeyboardMarkup([[
                        InlineKeyboardButton("📥 DOWNLOAD NOW", url=watch_link)
                    ]])
                    if image_url:
                        await app.bot.send_photo(chat_id=CHANNEL_ID, photo=image_url, caption=text, parse_mode='HTML', reply_markup=channel_buttons)
                    else:
                        await app.bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode='HTML', reply_markup=channel_buttons)
                    auto_posted += 1
                    skipped_in_db += 1
                    logger.info(f"Auto-posted: {title}")
                except Exception as e:
                    logger.error(f"Channel post failed: {e}")
            else:
                # Send to admin
                try:
                    if image_url:
                        await app.bot.send_photo(chat_id=admin_id, photo=image_url, caption=text, parse_mode='HTML', reply_markup=admin_buttons)
                    else:
                        await app.bot.send_message(chat_id=admin_id, text=text, parse_mode='HTML', reply_markup=admin_buttons)
                    new_alerts += 1
                    logger.info(f"Admin alert: {title}")
                except Exception as e:
                    logger.error(f"Admin alert failed: {e}")
            
            await asyncio.sleep(3)
        
        # Summary
        summary = build_summary_message(new_alerts, total, skipped_in_db, skipped_already)
        try:
            await app.bot.send_message(chat_id=admin_id, text=summary, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Summary send failed: {e}")
        
        cur.close()
        update_last_check()
    except Exception as e:
        logger.error(f"check_and_alert_trending error: {e}")
    finally:
        if conn:
            close_db_connection(conn)
        release_lock()
    
    return new_alerts

# -------------------------------------------------------------
# SCHEDULER: Fixed IST times (e.g., 00:00, 06:00, 12:00, 18:00)
# -------------------------------------------------------------
def get_next_run_time_ist(hours=[0, 6, 12, 18]):
    """
    Returns next datetime (in UTC) when we should run, based on IST hours.
    """
    tz = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(tz)
    candidates = []
    for h in hours:
        candidate = now_ist.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidate <= now_ist:
            candidate += timedelta(days=1)
        candidates.append(candidate)
    next_ist = min(candidates)
    return next_ist.astimezone(pytz.UTC)

async def trending_worker_loop(app, admin_id):
    logger.info("🛠️ TRENDING WORKER STARTED (Fixed Schedule IST)")
    if not setup_trending_db():
        logger.error("DB setup failed, worker stopping")
        return
    
    # Send startup message with next run time
    next_run_utc = get_next_run_time_ist()
    tz = pytz.timezone('Asia/Kolkata')
    next_ist = next_run_utc.astimezone(tz)
    time_str = next_ist.strftime("%I:%M %p IST")
    
    startup_msg = (
        f"🚀 <b>TRENDING MONITOR ON</b>\n\n"
        f"🕒 Fixed Schedule: 00:00, 06:00, 12:00, 18:00 IST\n"
        f"⏱️ Next Check: <code>{time_str}</code>\n\n"
        f"💤 Bot will sleep until then."
    )
    try:
        await app.bot.send_message(chat_id=admin_id, text=startup_msg, parse_mode='HTML')
        logger.info(f"Startup message sent to {admin_id}")
    except Exception as e:
        logger.error(f"Failed to send startup: {e}")
    
    # Main loop
    while True:
        now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
        next_run_utc = get_next_run_time_ist()
        sleep_seconds = (next_run_utc - now_utc).total_seconds()
        if sleep_seconds < 0:
            sleep_seconds = 0
        
        logger.info(f"Sleeping for {sleep_seconds/3600:.2f} hours until next scheduled time")
        await asyncio.sleep(sleep_seconds)
        
        # Run check
        logger.info("🔍 Running scheduled trending check...")
        await check_and_alert_trending(app, admin_id)
        
        # After run, loop will recalculate next run
