import asyncio
import logging
import os
import socket
from datetime import datetime, timedelta

import requests
import psycopg2
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# -------------------------------------------------------------
# 1. LOGGING SETUP
# -------------------------------------------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# 2. ENVIRONMENT VARIABLES
# -------------------------------------------------------------
TMDB_API_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"   # Public TMDB key
CHANNEL_ID = int(os.environ.get('CHANNEL_ID', '-1002555232489'))   # Trending posts yahan jayenge
BOT_INSTANCE_ID = os.environ.get('BOT_INSTANCE_ID', socket.gethostname())
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', '6946322342'))   # Aapki real user ID

# -------------------------------------------------------------
# 3. DATABASE HELPERS (same as main.py)
# -------------------------------------------------------------
# NOTE: Hum 'db_pool' ko import nahi kar sakte (circular import se bachne ke liye)
# Isliye yahan naye connections banayenge, lekin wahi DATABASE_URL use karenge.

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    logger.error("❌ DATABASE_URL not set in environment!")
else:
    logger.info(f"✅ DATABASE_URL found (length {len(DATABASE_URL)})")

def get_db_connection():
    """Simple connection (not from pool) – but uses same DATABASE_URL"""
    if not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"❌ get_db_connection error: {e}")
        return None

def close_db_connection(conn):
    if conn:
        try:
            conn.close()
        except:
            pass

# -------------------------------------------------------------
# 4. TMDB API HELPERS
# -------------------------------------------------------------
GENRE_MAP = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
    80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
    14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Sci-Fi", 10770: "TV Movie",
    53: "Thriller", 10752: "War", 37: "Western",
    10759: "Action & Adventure", 10762: "Kids", 10763: "News",
    10764: "Reality", 10765: "Sci-Fi & Fantasy", 10766: "Soap",
    10767: "Talk", 10768: "War & Politics"
}

def fetch_trending(time_window="day"):
    try:
        url = f"https://api.themoviedb.org/3/trending/all/{time_window}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        results = resp.json().get('results', [])[:15]
        logger.info(f"🌐 Fetched {len(results)} trending items from TMDB")
        return results
    except Exception as e:
        logger.error(f"❌ fetch_trending error: {e}")
        return []

def fetch_extra_details(tmdb_id, media_type):
    try:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=10)
        return resp.json()
    except Exception as e:
        logger.error(f"❌ fetch_extra_details({tmdb_id}) error: {e}")
        return {}

def build_premium_alert(item, extra):
    title = item.get('title') or item.get('name') or "Unknown"
    media_type = item.get('media_type', 'movie')
    tmdb_id = item.get('id')
    overview = item.get('overview', '')
    popularity = item.get('popularity', 0)
    vote_avg = item.get('vote_average', 0)
    release = item.get('release_date') or item.get('first_air_date') or "N/A"

    stars = "⭐" * min(int(round(vote_avg / 2)), 5) if vote_avg else "☆☆☆☆☆"
    type_emoji = "🎬" if media_type == "movie" else "📺"
    type_label = "Movie" if media_type == "movie" else "TV Series"

    safe_title = str(title).replace('<', '&lt;').replace('>', '&gt;')
    safe_overview = str(overview).replace('<', '&lt;').replace('>', '&gt;')
    if len(safe_overview) > 200:
        safe_overview = safe_overview[:197] + "..."

    text = (
        f"┌─────────────────────────┐\n"
        f"   🚨  <b>TRENDING ALERT</b> 🚨\n"
        f"└─────────────────────────┘\n\n"
        f"{type_emoji} <b>{safe_title}</b>\n\n"
        f"┌ 📌 <b>Type:</b> <code>{type_label}</code>\n"
        f"├ 📅 <b>Released:</b> <code>{release}</code>\n"
        f"└ {stars}  <b>{vote_avg}/10</b>\n\n"
        f"📝 <b>Synopsis:</b>\n<i>{safe_overview}</i>\n"
        f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Yeh tere database mein nahi hai!</i>\n"
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
    now = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC")
    text = (
        f"┌─────────────────────────┐\n"
        f"   📊  <b>TRENDING SUMMARY</b>\n"
        f"└─────────────────────────┘\n\n"
        f"🤖 <b>Bot:</b> <code>{BOT_INSTANCE_ID}</code>\n"
        f"🕐 <b>Time:</b> <code>{now}</code>\n"
        f"🔍 <b>Checked:</b> <code>{total_checked}</code> trending items\n"
        f"🚀 <b>Auto-Posted:</b> <code>{skipped_in_db}</code>\n"
        f"🔁 <b>Already Alerted:</b> <code>{skipped_already}</code>\n"
        f"🆕 <b>New Alerts:</b> <code>{new_count}</code>\n\n"
    )
    if new_count == 0:
        text += "💤 <i>Sab kuch covered hai.</i>"
    else:
        text += "🔥 <i>Naya content aaya hai!</i>"
    return text

# -------------------------------------------------------------
# 5. DATABASE SETUP (trending_history + meta)
# -------------------------------------------------------------
def setup_trending_db():
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL missing, cannot setup trending DB")
        return False
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Cannot connect to DB for trending setup")
            return False
        cur = conn.cursor()

        # Create trending_history if not exists
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

        # Create trending_meta if not exists, otherwise add missing columns
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_meta (
                id INTEGER PRIMARY KEY,
                last_check TIMESTAMP,
                locked_by TEXT DEFAULT NULL,
                lock_time TIMESTAMP DEFAULT NULL
            )
        """)

        # Check and add missing columns (for existing tables)
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='trending_meta' AND column_name='locked_by'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE trending_meta ADD COLUMN locked_by TEXT DEFAULT NULL")
            logger.info("➕ Added column 'locked_by' to trending_meta")

        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='trending_meta' AND column_name='lock_time'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE trending_meta ADD COLUMN lock_time TIMESTAMP DEFAULT NULL")
            logger.info("➕ Added column 'lock_time' to trending_meta")

        # Insert default row if not exists
        cur.execute("SELECT 1 FROM trending_meta WHERE id = 1")
        if not cur.fetchone():
            cur.execute("INSERT INTO trending_meta (id, last_check) VALUES (1, '2000-01-01')")

        # Clean old entries (30 days)
        cur.execute("DELETE FROM trending_history WHERE alerted_at < NOW() - INTERVAL '30 days'")

        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info("✅ Trending database tables ready")
        return True
    except Exception as e:
        logger.error(f"❌ setup_trending_db error: {e}", exc_info=True)
        return False

def get_time_since_last_check():
    try:
        conn = get_db_connection()
        if not conn:
            return timedelta(hours=4)
        cur = conn.cursor()
        cur.execute("SELECT last_check FROM trending_meta WHERE id = 1")
        row = cur.fetchone()
        cur.close()
        close_db_connection(conn)
        if row and row[0]:
            return datetime.utcnow() - row[0]
        return timedelta(hours=4)
    except Exception as e:
        logger.error(f"get_time_since_last_check error: {e}")
        return timedelta(hours=4)

# -------------------------------------------------------------
# 6. DISTRIBUTED LOCK (PostgreSQL based)
# -------------------------------------------------------------
def acquire_lock():
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        # Check if lock is held by another bot and still fresh (<10 min)
        cur.execute("SELECT locked_by, lock_time FROM trending_meta WHERE id = 1")
        result = cur.fetchone()
        if result and result[0]:
            locked_by, lock_time = result
            if lock_time and (datetime.utcnow() - lock_time).total_seconds() < 600:
                if locked_by != BOT_INSTANCE_ID:
                    logger.info(f"🔒 Lock held by {locked_by}, skipping...")
                    cur.close()
                    close_db_connection(conn)
                    return False
        # Acquire lock
        cur.execute("""
            UPDATE trending_meta 
            SET locked_by = %s, lock_time = CURRENT_TIMESTAMP 
            WHERE id = 1
        """, (BOT_INSTANCE_ID,))
        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info(f"✅ Lock acquired by {BOT_INSTANCE_ID}")
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
        cur.execute("""
            UPDATE trending_meta 
            SET locked_by = NULL, lock_time = NULL, last_check = CURRENT_TIMESTAMP
            WHERE id = 1 AND locked_by = %s
        """, (BOT_INSTANCE_ID,))
        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info(f"🔓 Lock released by {BOT_INSTANCE_ID}")
    except Exception as e:
        logger.error(f"release_lock error: {e}")

# -------------------------------------------------------------
# 7. MAIN TRENDING CHECK & ALERT FUNCTION
# -------------------------------------------------------------
async def check_and_alert_trending(app, admin_id):
    """Returns number of new alerts sent to admin"""
    if not acquire_lock():
        logger.info("⏭️ Skipping check - another bot is processing")
        return 0

    logger.info(f"🔍 {BOT_INSTANCE_ID} checking Worldwide Trending...")
    new_alerts = 0
    auto_posted = 0
    skipped_already = 0
    skipped_in_db = 0

    conn = None
    try:
        trending_items = fetch_trending("day")
        if not trending_items:
            release_lock()
            return 0

        conn = get_db_connection()
        if not conn:
            logger.error("❌ Cannot get DB connection for trending check")
            release_lock()
            return 0

        cur = conn.cursor()
        total_checked = len(trending_items)

        for item in trending_items:
            tmdb_id = int(item.get('id', 0))
            title = item.get('title') or item.get('name')
            media_type = item.get('media_type', 'movie')

            if not title or not tmdb_id:
                continue

            # Check if already alerted
            cur.execute("SELECT tmdb_id, posted_by FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
            existing = cur.fetchone()
            if existing:
                logger.info(f"⏭️ Already alerted by {existing[1]}: {title}")
                skipped_already += 1
                continue

            # Insert into history (with this bot's ID)
            try:
                cur.execute("""
                    INSERT INTO trending_history 
                    (tmdb_id, title, media_type, popularity, vote_average, posted_by) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tmdb_id) DO NOTHING
                """, (tmdb_id, title, media_type,
                      float(item.get('popularity', 0)),
                      float(item.get('vote_average', 0)),
                      BOT_INSTANCE_ID))
                conn.commit()
                logger.info(f"✅ Saved to history: {title}")
            except Exception as e:
                logger.error(f"❌ DB insert failed for {title}: {e}")
                conn.rollback()
                continue

            # Fetch extra details for better message
            extra = fetch_extra_details(tmdb_id, media_type)
            text, admin_buttons, image_url = build_premium_alert(item, extra)

            # Check if movie exists in main 'movies' table
            cur.execute("SELECT id FROM movies WHERE title ILIKE %s LIMIT 1", (f"%{title}%",))
            movie_row = cur.fetchone()

            post_success = False
            if movie_row:
                # Auto-post to channel
                try:
                    watch_link = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_row[0]}"
                    channel_buttons = InlineKeyboardMarkup([[
                        InlineKeyboardButton("📥 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗡𝗢𝗪", url=watch_link)
                    ]])
                    if image_url:
                        await app.bot.send_photo(
                            chat_id=CHANNEL_ID,
                            photo=image_url,
                            caption=text,
                            parse_mode='HTML',
                            reply_markup=channel_buttons
                        )
                    else:
                        await app.bot.send_message(
                            chat_id=CHANNEL_ID,
                            text=text,
                            parse_mode='HTML',
                            reply_markup=channel_buttons
                        )
                    auto_posted += 1
                    skipped_in_db += 1
                    post_success = True
                    logger.info(f"📤 Auto-posted to channel: {title}")
                except Exception as e:
                    logger.error(f"❌ Channel post failed for {title}: {e}")
                    # Try to send error to admin
                    try:
                        await app.bot.send_message(
                            chat_id=admin_id,
                            text=f"⚠️ Auto-post failed for {title}\nError: {e}",
                            parse_mode='HTML'
                        )
                    except:
                        pass
            else:
                # Send alert to admin
                try:
                    if image_url:
                        await app.bot.send_photo(
                            chat_id=admin_id,
                            photo=image_url,
                            caption=text,
                            parse_mode='HTML',
                            reply_markup=admin_buttons
                        )
                    else:
                        await app.bot.send_message(
                            chat_id=admin_id,
                            text=text,
                            parse_mode='HTML',
                            reply_markup=admin_buttons
                        )
                    new_alerts += 1
                    post_success = True
                    logger.info(f"🚨 Admin alert sent: {title}")
                except Exception as e:
                    logger.error(f"❌ Admin alert failed for {title}: {e}")

            # If both posting attempts failed, remove from history so it can be retried later
            if not post_success:
                cur.execute("DELETE FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
                conn.commit()
                logger.warning(f"🗑️ Removed {title} from history due to post failure")

            await asyncio.sleep(3)   # rate limit

        # Send summary to admin
        summary = build_summary_message(new_alerts, total_checked, skipped_in_db, skipped_already)
        try:
            await app.bot.send_message(chat_id=admin_id, text=summary, parse_mode='HTML')
            logger.info("📊 Summary sent to admin")
        except Exception as e:
            logger.error(f"❌ Failed to send summary: {e}")

        cur.close()
    except Exception as e:
        logger.error(f"❌ check_and_alert_trending error: {e}", exc_info=True)
    finally:
        if conn:
            close_db_connection(conn)
        release_lock()

    return new_alerts

# -------------------------------------------------------------
# 8. BACKGROUND WORKER (runs every 3 hours)
# -------------------------------------------------------------
async def trending_worker_loop(app, admin_id):
    logger.info("🛠️ TRENDING WORKER STARTED! Checking DB...")
    db_ok = setup_trending_db()
    if not db_ok:
        logger.error("❌ TRENDING DB SETUP FAILED. Worker stopping.")
        return

    # ---- Startup message ----
    time_since_last = get_time_since_last_check()
    if time_since_last < timedelta(hours=3):
        remaining = (timedelta(hours=3) - time_since_last).total_seconds()
    else:
        remaining = 0

    next_check_time = datetime.utcnow() + timedelta(seconds=remaining)
    try:
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        next_check_ist = next_check_time.replace(tzinfo=pytz.utc).astimezone(ist)
        time_str = next_check_ist.strftime("%I:%M %p (IST)")
    except:
        time_str = next_check_time.strftime("%H:%M UTC")

    startup_msg = (
        f"┌─────────────────────────┐\n"
        f"   🚀  <b>TRENDING MONITOR ON</b>\n"
        f"└─────────────────────────┘\n\n"
        f"🔄 Har 3 Ghante mein check hoga\n"
        f"🎬 Nayi trending movie → Alert + Auto-Post\n\n"
        f"⏱️ <b>Next Check:</b> <code>{time_str}</code>\n\n"
        f"💤 Main kaam kar raha hoon. Tu chill kar."
    )

    # Send startup message to admin (only if admin_id is valid)
    if admin_id and admin_id != 0:
        try:
            await app.bot.send_message(chat_id=admin_id, text=startup_msg, parse_mode='HTML')
            logger.info(f"✅ Startup message sent to admin {admin_id}")
        except Exception as e:
            logger.error(f"❌ Failed to send startup message: {e}")
    else:
        logger.warning("⚠️ ADMIN_USER_ID is 0 or invalid, cannot send startup message")

    # Small random delay to avoid multiple bots hitting exactly at same time
    await asyncio.sleep(hash(BOT_INSTANCE_ID) % 60)

    # Main loop
    while True:
        try:
            # Wait until 3 hours have passed since last check
            time_since_last = get_time_since_last_check()
            if time_since_last < timedelta(hours=3):
                remaining = (timedelta(hours=3) - time_since_last).total_seconds()
                logger.info(f"⏳ Waiting {int(remaining)} seconds before next trending check...")
                await asyncio.sleep(remaining)

            logger.info("🔍 Running check_and_alert_trending...")
            new_count = await check_and_alert_trending(app, admin_id)

            # If we got many new alerts, check again sooner (2 hours)
            wait_hours = 2 if new_count >= 5 else 3
            logger.info(f"💤 Sleeping for {wait_hours} hours until next check")
            await asyncio.sleep(wait_hours * 3600)

        except Exception as e:
            logger.error(f"❌ Worker loop error: {e}", exc_info=True)
            await asyncio.sleep(3600)   # retry after 1 hour
