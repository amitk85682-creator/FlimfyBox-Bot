import asyncio
import requests
import logging
import os
import psycopg2
import socket
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)
TMDB_API_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"
# 👇 NAYA CODE DALO 👇
try:
    import db_utils
    FIXED_DATABASE_URL = getattr(db_utils, "FIXED_DATABASE_URL", None)
except ImportError:
    FIXED_DATABASE_URL = None

DATABASE_URL = FIXED_DATABASE_URL or os.environ.get('DATABASE_URL')
CHANNEL_ID = int(os.environ.get('CHANNEL_ID', '-1002555232489'))

# 🆔 BOT UNIQUE ID (Environment variable se lo)
BOT_INSTANCE_ID = os.environ.get('BOT_INSTANCE_ID', socket.gethostname())

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

# ═══════════════════════════════════════════════
#  📦 DATABASE SETUP (WITH LOCKING)
# ═══════════════════════════════════════════════
def setup_trending_db():
    if not DATABASE_URL:
        logger.warning("⚠️ DATABASE_URL not set! Trending system disabled.")
        return False
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # ✅ Main History Table
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
        
        # ✅ Meta Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_meta (
                id INTEGER PRIMARY KEY,
                last_check TIMESTAMP,
                locked_by TEXT DEFAULT NULL,
                lock_time TIMESTAMP DEFAULT NULL
            )
        """)
        
        cur.execute("""
            INSERT INTO trending_meta (id, last_check, locked_by, lock_time) 
            VALUES (1, '2000-01-01', NULL, NULL) 
            ON CONFLICT (id) DO NOTHING
        """)
        
        # ✅ Cleanup old entries
        cur.execute("DELETE FROM trending_history WHERE alerted_at < NOW() - INTERVAL '30 days'")
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"✅ Database setup complete for bot: {BOT_INSTANCE_ID}")
        return True
    except Exception as e:
        logger.error(f"❌ Trending DB Setup Error: {e}")
        return False


# ═══════════════════════════════════════════════
#  🔒 DISTRIBUTED LOCK FUNCTIONS
# ═══════════════════════════════════════════════
def acquire_lock():
    """Try to acquire exclusive lock for this bot instance"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Check if lock is held by another bot (and is still fresh)
        cur.execute("""
            SELECT locked_by, lock_time 
            FROM trending_meta 
            WHERE id = 1
        """)
        result = cur.fetchone()
        
        if result and result[0]:
            locked_by, lock_time = result
            # If lock is older than 10 minutes, consider it stale
            if lock_time and (datetime.utcnow() - lock_time).total_seconds() < 600:
                if locked_by != BOT_INSTANCE_ID:
                    logger.info(f"🔒 Lock held by {locked_by}, skipping...")
                    cur.close()
                    conn.close()
                    return False
        
        # Acquire lock
        cur.execute("""
            UPDATE trending_meta 
            SET locked_by = %s, lock_time = CURRENT_TIMESTAMP 
            WHERE id = 1
        """, (BOT_INSTANCE_ID,))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"✅ Lock acquired by {BOT_INSTANCE_ID}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Lock acquisition error: {e}")
        return False


def release_lock():
    """Release the lock after posting"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE trending_meta 
            SET locked_by = NULL, lock_time = NULL, last_check = CURRENT_TIMESTAMP
            WHERE id = 1 AND locked_by = %s
        """, (BOT_INSTANCE_ID,))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"🔓 Lock released by {BOT_INSTANCE_ID}")
        
    except Exception as e:
        logger.error(f"❌ Lock release error: {e}")


def get_time_since_last_check():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT last_check FROM trending_meta WHERE id = 1")
        last_check = cur.fetchone()[0]
        cur.close()
        conn.close()
        return datetime.utcnow() - last_check
    except:
        return timedelta(hours=4)


# ═══════════════════════════════════════════════
#  🌐 TMDB & MESSAGE HELPERS (SAME AS BEFORE)
# ═══════════════════════════════════════════════
def fetch_trending(time_window="day"):
    try:
        url = f"https://api.themoviedb.org/3/trending/all/{time_window}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get('results', [])[:15]
    except Exception as e:
        return []

def fetch_extra_details(tmdb_id, media_type):
    try:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=10)
        return resp.json()
    except Exception:
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
    
    # Text escape for HTML (Taaki special chars crash na karein)
    safe_title = str(title).replace('<', '&lt;').replace('>', '&gt;')
    safe_overview = str(overview).replace('<', '&lt;').replace('>', '&gt;')
    if len(safe_overview) > 200: safe_overview = safe_overview[:197] + "..."

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

    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 View on TMDB", url=f"https://www.themoviedb.org/{media_type}/{tmdb_id}"),
            InlineKeyboardButton("🔍 Search Google", url=f"https://www.google.com/search?q={safe_title.replace(' ', '+')}+download")
        ]
    ])

    image_url = f"https://image.tmdb.org/t/p/w780{item.get('backdrop_path')}" if item.get('backdrop_path') else None
    return text, buttons, image_url

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
    if new_count == 0: text += "💤 <i>Sab kuch covered hai.</i>"
    else: text += "🔥 <i>Naya content aaya hai!</i>"
    return text


# ═══════════════════════════════════════════════
#  🔄 CORE LOGIC WITH LOCKING
# ═══════════════════════════════════════════════
async def check_and_alert_trending(app, admin_id):
    # ✅ Try to acquire lock first
    if not acquire_lock():
        logger.info(f"⏭️ Skipping check - another bot is processing")
        return 0
    
    logger.info(f"🔍 {BOT_INSTANCE_ID} checking Worldwide Trending...")
    new_alerts, auto_posted, skipped_already, skipped_in_db = 0, 0, 0, 0
    conn = None

    try:
        trending_items = fetch_trending("day")
        if not trending_items: 
            release_lock()
            return 0

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        total_checked = len(trending_items)

        for item in trending_items:
            tmdb_id = int(item.get('id', 0))
            title = item.get('title') or item.get('name')
            media_type = item.get('media_type', 'movie')

            if not title or not tmdb_id: continue

            # ✅ STEP 1: History Check
            cur.execute("SELECT tmdb_id, posted_by FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
            existing = cur.fetchone()
            
            if existing:
                logger.info(f"⏭️ Already alerted by {existing[1]}: {title}")
                skipped_already += 1
                continue

            # ✅ STEP 2: Save to DB WITH bot_id
            try:
                cur.execute(
                    """INSERT INTO trending_history 
                       (tmdb_id, title, media_type, popularity, vote_average, posted_by) 
                       VALUES (%s, %s, %s, %s, %s, %s) 
                       ON CONFLICT (tmdb_id) DO NOTHING""",
                    (tmdb_id, title, media_type, 
                     float(item.get('popularity', 0)), 
                     float(item.get('vote_average', 0)),
                     BOT_INSTANCE_ID)
                )
                conn.commit()
                logger.info(f"✅ Saved by {BOT_INSTANCE_ID}: {title}")
            except Exception as e:
                logger.error(f"❌ DB Insert Failed: {e}")
                conn.rollback()
                continue

            # ✅ STEP 3: Message Prepare
            extra = fetch_extra_details(tmdb_id, media_type)
            text, admin_buttons, image_url = build_premium_alert(item, extra) 

            # ✅ STEP 4: Check Movie DB
            cur.execute("SELECT id FROM movies WHERE title ILIKE %s LIMIT 1", (f"%{title}%",))
            movie = cur.fetchone()

            # ✅ STEP 5: Post
            post_success = False
            if movie:
                try:
                    watch_link = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie[0]}"
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
                    logger.info(f"📤 Posted by {BOT_INSTANCE_ID}: {title}")
                    
                except Exception as e:
                    logger.error(f"❌ Channel Post Failed: {e}")
                    # Agar channel post fail ho, admin ko alert karo
                    await app.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ <b>AUTO-POST FAILED!</b>\nMovie: <i>{title}</i>\nReason: <code>{e}</code>",
                        parse_mode='HTML'
                    )
            
            else:
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
                    logger.info(f"🚨 Admin alert by {BOT_INSTANCE_ID}: {title}")
                    
                except Exception as e:
                    logger.error(f"❌ Admin Alert Failed: {e}")
                    await app.bot.send_message(chat_id=admin_id, text=f"⚠️ Failed to send alert for: {title}")

            # Agar dono me se koi bhi fail hua, toh DB se entry delete kar do taaki agli baar retry ho sake
            if not post_success:
                cur.execute("DELETE FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
                conn.commit()

            await asyncio.sleep(3)

        # ✅ Summary
        summary = build_summary_message(new_alerts, total_checked, skipped_in_db, skipped_already)
        await app.bot.send_message(chat_id=admin_id, text=summary, parse_mode='HTML')  # Changed to HTML

    except Exception as e:
        logger.error(f"❌ Trending Error: {e}")
    
    finally:
        if conn:
            try:
                cur.close()
                conn.close()
            except: pass
        
        # ✅ Always release lock
        release_lock()

    return new_alerts


# ═══════════════════════════════════════════════
#  ♻️ BACKGROUND WORKER
# ═══════════════════════════════════════════════
async def trending_worker_loop(app, admin_id):
    db_ok = setup_trending_db()
    if not db_ok: return

    # ✅ Random stagger to avoid simultaneous starts
    await asyncio.sleep(hash(BOT_INSTANCE_ID) % 60)

    # 👇 NAYA STARTUP MESSAGE LOGIC 👇
    try:
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
    except ImportError:
        ist = None

    time_since_last = get_time_since_last_check()
    if time_since_last < timedelta(hours=3):
        remaining = (timedelta(hours=3) - time_since_last).total_seconds()
    else:
        remaining = 0
        
    next_check_time = datetime.utcnow() + timedelta(seconds=remaining)
    
    if remaining == 0:
        time_str = "Abhi turant (Checking now...)"
    else:
        if ist:
            next_check_ist = next_check_time.replace(tzinfo=pytz.utc).astimezone(ist)
            time_str = next_check_ist.strftime("%I:%M %p (IST)")
        else:
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
    
    try:
        await app.bot.send_message(chat_id=admin_id, text=startup_msg, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Failed to send trending startup msg: {e}")
    # 👆 YAHAN TAK NAYA CODE HAI 👆

    while True:
        try:
            # Har loop cycle mein naya time check hoga
            time_since_last = get_time_since_last_check()
            if time_since_last < timedelta(hours=3):
                remaining = (timedelta(hours=3) - time_since_last).total_seconds()
                await asyncio.sleep(remaining)
            
            new_count = await check_and_alert_trending(app, admin_id)

            wait_hours = 2 if new_count >= 5 else 3
            await asyncio.sleep(wait_hours * 3600)

        except Exception as e:
            logger.error(f"❌ Worker loop error: {e}")
            await asyncio.sleep(3600)
