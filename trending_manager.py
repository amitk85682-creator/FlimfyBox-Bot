import asyncio
import requests
import logging
import os
import psycopg2
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)
TMDB_API_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"
DATABASE_URL = os.environ.get('DATABASE_URL')
CHANNEL_ID = int(os.environ.get('CHANNEL_ID', '-1002555232489'))

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
#  📦 DATABASE SETUP
# ═══════════════════════════════════════════════
def setup_trending_db():
    if not DATABASE_URL:
        logger.warning("⚠️ DATABASE_URL not set! Trending system disabled.")
        return False
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_history (
                tmdb_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                media_type TEXT DEFAULT 'movie',
                popularity REAL DEFAULT 0,
                vote_average REAL DEFAULT 0,
                alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_meta (
                id INTEGER PRIMARY KEY,
                last_check TIMESTAMP
            )
        """)
        cur.execute("INSERT INTO trending_meta (id, last_check) VALUES (1, '2000-01-01') ON CONFLICT (id) DO NOTHING")
        cur.execute("DELETE FROM trending_history WHERE alerted_at < NOW() - INTERVAL '30 days'")
        
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ Trending DB Setup Error: {e}")
        return False

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

def update_last_check_time():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("UPDATE trending_meta SET last_check = CURRENT_TIMESTAMP WHERE id = 1")
        conn.commit()
        cur.close()
        conn.close()
    except: pass


# ═══════════════════════════════════════════════
#  🌐 TMDB & MESSAGE HELPERS
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
    
    if overview and len(overview) > 200: overview = overview[:197] + "..."

    text = (
        f"┌─────────────────────────┐\n"
        f"   🚨  **TRENDING ALERT** 🚨\n"
        f"└─────────────────────────┘\n\n"
        f"{type_emoji} **{title}**\n\n"
        f"┌ 📌 **Type:** `{type_label}`\n"
        f"├ 📅 **Released:** `{release}`\n"
        f"└ {stars}  **{vote_avg}/10**\n\n"
        f"📝 **Synopsis:**\n_{overview}_\n"
        f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ _Yeh tere database mein nahi hai!_\n"
        f"📥 _Add kar le before users search karein._"
    )

    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 View on TMDB", url=f"https://www.themoviedb.org/{media_type}/{tmdb_id}"),
            InlineKeyboardButton("🔍 Search Google", url=f"https://www.google.com/search?q={title.replace(' ', '+')}+download")
        ]
    ])

    image_url = f"https://image.tmdb.org/t/p/w780{item.get('backdrop_path')}" if item.get('backdrop_path') else None
    return text, buttons, image_url

def build_summary_message(new_count, total_checked, skipped_in_db, skipped_already):
    now = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC")
    text = (
        f"┌─────────────────────────┐\n"
        f"   📊  **TRENDING SUMMARY**\n"
        f"└─────────────────────────┘\n\n"
        f"🕐 **Time:** `{now}`\n"
        f"🔍 **Checked:** `{total_checked}` trending items\n"
        f"🚀 **Auto-Posted:** `{skipped_in_db}`\n"
        f"🔁 **Already Alerted:** `{skipped_already}`\n"
        f"🆕 **New Alerts:** `{new_count}`\n\n"
    )
    if new_count == 0: text += "💤 _Sab kuch covered hai._"
    else: text += "🔥 _Naya content aaya hai!_"
    return text

# ═══════════════════════════════════════════════
#  🔄 BULLETPROOF CORE LOGIC
# ═══════════════════════════════════════════════
async def check_and_alert_trending(app, admin_id):
    logger.info("🔍 Checking Worldwide Trending...")
    new_alerts, auto_posted, skipped_already, skipped_in_db = 0, 0, 0, 0
    conn = None

    try:
        trending_items = fetch_trending("day")
        if not trending_items: return 0

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        total_checked = len(trending_items)

        for item in trending_items:
            tmdb_id = int(item.get('id', 0))
            title = item.get('title') or item.get('name')
            media_type = item.get('media_type', 'movie')

            if not title or not tmdb_id: continue

            # 🛑 STEP 1: History Check
            cur.execute("SELECT tmdb_id FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
            if cur.fetchone():
                skipped_already += 1
                continue

            # 🛑 STEP 2: PEHLE SAVE KARO! (Yahi tera spam block karega)
            try:
                cur.execute(
                    """INSERT INTO trending_history (tmdb_id, title, media_type, popularity, vote_average) 
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (tmdb_id) DO NOTHING""",
                    (tmdb_id, title, media_type, float(item.get('popularity', 0)), float(item.get('vote_average', 0)))
                )
                conn.commit()
            except Exception as e:
                logger.error(f"DB Insert Error: {e}")
                conn.rollback()

            # 🛑 STEP 3: Message Banao
            extra = fetch_extra_details(tmdb_id, media_type)
            text, admin_buttons, image_url = build_premium_alert(item, extra) 

            # 🛑 STEP 4: DB Check & Post
            cur.execute("SELECT id FROM movies WHERE title ILIKE %s LIMIT 1", (f"%{title}%",))
            movie = cur.fetchone()

            if movie:
                # 🎬 DB MEIN HAI -> CHANNEL PE POST
                skipped_in_db += 1
                watch_link = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie[0]}"
                channel_buttons = InlineKeyboardMarkup([[InlineKeyboardButton("📥 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗡𝗢𝗪", url=watch_link)]])
                
                try:
                    if image_url:
                        await app.bot.send_photo(chat_id=CHANNEL_ID, photo=image_url, caption=text, parse_mode=ParseMode.MARKDOWN, reply_markup=channel_buttons)
                    else:
                        await app.bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=channel_buttons)
                    auto_posted += 1
                except Exception as e:
                    logger.error(f"❌ Channel Post Error: {e}")
                    # Bulletproof error message (No Markdown crash)
                    try:
                        await app.bot.send_message(chat_id=admin_id, text=f"⚠️ Auto-Post Failed!\n🎬 Movie: {title}\n❌ Error: {e}")
                    except: pass
            
            else:
                # 🚨 DB MEIN NAHI HAI -> ADMIN KO ALERT
                try:
                    if image_url:
                        await app.bot.send_photo(chat_id=admin_id, photo=image_url, caption=text, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_buttons)
                    else:
                        await app.bot.send_message(chat_id=admin_id, text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_buttons)
                    new_alerts += 1
                except Exception as e:
                    logger.error(f"❌ Admin Alert Error: {e}")

            await asyncio.sleep(2)

        # 📊 SINGLE SUMMARY MESSAGE
        summary = build_summary_message(new_alerts, total_checked, skipped_in_db, skipped_already)
        await app.bot.send_message(chat_id=admin_id, text=summary, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        logger.error(f"❌ Trending Full Error: {e}")
    finally:
        if conn:
            try:
                cur.close()
                conn.close()
            except: pass

    return new_alerts


# ═══════════════════════════════════════════════
#  ♻️ BACKGROUND WORKER
# ═══════════════════════════════════════════════
async def trending_worker_loop(app, admin_id):
    db_ok = setup_trending_db()
    if not db_ok: return

    while True:
        try:
            time_since_last = get_time_since_last_check()
            if time_since_last < timedelta(hours=3):
                remaining = (timedelta(hours=3) - time_since_last).total_seconds()
                await asyncio.sleep(remaining)
            
            new_count = await check_and_alert_trending(app, admin_id)
            update_last_check_time()

            wait_hours = 2 if new_count >= 5 else 3
            await asyncio.sleep(wait_hours * 3600)

        except Exception as e:
            await asyncio.sleep(3600)
