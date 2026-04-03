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
# Yahan apna main channel ID daal de jahan post karni hai
CHANNEL_ID = int(os.environ.get('CHANNEL_ID', '-1001234567890'))

# ─────────────────────────────────────────────
# 🎨 GENRE MAP — TMDB genre IDs to readable names
# ─────────────────────────────────────────────
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
#  📦 DATABASE SETUP — Premium Schema
# ═══════════════════════════════════════════════
def setup_trending_db():
    """Bot start hote hi premium history table banata hai"""
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

        # 🧹 Purane 30 din se zyada records auto-delete
        cur.execute("""
            DELETE FROM trending_history
            WHERE alerted_at < NOW() - INTERVAL '30 days'
        """)

        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Trending DB ready & cleaned!")
        return True
    except Exception as e:
        logger.error(f"❌ Trending DB Setup Error: {e}")
        return False


# ═══════════════════════════════════════════════
#  🌐 TMDB API HELPERS — Fetch Rich Data
# ═══════════════════════════════════════════════
def fetch_trending(time_window="day"):
    """TMDB se top trending items fetch karta hai"""
    try:
        url = f"https://api.themoviedb.org/3/trending/all/{time_window}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get('results', [])[:15]  # Top 15 check
    except requests.exceptions.RequestException as e:
        logger.error(f"🌐 TMDB API Error: {e}")
        return []


def fetch_extra_details(tmdb_id, media_type):
    """Ek item ka full detail — runtime, tagline, etc."""
    try:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return {}


# ═══════════════════════════════════════════════
#  🎨 MESSAGE BUILDER — Premium Alert Card
# ═══════════════════════════════════════════════
def build_premium_alert(item, extra):
    """Ek mast dikhne wala alert message banata hai"""

    title = item.get('title') or item.get('name') or "Unknown"
    media_type = item.get('media_type', 'movie')
    tmdb_id = item.get('id')
    overview = item.get('overview', '')
    popularity = item.get('popularity', 0)
    vote_avg = item.get('vote_average', 0)
    vote_count = item.get('vote_count', 0)
    release = item.get('release_date') or item.get('first_air_date') or "N/A"
    poster_path = item.get('poster_path')
    backdrop_path = item.get('backdrop_path')

    # Genre names build karo
    genre_ids = item.get('genre_ids', [])
    genres = ", ".join([GENRE_MAP.get(g, "Other") for g in genre_ids]) or "N/A"

    # Extra details se aur info
    tagline = extra.get('tagline', '')
    runtime = extra.get('runtime') or extra.get('episode_run_time', [None])
    if isinstance(runtime, list):
        runtime = runtime[0] if runtime else None
    original_lang = (extra.get('original_language') or 'en').upper()
    status = extra.get('status', 'N/A')

    # ⭐ Rating stars banao
    stars = "⭐" * min(int(round(vote_avg / 2)), 5) if vote_avg else "☆☆☆☆☆"

    # 🔥 Popularity level
    if popularity >= 500:
        pop_emoji = "🔥🔥🔥 ULTRA HOT"
    elif popularity >= 200:
        pop_emoji = "🔥🔥 Very Hot"
    elif popularity >= 100:
        pop_emoji = "🔥 Hot"
    else:
        pop_emoji = "📈 Rising"

    # 🎬 Media type emoji
    type_emoji = "🎬" if media_type == "movie" else "📺"
    type_label = "Movie" if media_type == "movie" else "TV Series"

    # Overview ko 200 chars me limit karo
    if overview and len(overview) > 200:
        overview = overview[:197] + "..."

    # ─── Build the premium message ───
    text = (
        f"┌─────────────────────────┐\n"
        f"   🚨  **TRENDING ALERT**  🚨\n"
        f"└─────────────────────────┘\n\n"

        f"{type_emoji} **{title}**\n"
    )

    if tagline:
        text += f"    _\"{tagline}\"_\n"

    text += f"\n"

    text += (
        f"┌ 📌 **Type:**      `{type_label}`\n"
        f"├ 🌍 **Language:**  `{original_lang}`\n"
        f"├ 🎭 **Genre:**     `{genres}`\n"
        f"├ 📅 **Released:**  `{release}`\n"
    )

    if runtime:
        text += f"├ ⏱ **Runtime:**   `{runtime} min`\n"

    text += (
        f"├ {stars}  **{vote_avg}/10**  ({vote_count} votes)\n"
        f"├ 📊 **Status:**    `{status}`\n"
        f"└ 💥 **Hype:**      {pop_emoji}\n"
    )

    if overview:
        text += f"\n📝 **Synopsis:**\n_{overview}_\n"

    text += (
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ _Yeh tere database mein nahi hai!_\n"
        f"📥 _Add kar le before users search karein._"
    )

    # ─── Buttons ───
    tmdb_url = f"https://www.themoviedb.org/{media_type}/{tmdb_id}"
    google_url = f"https://www.google.com/search?q={title.replace(' ', '+')}+{release[:4] if release != 'N/A' else ''}+download"

    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 View on TMDB", url=tmdb_url),
            InlineKeyboardButton("🔍 Search Google", url=google_url),
        ]
    ])

    # ─── Image URL ───
    image_url = None
    if backdrop_path:
        image_url = f"https://image.tmdb.org/t/p/w780{backdrop_path}"
    elif poster_path:
        image_url = f"https://image.tmdb.org/t/p/w500{poster_path}"

    return text, buttons, image_url


def build_summary_message(new_count, total_checked, skipped_in_db, skipped_already_alerted):
    """Har check ke baad ek chota summary"""
    now = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC")
    text = (
        f"┌─────────────────────────┐\n"
        f"   📊  **TRENDING SUMMARY**\n"
        f"└─────────────────────────┘\n\n"
        f"🕐 **Time:** `{now}`\n"
        f"🔍 **Checked:** `{total_checked}` trending items\n"
        f"✅ **Already in DB:** `{skipped_in_db}`\n"
        f"🔁 **Already Alerted:** `{skipped_already_alerted}`\n"
        f"🆕 **New Alerts Sent:** `{new_count}`\n\n"
    )

    if new_count == 0:
        text += "💤 _Sab kuch covered hai. Chill maar!_"
    elif new_count <= 3:
        text += "👀 _Kuch nayi cheezein aayi hain. Check kar!_"
    else:
        text += "🔥 _Bahut kuch naya trend ho raha hai! Jaldi add kar!_"

    return text


# ═══════════════════════════════════════════════
#  🔄 CORE LOGIC — CHECK & ALERT & AUTO-POST
# ═══════════════════════════════════════════════
async def check_and_alert_trending(app, admin_id):
    logger.info("🔍 Checking Worldwide Trending...")

    new_alerts = 0
    auto_posted = 0
    skipped_already = 0
    total_checked = 0

    try:
        trending_items = fetch_trending("day")
        if not trending_items:
            logger.warning("⚠️ No trending data!")
            return 0

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        total_checked = len(trending_items)

        for item in trending_items:
            tmdb_id = item.get('id')
            title = item.get('title') or item.get('name')
            media_type = item.get('media_type', 'movie')

            if not title or not tmdb_id:
                continue

            # Step 1: Already Alerted or Posted? (History Check)
            cur.execute(
                "SELECT tmdb_id FROM trending_history WHERE tmdb_id = %s",
                (tmdb_id,)
            )
            if cur.fetchone():
                skipped_already += 1
                continue

            # Step 2: Message ka text aur image ready karo
            extra = fetch_extra_details(tmdb_id, media_type)
            # Yahan hum existing buttons ignore kar rahe hain kyunki hum naye banayenge
            text, _, image_url = build_premium_alert(item, extra) 

            # Step 3: Check if in Movies DB
            cur.execute(
                "SELECT id FROM movies WHERE title ILIKE %s LIMIT 1",
                (f"%{title}%",)
            )
            movie = cur.fetchone()

            if movie:
                # 🎬 DB MEIN HAI -> CHANNEL PE POST KARO
                movie_id = movie[0]
                watch_link = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_id}"
                
                channel_buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📥 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗡𝗢𝗪", url=watch_link)]
                ])
                
                try:
                    if image_url:
                        await app.bot.send_photo(
                            chat_id=CHANNEL_ID,
                            photo=image_url,
                            caption=text,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=channel_buttons
                        )
                    else:
                        await app.bot.send_message(
                            chat_id=CHANNEL_ID,
                            text=text,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=channel_buttons,
                            disable_web_page_preview=False
                        )
                    auto_posted += 1
                    logger.info(f"✅ Auto-Posted to Channel: {title}")
                except Exception as e:
                    logger.error(f"❌ Channel Post Error '{title}': {e}")
            
            else:
                # 🚨 DB MEIN NAHI HAI -> ADMIN KO ALERT BHEJO
                admin_buttons = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("🔗 TMDB", url=f"https://www.themoviedb.org/{media_type}/{tmdb_id}"),
                        InlineKeyboardButton("🔍 Google", url=f"https://www.google.com/search?q={title.replace(' ', '+')}+download")
                    ]
                ])

                try:
                    if image_url:
                        await app.bot.send_photo(
                            chat_id=admin_id,
                            photo=image_url,
                            caption=text,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=admin_buttons
                        )
                    else:
                        await app.bot.send_message(
                            chat_id=admin_id,
                            text=text,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=admin_buttons,
                            disable_web_page_preview=False
                        )
                    new_alerts += 1
                    logger.info(f"✅ Admin Alert: {title}")
                except Exception as e:
                    logger.error(f"❌ Admin Alert Error '{title}': {e}")

            # Step 4: Dono case mein history save kar do taaki dobara check na ho
            cur.execute(
                """INSERT INTO trending_history 
                   (tmdb_id, title, media_type, popularity, vote_average) 
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (tmdb_id) DO NOTHING""",
                (tmdb_id, title, media_type,
                 item.get('popularity', 0),
                 item.get('vote_average', 0))
            )
            conn.commit()

            await asyncio.sleep(3)

        # Summary Message thoda update kiya auto_posted dekhne ke liye
        try:
            now = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC")
            summary = (
                f"┌─────────────────────────┐\n"
                f"   📊  **TRENDING SUMMARY**\n"
                f"└─────────────────────────┘\n\n"
                f"🕐 **Time:** `{now}`\n"
                f"🔍 **Checked:** `{total_checked}` items\n"
                f"🚀 **Auto-Posted:** `{auto_posted}`\n"
                f"🔁 **Already Handled:** `{skipped_already}`\n"
                f"🆕 **New Alerts:** `{new_alerts}`\n\n"
            )
            await app.bot.send_message(chat_id=admin_id, text=summary, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"❌ Summary Error: {e}")

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"❌ Trending Error: {e}")

    return new_alerts


# ═══════════════════════════════════════════════
#  ♻️ BACKGROUND WORKER — Smart Interval Loop
# ═══════════════════════════════════════════════
async def trending_worker_loop(app, admin_id):
    """Background loop — smart interval based on results"""
    db_ok = setup_trending_db()
    if not db_ok:
        logger.error("❌ Trending worker NOT started — DB issue!")
        return

    logger.info("🚀 Trending Worker Started! Monitoring worldwide trends...")

    # Startup pe ek welcome message
    try:
        await app.bot.send_message(
            chat_id=admin_id,
            text=(
                "┌─────────────────────────┐\n"
                "   🚀  **TRENDING MONITOR ACTIVE**\n"
                "└─────────────────────────┘\n\n"
                "🔄 Har **6 ghante** mein worldwide trends check hoga.\n"
                "🎬 Agar koi nayi movie/show trending hai\n"
                "    jo tere DB mein nahi — tujhe alert milega!\n\n"
                "💤 _Relax. Main kaam kar raha hoon._"
            ),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass

    while True:
        try:
            new_count = await check_and_alert_trending(app, admin_id)

            # 🧠 Smart interval:
            # Zyada nayi cheezein mili → jaldi check karo
            # Kuch nahi mila → aaram se check karo
            if new_count >= 5:
                wait_hours = 4   # Bahut kuch naya hai, 4 ghante baad
            elif new_count >= 1:
                wait_hours = 6   # Kuch naya hai, 6 ghante
            else:
                wait_hours = 8   # Sab covered, 8 ghante

            wait_seconds = wait_hours * 3600
            logger.info(f"⏳ Next check in {wait_hours} hours...")
            await asyncio.sleep(wait_seconds)

        except Exception as e:
            logger.error(f"❌ Worker Loop Error: {e}")
            await asyncio.sleep(3600)  # Error pe 1 ghanta wait
