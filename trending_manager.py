import asyncio
import requests
import logging
import os
import psycopg2

logger = logging.getLogger(__name__)
TMDB_API_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7" # Tera existing TMDB key
DATABASE_URL = os.environ.get('DATABASE_URL')

def setup_trending_db():
    """Bot start hote hi history table check/create karega"""
    if not DATABASE_URL:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        # Ye table yaad rakhegi ki kis movie ka msg tuje bhej diya gaya hai
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_history (
                tmdb_id INTEGER PRIMARY KEY,
                title TEXT,
                alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Trending DB Setup Error: {e}")

async def check_and_alert_trending(app, admin_id):
    logger.info("🔍 Checking Trending Movies Worldwide...")
    try:
        url = f"https://api.themoviedb.org/3/trending/all/day?api_key={TMDB_API_KEY}"
        resp = requests.get(url, timeout=10).json()
        trending_items = resp.get('results', [])[:10] # Top 10 movies check karega
        
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        for item in trending_items:
            tmdb_id = item.get('id')
            title = item.get('title') or item.get('name')
            media_type = item.get('media_type', 'Movie').capitalize()
            
            if not title: continue
            
            # 1. Kya ye main 'movies' table me pehle se hai?
            cur.execute("SELECT id FROM movies WHERE title ILIKE %s LIMIT 1", (f"%{title}%",))
            if cur.fetchone():
                continue # DB me hai, skip karo

            # 2. Kya main iska alert tujhe pehle bhej chuka hoon?
            cur.execute("SELECT tmdb_id FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
            if cur.fetchone():
                continue # Pehle bata chuka hoon, skip karo

            # 3. Nayi movie mili! Alert bhejo aur History me daalo
            alert_text = (
                f"🚨 **New Trending Alert!**\n\n"
                f"🎬 **Name:** `{title}`\n"
                f"📺 **Type:** {media_type}\n\n"
                f"Yeh abhi worldwide trending hai aur tere DB mein nahi hai. Check kar le!"
            )
            
            try:
                await app.bot.send_message(chat_id=admin_id, text=alert_text, parse_mode='Markdown')
                
                # History me save kar lo taaki next time message na aaye
                cur.execute("INSERT INTO trending_history (tmdb_id, title) VALUES (%s, %s)", (tmdb_id, title))
                conn.commit()
            except Exception as e:
                logger.error(f"Trending Alert Send Error: {e}")
            
            await asyncio.sleep(2) # Spam block se bachne ke liye
            
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Trending Logic Error: {e}")

async def trending_worker_loop(app, admin_id):
    """Ye background me har 8 ghante me ghoomta rahega"""
    setup_trending_db() 
    while True:
        await check_and_alert_trending(app, admin_id)
        await asyncio.sleep(28800) # Har 8 ghante me ek baar check karega (8 * 60 * 60)
