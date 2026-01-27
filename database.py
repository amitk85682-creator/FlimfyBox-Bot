import os
import logging
import asyncpg
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool = None
        self.db_url = os.environ.get("DATABASE_URL")

    async def connect(self):
        """Initializes the connection pool"""
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(
                    dsn=self.db_url,
                    min_size=1,
                    max_size=10, # Keep connections ready
                    command_timeout=60
                )
                logger.info("✅ Database pool created successfully.")
            except Exception as e:
                logger.error(f"❌ Failed to create DB pool: {e}")
                raise e

    async def close(self):
        """Closes the pool"""
        if self.pool:
            await self.pool.close()
            logger.info("🛑 Database pool closed.")

    async def get_movie_by_title(self, query):
        """Example of fetching data asynchronously"""
        sql = """
            SELECT id, title, url, file_id, poster_url, year, genre 
            FROM movies 
            WHERE title ILIKE $1 
            LIMIT 10
        """
        # Note: asyncpg uses $1, $2, not %s
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(sql, f"%{query}%")
                return rows # Returns a list of Record objects
        except Exception as e:
            logger.error(f"DB Error: {e}")
            return []

    async def add_request(self, user_id, username, movie_title):
        sql = """
            INSERT INTO user_requests (user_id, username, movie_title)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, movie_title) DO NOTHING
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(sql, user_id, username, movie_title)
                return True
        except Exception as e:
            logger.error(f"DB Insert Error: {e}")
            return False

# Create a singleton instance
db = Database()
