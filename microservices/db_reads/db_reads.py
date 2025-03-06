from fastapi import FastAPI, Query, Depends, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import asyncpg
import asyncio
import json
import os
from typing import AsyncGenerator, Dict, Optional
import redis

load_dotenv()

POSTGRE_DB = os.getenv("POSTGRE_DB")
POSTGRE_USER = os.getenv("POSTGRE_USER")
POSTGRE_PW = os.getenv("POSTGRE_PW")
POSTGRE_HOST = os.getenv("POSTGRE_HOST")
# POSTGRE_READ_PORT = os.getenv("POSTGRE_READ_PORT")
POSTGRE_WRITE_PORT = os.getenv("POSTGRE_WRITE_PORT")


REDIS_HOST = os.getenv("REDIS_HOST", "driven-robin-54477.upstash.io")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                database=POSTGRE_DB,
                user=POSTGRE_USER,
                password=POSTGRE_PW,
                host=POSTGRE_HOST,
                port=POSTGRE_WRITE_PORT,
                min_size=3,
                max_size=5,
                command_timeout=60,
                max_inactive_connection_lifetime=300.0
            )

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def get_connection(self) -> AsyncGenerator[asyncpg.Pool, None]:
        if not self.pool:
            await self.connect()
        try:
            yield self.pool
        finally:
            pass


# APP DEFINITION
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    print("✅ Database pool created")
    
    # Initialize Redis connection
    global redis_client
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True,
        decode_responses=True  # Automatically decode responses to Python strings
    )
    
    yield

    await db.disconnect()
    print("🛑 Database pool closed")

app = FastAPI(lifespan=lifespan)
db = Database()


# --------------- Streaming Endpoints ----------------

async def stream_query(query: str, *params):
    """Helper function to stream query results as JSON."""
    async with db.get_connection() as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor(query, *params):
                    yield json.dumps(dict(record)) + "\n"

@app.get("/events", response_class=StreamingResponse)
async def get_events(pool=Depends(db.get_connection)):
    """Stream all events from event_data & published_events."""

    query = """
    SELECT * FROM event_data
    UNION ALL
    SELECT * FROM published_events;
    """
    return StreamingResponse(stream_query(query), media_type="application/json")

@app.get("/djs", response_class=StreamingResponse)
async def get_djs():
    """Stream all DJs with their socials."""
    query = """
    SELECT dj.*, dj_socials.*
    FROM dj
    LEFT JOIN dj_socials ON dj.id = dj_socials.dj_id;
    """
    return StreamingResponse(stream_query(query), media_type="application/json")

# --------------- Direct Proxy Endpoints ----------------
@app.get("/event/{event_id}")
async def get_event_details(event_id: int):
    """Fetch detailed event info including venue & organizer."""

    query = """
    SELECT pe.*, v.*, o.*
    FROM event_data pe
    LEFT JOIN venues v ON pe.venue_id = v.id
    LEFT JOIN organizer o ON pe.organizer_id = o.id
    WHERE pe.id = $1
    """
    async with db.get_connection() as pool:
        result = await pool.fetchrow(query, event_id)
        if not result:
            return JSONResponse({"error": "Event not found"}, status_code=404)
        return dict(result)
    
@app.get("/user_recommendation_data/{user_id}")
async def get_user_recommendation_data(
    user_id: int,
    pool=Depends(db.get_connection)
) -> Dict:
    """Fetch and aggregate user event history data for recommendations."""
    async with pool.acquire() as conn:
        # First get user's core data and genres
        user_query = """
        SELECT 
            core::json,
            genres::json
        FROM user_data 
        WHERE id = $1;
        """
        user_data = await conn.fetchrow(user_query, user_id)
        
        # Initialize result with user data
        result = {
            "user_id": user_id,
            "core": dict(user_data)['core'] if user_data else None,
            "genres": dict(user_data)['genres'] if user_data else None,
            "events": []
        }

        # Get event history if it exists
        purchase_query = """
        SELECT DISTINCT event_id 
        FROM purchase 
        WHERE user_id = $1;
        """
        event_ids = await conn.fetch(purchase_query, user_id)
        
        if event_ids:  # Only process events if they exist
            for record in event_ids:
                event_id = record['event_id']
                
                event_query = """
                SELECT 
                    id as event_id,
                    genre_dist::json,
                    venue_id
                FROM event_data 
                WHERE id = $1;
                """
                event_data = await conn.fetchrow(event_query, event_id)
                
                if event_data:
                    event_dict = dict(event_data)
                    
                    venue_query = """
                    SELECT 
                        language_distribution::json,
                        type_distribution::json,
                        features::json
                    FROM venues 
                    WHERE id = $1;
                    """
                    venue_data = await conn.fetchrow(venue_query, event_data['venue_id'])
                    if venue_data:
                        event_dict['venue'] = dict(venue_data)
                    
                    dj_query = """
                    SELECT dj_id 
                    FROM event_dj 
                    WHERE event_id = $1;
                    """
                    dj_ids = await conn.fetch(dj_query, event_id)
                    
                    event_dict['djs'] = []
                    for dj_record in dj_ids:
                        dj_id = dj_record['dj_id']
                        
                        dj_data_query = """
                        SELECT 
                            metrics::json,
                            language_distribution::json,
                            genre_dist::json
                        FROM dj 
                        WHERE id = $1;
                        """
                        dj_data = await conn.fetchrow(dj_data_query, dj_id)
                        if dj_data:
                            event_dict['djs'].append(dict(dj_data))
                    
                    result['events'].append(event_dict)
        
        # Helper function to parse JSON strings
        def parse_json_fields(data):
            if isinstance(data, dict):
                return {k: parse_json_fields(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [parse_json_fields(item) for item in data]
            elif isinstance(data, str):
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return data
            return data

        # Parse any JSON strings in the result
        result = parse_json_fields(result)
        
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("db_reads:app", host="0.0.0.0", port=8001, reload=True)

