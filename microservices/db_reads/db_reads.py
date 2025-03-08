from fastapi import FastAPI, Query, Depends, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import AsyncGenerator, Dict, Optional

import asyncpg
import asyncio
import json
import os
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

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

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

    async def get_connection(self):
        """Get a connection from the pool."""
        if not self.pool:
            await self.connect()
        return self.pool


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
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for record in conn.cursor(query, *params):
                yield json.dumps(
                    dict(record), 
                    cls=CustomJSONEncoder
                ) + "\n"

@app.get("/events", response_class=StreamingResponse)
async def get_events():
    """Stream all events with their display-relevant data and genres."""

    query = """
    WITH base_events AS (
        SELECT 
            e.id,
            e.event_name,
            e.date,
            v.name as venue_name,
            v.address as venue_address,
            v.city as venue_city,
            v.state as venue_state,
            v.zip as venue_zip,
            v.country as venue_country,
            o.name as organizer_name,
            (
                SELECT array_agg(g.name)
                FROM event_genres eg
                JOIN genres g ON eg.genre_id = g.id
                WHERE eg.event_id = e.id
            ) as genres,
            pe.event_poster,
            pe.bio
        FROM event_data e
        JOIN venues v ON e.venue_id = v.id
        JOIN organizer o ON e.organizer_id = o.id
        LEFT JOIN published_events pe ON e.id = pe.event_id
    )
    SELECT * FROM base_events;
    """
    
    return StreamingResponse(stream_query(query), media_type="application/json")

@app.get("/djs", response_class=StreamingResponse)
async def get_djs():
    """Stream all DJs with their socials and genres."""
    query = """
    SELECT 
        d.alias,
        d.first_name,
        d.last_name,
        d.bio,
        d.location,
        d.interested_count,
        d.created_at,
        ds.website,
        ds.soundcloud,
        ds.spotify,
        ds.facebook,
        ds.instagram,
        ds.snapchat,
        ds.x,
        (
            SELECT array_agg(g.name)
            FROM dj_genres dg
            JOIN genres g ON dg.genre_id = g.id
            WHERE dg.dj_id = d.id
        ) as genres
    FROM dj d
    LEFT JOIN dj_socials ds ON d.id = ds.dj_id;
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
    pool = await db.get_connection()
    result = await pool.fetchrow(query, event_id)
    if not result:
        return JSONResponse({"error": "Event not found"}, status_code=404)
    return dict(result)
    
@app.get("/user_recommendation_data/{user_id}")
async def get_user_recommendation_data(user_id: int) -> Dict:
    """Fetch and aggregate user event history data for recommendations."""
    
    # Check Redis cache first
    redis_key = f"user_rec_data:{user_id}"
    cached_data = redis_client.get(redis_key)
    if cached_data:
        print(f"Cache hit for user {user_id}")
        return json.loads(cached_data)
    
    print(f"Cache miss for user {user_id}, querying database...")
    
    pool = await db.get_connection()
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
        
        try:
            # Cache for 6 hours; change to 3 in production
            redis_client.setex(
                redis_key,
                21600, 
                json.dumps(result)
            )
            print(f"Cached recommendation data for user {user_id}")
        except Exception as e:
            print(f"Failed to cache data: {e}")
        
    return result


# --------------- Private User-Specific Endpoints ----------------
@app.get("/profile/{user_id}")
async def get_profile_data(user_id: int):
    """Fetch user profile data including roles and role-specific information."""
    
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        # First get user's basic data
        user_query = """
        SELECT 
            username, first_name, last_name, email, 
            location, language, gender, birthdate, 
            registered_at,
            (
                SELECT array_agg(g.name)
                FROM user_genres ug
                JOIN genres g ON ug.genre_id = g.id
                WHERE ug.user_id = user_data.id
            ) as genres
        FROM user_data 
        WHERE id = $1;
        """
        user_data = await conn.fetchrow(user_query, user_id)
        if not user_data:
            return JSONResponse({"error": "User not found"}, status_code=404)

        # Get user's role
        role_query = """
        SELECT role_id, status
        FROM user_roles
        WHERE user_id = $1;
        """
        role_data = await conn.fetchrow(role_query, user_id)
        
        result = dict(user_data)
        if role_data:
            result["role_id"] = role_data["role_id"]
            result["status"] = role_data["status"]
        
        return result

@app.get("/dj/{user_id}")
async def get_dj_profile(user_id: int):
    """Fetch DJ-specific profile data."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        query = """
        SELECT 
            bio,
            interested_count,
            notifications,
            phone,
            completed_events_count,
            genre_dist,
            language_distribution,
            metrics
        FROM dj 
        WHERE user_id = $1;
        """
        result = await conn.fetchrow(query, user_id)
        if not result:
            return JSONResponse({"error": "DJ not found"}, status_code=404)
        
        # Convert to dict and parse JSONB fields
        dj_data = dict(result)
        try:
            if dj_data.get('genre_dist'):
                dj_data['genre_dist'] = json.loads(dj_data['genre_dist'])
            if dj_data.get('language_distribution'):
                dj_data['language_distribution'] = json.loads(dj_data['language_distribution'])
            if dj_data.get('metrics'):
                dj_data['metrics'] = json.loads(dj_data['metrics'])
        except json.JSONDecodeError as e:
            print(f"Error parsing JSONB fields for DJ {user_id}: {e}")
        
        print(f"DJ profile data for user {user_id}: {dj_data}")
        return dj_data

@app.get("/venue/{user_id}")
async def get_venue_profile(user_id: int):
    """Fetch venue-specific profile data."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        query = """
        SELECT 
            capacity,
            address,
            city,
            state,
            zip,
            country,
            table_count,
            completed_events_count,
            type_distribution,
            language_distribution,
            features
        FROM venues 
        WHERE user_id = $1;
        """
        result = await conn.fetchrow(query, user_id)
        if not result:
            return JSONResponse({"error": "Venue not found"}, status_code=404)
        
        # Convert to dict and parse JSONB fields
        venue_data = dict(result)
        try:
            if venue_data.get('type_distribution'):
                venue_data['type_distribution'] = json.loads(venue_data['type_distribution'])
            if venue_data.get('language_distribution'):
                venue_data['language_distribution'] = json.loads(venue_data['language_distribution'])
            if venue_data.get('features'):
                venue_data['features'] = json.loads(venue_data['features'])
        except json.JSONDecodeError as e:
            print(f"Error parsing JSONB fields for venue {user_id}: {e}")
        
        print(f"Venue profile data for user {user_id}: {venue_data}")
        return venue_data

@app.get("/organizer/{user_id}")
async def get_organizer_profile(user_id: int):
    """Fetch organizer-specific profile data."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        query = """
        SELECT website
        FROM organizer 
        WHERE user_id = $1;
        """
        result = await conn.fetchrow(query, user_id)
        if not result:
            return JSONResponse({"error": "Organizer not found"}, status_code=404)
        
        organizer_data = dict(result)
        print(f"Organizer profile data for user {user_id}: {organizer_data}")
        return organizer_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("db_reads:app", host="0.0.0.0", port=8001, reload=True)

