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

# --------------- Constants ----------------
base_dynamic_fields = {"username", "first_name", "last_name", "email", "country", "city", "language", "gender", "birthdate"}
base_static_fields = {"user_id", "registered_at"}

user_dynamic_fields = {"genres"}
user_static_fields = {"attendance", "notifications"}

dj_dynamic_fields = {"alias", "bio", "country", "phone", "socials"}
dj_static_fields = {"dj_id","interested_count", "notifications", "completed_events_count", "metrics", "language_distribution", "genre_dist"}

venue_dynamic_fields = {"name", "capacity", "table_count"}
venue_static_fields = {"venue_id", "address", "city", "state", "zip", "country", "features", "type_distribution", "language_distribution"}

organizer_dynamic_fields = {"name", "phone", "country", "city", "website"}
organizer_static_fields = {"org_id", "notifications", "features"}


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


# --------------- Streaming Endpoints + Utils ----------------
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

# ADD FEATURED BOOL TO EVENT_DATA; FILTER RESPONSE ACCORDINGLY (j duplicate featured events into seperate object)
@app.get("/events")
async def get_events(
    user_id: Optional[int] = Query(None),
):
    """Fetch all events with their display-relevant data and genres. Public endpoint.
       If user provides JWT, then make sure to include if they're following the event or not."""

    if user_id is None:
        print("Public")
        query = """
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
                v.capacity as venue_capacity,
                o.name as organizer_name,
                (
                    SELECT array_agg(g.name)
                    FROM event_genres eg
                    JOIN genres g ON eg.genre_id = g.id
                    WHERE eg.event_id = e.id
                ) as genres,
                pe.event_poster,
                pe.bio,
                pe.featured,
                pe.sold_out
            FROM published_events pe
            JOIN event_data e ON pe.event_id = e.id
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id;
        """
    else:
        print(f"U id: {user_id}")
        query = """
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
                v.capacity as venue_capacity,
                o.name as organizer_name,
                (
                    SELECT array_agg(g.name)
                    FROM event_genres eg
                    JOIN genres g ON eg.genre_id = g.id
                    WHERE eg.event_id = e.id
                ) as genres,
                pe.event_poster,
                pe.bio,
                pe.featured,
                pe.sold_out,
                CASE 
                    WHEN uef.user_id IS NOT NULL THEN true 
                END AS following
            FROM published_events pe
            JOIN event_data e ON pe.event_id = e.id
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id
            LEFT JOIN user_event_followers uef ON e.id = uef.event_id AND uef.user_id = $1;
        """

    
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        if user_id is not None:
            results = await conn.fetch(query, user_id)
        else:
            results = await conn.fetch(query)

        events = [dict(row) for row in results]
        
        # Create a separate list for featured events
        featured_events = [event for event in events if event.get('featured')]
        
        print(f"Found {len(events)} events, {len(featured_events)} are featured")
        print(events)
        response = {
            "events": events,
            "featured_events": featured_events
        }
        
        json_str = json.dumps(response, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/djs")
async def get_djs(
    user_id: Optional[int] = Query(None)
):
    """Fetch all DJs with their socials and genres. Public endpoint"""
    if user_id is None:
        print("public")
        query = """
            SELECT 
                d.id AS dj_id,
                d.alias,
                d.bio,
                d.country,
                d.city,
                d.interested_count,
                d.created_at,
                d.profile_pic,
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
    else:
        print(f"u id: {user_id}")
        query = """
            SELECT 
                d.id AS dj_id,
                d.alias,
                d.bio,
                d.country,
                d.city,
                d.interested_count,
                d.created_at,
                d.profile_pic,
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
                ) as genres,
                CASE
                    WHEN udf.user_id IS NOT NULL THEN true
                END AS following
            FROM dj d
            LEFT JOIN dj_socials ds ON d.id = ds.dj_id
            LEFT JOIN user_dj_followers udf ON d.id = udf.dj_id AND udf.user_id = $1;
        """

    pool = await db.get_connection()
    async with pool.acquire() as conn:
        if user_id is not None:
            results = await conn.fetch(query, user_id)
        else:
            results = await conn.fetch(query)

        djs = [dict(row) for row in results]
        
        print(f"Found {len(djs)} DJs")
        
        json_str = json.dumps(djs, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/dj/{dj_id}")
async def get_djs(
    dj_id: int,
    user_id: Optional[int] = Query(None)
):
    """Fetch all DJs with their socials and genres. Public endpoint"""
    if user_id is None:
        print("public")
        query = """
            SELECT 
                d.id AS dj_id,
                d.user_id AS user_id,
                d.alias,
                d.bio,
                d.country,
                d.city,
                d.interested_count,
                d.created_at,
                d.profile_pic,
                d.monthly_streams,
                d.social_followers,
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
            LEFT JOIN dj_socials ds ON d.id = ds.dj_id
            WHERE d.id = $1;
        """
    else:
        print(f"U id: {user_id}")
        query = """
            SELECT 
                d.id AS dj_id,
                d.user_id AS user_id,
                d.alias,
                d.bio,
                d.country,
                d.city,
                d.interested_count,
                d.created_at,
                d.profile_pic,
                d.monthly_streams,
                d.social_followers,
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
                ) as genres,
                CASE
                    WHEN udf.user_id IS NOT NULL THEN true
                END AS following
            FROM dj d
            LEFT JOIN dj_socials ds ON d.id = ds.dj_id
            LEFT JOIN user_dj_followers udf ON d.id = udf.dj_id AND udf.user_id = $1
            WHERE d.id = $2;
        """

    pool = await db.get_connection()
    async with pool.acquire() as conn:
        if user_id is not None:
            result = await conn.fetchrow(query, user_id, dj_id)
        else:
            result = await conn.fetchrow(query, dj_id)

        if not result:
            return JSONResponse({"error": "DJ not found"}, status_code=404)
        
        dj = dict(result)
        print(f"Found DJ with id {dj_id}")

        json_str = json.dumps(dj, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))
        

# --------------- Direct Proxy Endpoints ----------------
@app.get("/venues")
async def get_venues():
    """Fetch all venues grouped by country. Public endpoint"""

    query = """
    SELECT 
        id,
        name,
        capacity,
        address,
        city,
        state,
        zip,
        country,
        profile_pic
    FROM venues
    ORDER BY country;
    """
    
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        venues = await conn.fetch(query)
        
        # Group venues by country
        grouped_venues = {}
        for venue in venues:
            venue_dict = dict(venue)
            country = venue_dict['country']
            
            if country not in grouped_venues:
                grouped_venues[country] = []
                
            grouped_venues[country].append({
                "id": venue_dict["id"],
                "name": venue_dict["name"],
                "capacity": venue_dict["capacity"],
                "address": venue_dict["address"],
                "city": venue_dict["city"],
                "state": venue_dict["state"],
                "zip": venue_dict["zip"],
                "country": venue_dict["country"],
                "profile_pic": venue_dict['profile_pic']
            })
        
        print(f"Fetched venues grouped by country: {grouped_venues}")
        json_str = json.dumps(grouped_venues, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/venue/{venue_id}")
async def get_djs(venue_id: int):
    """Fetch all DJs with their socials and genres. Public endpoint"""
    query = """
    SELECT 
        id,
        name,
        capacity,
        address,
        city,
        state,
        zip,
        country,
        table_count,
        created_at, 
        completed_events_count,
        profile_pic,
        (
            SELECT array_agg(vtd.name)
            FROM venue_type_def vtd
            JOIN venue_types vt ON vt.type_id = vtd.id
            WHERE vt.venue_id = v.id
        ) as venue_types
    FROM venues v
    WHERE v.id = $1;
    """

    pool = await db.get_connection()
    async with pool.acquire() as conn:
        result = await conn.fetchrow(query, venue_id)
        if not result:
            return JSONResponse({"error": "Venue not found"}, status_code=404)
        
        venue = dict(result)
        print(f"Found DJ with id {venue_id}")

        json_str = json.dumps(venue, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/event/{event_id}")
async def get_event_details(
    event_id: int,
    user_id: Optional[int] = Query(None)
    ):
    """
        Fetch detailed event info including venue, organizer, and DJs. 
        Public endpoint.
        THIS IS WHERE TICKETING INFO WILL BE DISPLAYED
    """
    if user_id is None:
        print("Public")
        query = """
            SELECT 
                e.id as event_id,
                e.event_name,
                e.date,
                e.pre_event_poster,
                e.pre_bio,
                (
                    SELECT array_agg(g.name)
                    FROM event_genres eg
                    JOIN genres g ON eg.genre_id = g.id
                    WHERE eg.event_id = e.id
                ) as genres,
                v.id as venue_id,
                v.name as venue_name,
                v.capacity,
                v.address,
                v.city,
                v.state,
                v.zip,
                v.country,
                v.type_distribution,
                o.id as organizer_id,
                o.name as organizer_name,
                o.website as organizer_website,
                pe.sold_out,
                pe.event_poster,
                pe.bio,
                pe.published_at,
                CASE 
                    WHEN pe.event_id IS NOT NULL THEN 'Published'
                    ELSE 'Pending'
                END as status,
                (
                    SELECT jsonb_agg(dj_info)
                    FROM (
                        SELECT 
                            d.id AS dj_id,
                            d.alias,
                            d.bio,
                            d.country,
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
                        FROM event_dj ed
                        JOIN dj d ON ed.dj_id = d.id
                        LEFT JOIN dj_socials ds ON d.id = ds.dj_id
                        WHERE ed.event_id = e.id
                    ) dj_info
                ) as djs
            FROM event_data e
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id
            LEFT JOIN published_events pe ON e.id = pe.event_id
            WHERE e.id = $1;
        """
    else:
        print(f"U id: {user_id}")
        query = """
            SELECT 
                e.id as event_id,
                e.event_name,
                e.date,
                e.pre_event_poster,
                e.pre_bio,
                (
                    SELECT array_agg(g.name)
                    FROM event_genres eg
                    JOIN genres g ON eg.genre_id = g.id
                    WHERE eg.event_id = e.id
                ) as genres,
                v.id as venue_id,
                v.name as venue_name,
                v.capacity,
                v.address,
                v.city,
                v.state,
                v.zip,
                v.country,
                v.type_distribution,
                o.id as organizer_id,
                o.name as organizer_name,
                o.website as organizer_website,
                pe.sold_out,
                pe.event_poster,
                pe.bio,
                pe.published_at,
                CASE 
                    WHEN pe.event_id IS NOT NULL THEN 'Published'
                    ELSE 'Pending'
                END as status,
                (
                    SELECT jsonb_agg(dj_info)
                    FROM (
                        SELECT 
                            d.id AS dj_id,
                            d.alias,
                            d.bio,
                            d.country,
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
                        FROM event_dj ed
                        JOIN dj d ON ed.dj_id = d.id
                        LEFT JOIN dj_socials ds ON d.id = ds.dj_id
                        WHERE ed.event_id = e.id
                    ) dj_info
                ) as djs,
                CASE 
                    WHEN uef.user_id IS NOT NULL THEN true 
                END AS following
            FROM event_data e
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id
            LEFT JOIN published_events pe ON e.id = pe.event_id
            LEFT JOIN user_event_followers uef ON e.id = uef.event_id AND uef.user_id = $1
            WHERE e.id = $2;
        """


    pool = await db.get_connection()
    async with pool.acquire() as conn:
        if user_id is not None:
            result = await conn.fetchrow(query, user_id, event_id)
        else:
            result = await conn.fetchrow(query, event_id)

        if not result:
            return JSONResponse({"error": "Event not found"}, status_code=404)
        
        # Convert to dict and parse JSONB fields
        event_dict = dict(result)
        
        # Structure the response
        response = {
            "event_id": event_dict["event_id"],
            "event_name": event_dict["event_name"],
            "date": event_dict["date"],
            "genres": event_dict["genres"] if event_dict["genres"] else None,
            "status": event_dict["status"],
            "following": event_dict['following'] if event_dict['following'] else None,
            "venue": {
                "venue_id": event_dict["venue_id"],
                "name": event_dict["venue_name"],
                "capacity": event_dict["capacity"],
                "address": event_dict["address"],
                "city": event_dict["city"],
                "state": event_dict["state"],
                "zip": event_dict["zip"],
                "country": event_dict["country"],
                "type_distribution": json.loads(event_dict["type_distribution"]) if event_dict["type_distribution"] else None
            },
            "organizer": {
                "organizer_id": event_dict["organizer_id"],
                "name": event_dict["organizer_name"],
                "website": event_dict["organizer_website"]
            },
            "djs": json.loads(event_dict["djs"]) if event_dict["djs"] else []
        }

        # Add appropriate event data based on status
        if event_dict["status"] == "Pending":
            response["unpublished"] = {
                "pre_event_poster": event_dict["pre_event_poster"],
                "pre_bio": event_dict["pre_bio"]
            }
        else:
            response["published"] = {
                "sold_out": event_dict["sold_out"],
                "event_poster": event_dict["event_poster"],
                "bio": event_dict["bio"],
                "published_at": event_dict["published_at"]
            }
        
        print(f"Event details for event {event_id}: {response}")
        json_str = json.dumps(response, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/events/by_dj/{dj_id}")
async def get_djs_events(
    dj_id: int,
    user_id: Optional[int] = Query(None)
):
    """Fetch all events for a specific DJ with the same format as the base events endpoint."""

    if user_id is None:
        print("public")
        query = """
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
                v.capacity as venue_capacity,
                o.name as organizer_name,
                (
                    SELECT array_agg(g.name)
                    FROM event_genres eg
                    JOIN genres g ON eg.genre_id = g.id
                    WHERE eg.event_id = e.id
                ) as genres,
                pe.event_poster,
                pe.bio,
                pe.featured
            FROM published_events pe
            JOIN event_data e ON pe.event_id = e.id
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id
            JOIN event_dj ed ON e.id = ed.event_id
            WHERE ed.dj_id = $1;
        """
    else:
        print(f"U id: {user_id}")
        query = """
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
                v.capacity as venue_capacity,
                o.name as organizer_name,
                (
                    SELECT array_agg(g.name)
                    FROM event_genres eg
                    JOIN genres g ON eg.genre_id = g.id
                    WHERE eg.event_id = e.id
                ) as genres,
                pe.event_poster,
                pe.bio,
                pe.featured,
                CASE 
                    WHEN uef.user_id IS NOT NULL THEN true
                END AS following
            FROM published_events pe
            JOIN event_data e ON pe.event_id = e.id
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id
            JOIN event_dj ed ON e.id = ed.event_id
            LEFT JOIN user_event_followers uef ON e.id = uef.event_id AND uef.user_id = $1
            WHERE ed.dj_id = $2;
        """
    
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        if user_id is not None:
            results = await conn.fetch(query, user_id, dj_id)
        else:
            results = await conn.fetch(query, dj_id)

        events = [dict(row) for row in results]
        
        # Create a separate list for featured events
        featured_events = [event for event in events if event.get('featured')]
        
        print(f"Found {len(events)} events for DJ {dj_id}, {len(featured_events)} are featured")
        
        response = {
            "events": events,
            "featured_events": featured_events
        }
        
        json_str = json.dumps(response, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/events/by_venue/{venue_id}")
async def get_venue_events(
    venue_id: int,
    user_id: Optional[int] = Query(None)
):
    """Fetch all events for a specific venue with the same format as the base events endpoint."""
    if user_id is None:
        print('public')
        query = """
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
                v.capacity as venue_capacity,
                v.profile_pic as profile_pic,
                o.name as organizer_name,
                (
                    SELECT array_agg(g.name)
                    FROM event_genres eg
                    JOIN genres g ON eg.genre_id = g.id
                    WHERE eg.event_id = e.id
                ) as genres,
                pe.event_poster,
                pe.bio,
                pe.featured
            FROM published_events pe
            JOIN event_data e ON pe.event_id = e.id
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id
            WHERE e.venue_id = $1;
        """
    else:
        print(f"U id: {user_id}")
        query = """
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
                v.capacity as venue_capacity,
                v.profile_pic as profile_pic,
                o.name as organizer_name,
                (
                    SELECT array_agg(g.name)
                    FROM event_genres eg
                    JOIN genres g ON eg.genre_id = g.id
                    WHERE eg.event_id = e.id
                ) as genres,
                pe.event_poster,
                pe.bio,
                pe.featured,
                CASE 
                    WHEN uef.user_id IS NOT NULL THEN true
                END AS following
            FROM published_events pe
            JOIN event_data e ON pe.event_id = e.id
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id
            LEFT JOIN user_event_followers uef ON e.id = uef.event_id AND uef.user_id = $1
            WHERE e.venue_id = $2;
        """
    
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        if user_id is not None:
            results = await conn.fetch(query, user_id, venue_id)
        else:
            results = await conn.fetch(query, venue_id)

        events = [dict(row) for row in results]
        
        # Create a separate list for featured events
        featured_events = [event for event in events if event.get('featured')]
        
        print(f"Found {len(events)} events for venue {venue_id}, {len(featured_events)} are featured")
        
        response = {
            "events": events,
            "featured_events": featured_events
        }
        
        json_str = json.dumps(response, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

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


# --------------- Private User-Specific Profile Data; Sensitive ----------------
@app.get("/profile/{user_id}")
async def get_profile_data(user_id: int):
    """Fetch general profile data for all entities"""
    
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        # First get user's basic data
        #             (

        query = """
        SELECT 
            id AS user_id, username, first_name, last_name, email, 
            country, city, language, gender, birthdate, 
            registered_at
        FROM user_data 
        WHERE id = $1;
        """
        profile_data = await conn.fetchrow(query, user_id)
        if not profile_data:
            return JSONResponse({"error": "User not found"}, status_code=404)

        # Format output
        result = dict(profile_data)
        dynamic_data = {key: value for key, value in result.items() if key in base_dynamic_fields}
        static_data = {key: value for key, value in result.items() if key in base_static_fields}

        response = {"dynamic_data": dynamic_data, "static_data": static_data}

        
        return response

@app.get("/user/{user_id}")
async def get_user_profile(user_id: int):
    """Fetch user profile data"""

    pool = await db.get_connection()
    async with pool.acquire() as conn:
        query = """
            SELECT
                id AS user_id, attendance, 
                (
                    SELECT array_agg(g.name)
                    FROM user_genres ug
                    JOIN genres g ON ug.genre_id = g.id
                    WHERE ug.user_id = user_data.id
                ) as genres
            FROM user_data 
            WHERE id = $1;
        """
        user_data = await conn.fetchrow(query, user_id)
        if not user_data:
            return JSONResponse({"error": "User not found"}, status_code=404)
        
        # format output
        result = dict(user_data)
        dynamic_data = {key: value for key, value in result.items() if key in user_dynamic_fields}
        static_data = {key: value for key, value in result.items() if key in user_static_fields}
        response = {"dynamic_data": dynamic_data, "static_data": static_data}   

        return response

@app.get("/dj/{user_id}")
async def get_dj_profile(user_id: int):
    """Fetch DJ-specific profile data."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        query = """
            SELECT
                id AS dj_id,
                alias,
                bio,
                country,
                phone,
                interested_count,
                notifications,
                completed_events_count,
                genre_dist,
                language_distribution,
                metrics,
                (
                    SELECT jsonb_agg(jsonb_build_object(
                                'website', ds.website,
                                'soundcloud', ds.soundcloud,
                                'spotify', ds.spotify,
                                'facebook', ds.facebook,
                                'instagram', ds.instagram,
                                'snapchat', ds.snapchat,
                                'x', ds.x
                    ))
                    FROM dj_socials ds
                    WHERE ds.dj_id = dj.id
                ) AS socials
            FROM dj
            WHERE user_id = $1;
        """
        dj_data = await conn.fetchrow(query, user_id)
        if not dj_data:
            return JSONResponse({"error": "DJ not found"}, status_code=404)

        # Convert to dict and parse JSONB fields
        result = dict(dj_data)
        try:
            if result.get('genre_dist'):
                result['genre_dist'] = json.loads(result['genre_dist'])
            if result.get('language_distribution'):
                result['language_distribution'] = json.loads(result['language_distribution'])
            if result.get('metrics'):
                result['metrics'] = json.loads(result['metrics'])
            if result.get('socials'):
                result['socials'] = json.loads(result['socials'])
        except json.JSONDecodeError as e:
            print(f"Error parsing JSONB fields for DJ {user_id}: {e}")
        
        # format output
        dynamic_data = {key: value for key, value in result.items() if key in dj_dynamic_fields}
        static_data = {key: value for key, value in result.items() if key in dj_static_fields}
        response = {"dynamic_data": dynamic_data, "static_data": static_data}
        
        print(f"DJ profile data for user {user_id}: {response}")
        return response

@app.get("/venue/{user_id}")
async def get_venue_profile(user_id: int):
    """Fetch venue-specific profile data."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        query = """
        SELECT
            id AS venue_id,
            name,
            capacity,
            table_count,
            address,
            city,
            state,
            zip,
            country,
            type_distribution,
            language_distribution,
            features
        FROM venues 
        WHERE user_id = $1;
        """
        venue_data = await conn.fetchrow(query, user_id)
        if not venue_data:
            return JSONResponse({"error": "Venue not found"}, status_code=404)
        
        # Convert to dict and parse JSONB fields
        result = dict(venue_data)
        try:
            if result.get('type_distribution'):
                result['type_distribution'] = json.loads(result['type_distribution'])
            if result.get('language_distribution'):
                result['language_distribution'] = json.loads(result['language_distribution'])
            if result.get('features'):
                result['features'] = json.loads(result['features'])
        except json.JSONDecodeError as e:
            print(f"Error parsing JSONB fields for venue {user_id}: {e}")
        
        # format output
        print(result)
        dynamic_data = {key: value for key, value in result.items() if key in venue_dynamic_fields}
        static_data = {key: value for key, value in result.items() if key in venue_static_fields}
        response = {"dynamic_data": dynamic_data, "static_data": static_data}
        
        print(f"Venue profile data for user {user_id}: {response}")
        return response

@app.get("/organizer/{user_id}")
async def get_organizer_profile(user_id: int):
    """Fetch organizer-specific profile data."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        query = """
            SELECT name, phone, country, city, website, notifications, features, id AS org_id
            FROM organizer 
            WHERE user_id = $1;
        """
        organizer_data = await conn.fetchrow(query, user_id)
        if not organizer_data:
            return JSONResponse({"error": "Organizer not found"}, status_code=404)
        
        result = dict(organizer_data)
        try:
            if result.get('features'):
                result['features'] = json.loads(result['features'])
        except json.JSONDecodeError as e:
            print(f"Error parsing JSONB fields for organizer {user_id}: {e}")

        # format output
        dynamic_data = {key: value for key, value in result.items() if key in organizer_dynamic_fields}
        static_data = {key: value for key, value in result.items() if key in organizer_static_fields}
        response = {"dynamic_data": dynamic_data, "static_data": static_data}
        
        print(f"Organizer profile data for user {user_id}: {response}")
        return response


# --------------- Private User-Specific Event Data; Sensitive ----------------
@app.get("/events/dj/{user_id}")
async def get_dj_events(user_id: int):
    """Fetch events specific to a DJ with three different categories."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        # First get the DJ's ID
        dj_query = """
        SELECT id FROM dj WHERE user_id = $1;
        """
        dj_result = await conn.fetchrow(dj_query, user_id)
        if not dj_result:
            return JSONResponse({"error": "DJ not found"}, status_code=404)
        
        dj_id = dj_result['id']
        print(f"Dj id: {dj_id}")
        
        # Get all event IDs for this DJ
        events_query = """
        WITH dj_events AS (
            SELECT event_id 
            FROM event_dj 
            WHERE dj_id = $1
        ),
        base_event_data AS (
            SELECT 
                e.id as event_id_,
                e.event_name,
                e.date,
                e.pre_event_poster,
                e.pre_bio,
                e.venue_id,
                v.name as venue_name,
                v.address as venue_address,
                v.city as venue_city,
                v.state as venue_state,
                v.zip as venue_zip,
                v.country as venue_country,
                o.id as organizer_id,
                o.name as organizer_name,
                o.phone as organizer_phone,
                o.email as organizer_email,
                o.website as organizer_website,
                pe.completed,
                pe.event_poster,
                pe.bio,
                pe.published_at,
                CASE 
                    WHEN pe.event_id IS NOT NULL THEN 'Published'
                    ELSE 'Pending'
                END as status
            FROM dj_events de
            JOIN event_data e ON de.event_id = e.id
            JOIN venues v ON e.venue_id = v.id
            JOIN organizer o ON e.organizer_id = o.id
            LEFT JOIN published_events pe ON e.id = pe.event_id
        )
        SELECT 
            bed.*,
            cem.* as metrics
        FROM base_event_data bed
        LEFT JOIN completed_event_metrics cem ON bed.event_id_ = cem.event_id;
        """
        
        events = await conn.fetch(events_query, dj_id)
        
        # Organize events into three categories
        completed_events = []
        published_events = []
        unpublished_events = []
        
        for event in events:
            event_dict = dict(event)
            venue_info = {
                "venue_id": event_dict["venue_id"],
                "venue_name": event_dict["venue_name"],
                "venue_address": event_dict["venue_address"],
                "venue_city": event_dict["venue_city"],
                "venue_state": event_dict["venue_state"],
                "venue_zip": event_dict["venue_zip"],
                "venue_country": event_dict["venue_country"]
            }
            organizer_info = {
                "organizer_id": event_dict["organizer_id"],
                "organizer_name": event_dict["organizer_name"],
                "organizer_phone": event_dict["organizer_phone"],
                "organizer_email": event_dict["organizer_email"],
                "organizer_website": event_dict["organizer_website"],
            }
            
            if event_dict.get("completed"):
                completed_events.append({
                    "event_id": event_dict["event_id_"],
                    "event_name": event_dict["event_name"],
                    "date": event_dict["date"],
                    "venue": venue_info,
                    "organizer": organizer_info,
                    "event_poster": event_dict["event_poster"] or event_dict["pre_event_poster"],
                    "bio": event_dict["bio"] or event_dict["pre_bio"],
                    "metrics": event_dict.get("metrics"),
                    "status": event_dict["status"]
                })
            elif event_dict.get("published_at"):
                published_events.append({
                    "event_id": event_dict["event_id_"],
                    "event_name": event_dict["event_name"],
                    "date": event_dict["date"],
                    "venue": venue_info,
                    "organizer": organizer_info,
                    "event_poster": event_dict["event_poster"] or event_dict["pre_event_poster"],
                    "bio": event_dict["bio"] or event_dict["pre_bio"],
                    "published_at": event_dict["published_at"],
                    "status": event_dict["status"]
                })
            else:
                unpublished_events.append({
                    "event_id": event_dict["event_id_"],
                    "event_name": event_dict["event_name"],
                    "date": event_dict["date"],
                    "venue": venue_info,
                    "organizer": organizer_info,
                    "event_poster": event_dict["pre_event_poster"],
                    "bio": event_dict["pre_bio"],
                    "status": event_dict["status"]
                })
        
        result = {
            "completed_events": completed_events,
            "published_events": published_events,
            "unpublished_events": unpublished_events
        }
        
        print(f"DJ events for user {user_id}: {result}")
        # Encode the result with CustomJSONEncoder before creating JSONResponse
        json_str = json.dumps(result, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/events/venue/{user_id}")
async def get_venue_events(user_id: int):
    """Fetch events specific to a venue with three different categories."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        # First get the venue's ID and details
        venue_query = """
        SELECT 
            id,
            name as venue_name,
            address as venue_address,
            city as venue_city,
            state as venue_state,
            zip as venue_zip,
            country as venue_country
        FROM venues 
        WHERE user_id = $1;
        """
        venue_result = await conn.fetchrow(venue_query, user_id)
        if not venue_result:
            return JSONResponse({"error": "Venue not found"}, status_code=404)
        
        venue_id = venue_result['id']
        venue_details = dict(venue_result)
        
        # Get all events for this venue
        events_query = """
        WITH base_event_data AS (
            SELECT 
                e.id as event_id_,
                e.event_name,
                e.date,
                e.pre_event_poster,
                e.pre_bio,
                o.id as organizer_id,
                o.name as organizer_name,
                o.phone as organizer_phone,
                o.email as organizer_email,
                o.website as organizer_website,
                pe.completed,
                pe.event_poster,
                pe.bio,
                pe.published_at,
                CASE 
                    WHEN pe.event_id IS NOT NULL THEN 'Published'
                    ELSE 'Pending'
                END as status,
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'dj_id', d.id,
                            'dj_name', d.alias
                        )
                    )
                    FROM event_dj ed
                    JOIN dj d ON ed.dj_id = d.id
                    WHERE ed.event_id = e.id
                ) as djs
            FROM event_data e
            JOIN organizer o ON e.organizer_id = o.id
            LEFT JOIN published_events pe ON e.id = pe.event_id
            WHERE e.venue_id = $1
        )
        SELECT 
            bed.*,
            cem.* as metrics
        FROM base_event_data bed
        LEFT JOIN completed_event_metrics cem ON bed.event_id_ = cem.event_id;
        """
        
        events = await conn.fetch(events_query, venue_id)
        print(events)
        
        # Organize events into three categories
        completed_events = []
        published_events = []
        unpublished_events = []
        
        for event in events:
            event_dict = dict(event)
            try:
                djs = json.loads(event_dict["djs"]) if event_dict.get("djs") else []
            except (TypeError, json.JSONDecodeError):
                djs = []

            
            venue_info = {
                "venue_id": venue_details["id"],
                "venue_name": venue_details["venue_name"],
                "venue_address": venue_details["venue_address"],
                "venue_city": venue_details["venue_city"],
                "venue_state": venue_details["venue_state"],
                "venue_zip": venue_details["venue_zip"],
                "venue_country": venue_details["venue_country"]
            }
            
            event_info = {
                "event_id": event_dict["event_id_"],
                "event_name": event_dict["event_name"],
                "date": event_dict["date"],
                "venue": venue_info,
                "organizer": {
                    "organizer_id": event_dict["organizer_id"],
                    "organizer_name": event_dict["organizer_name"],
                    "organizer_phone": event_dict["organizer_phone"],
                    "organizer_email": event_dict["organizer_email"],
                    "organizer_website": event_dict["organizer_website"]
                },
                "djs": djs,
                "status": event_dict["status"]
            }
            
            if event_dict.get("completed"):
                completed_events.append({
                    **event_info,
                    "event_poster": event_dict["event_poster"] or event_dict["pre_event_poster"],
                    "bio": event_dict["bio"] or event_dict["pre_bio"],
                    "metrics": event_dict.get("metrics")
                })
            elif event_dict.get("published_at"):
                published_events.append({
                    **event_info,
                    "event_poster": event_dict["event_poster"] or event_dict["pre_event_poster"],
                    "bio": event_dict["bio"] or event_dict["pre_bio"],
                    "published_at": event_dict["published_at"]
                })
            else:
                unpublished_events.append({
                    **event_info,
                    "event_poster": event_dict["pre_event_poster"],
                    "bio": event_dict["pre_bio"]
                })
        
        result = {
            "completed_events": completed_events,
            "published_events": published_events,
            "unpublished_events": unpublished_events
        }
        
        print(f"Venue events for user {user_id}: {result}")
        json_str = json.dumps(result, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/events/organizer/{user_id}")
async def get_organizer_events(user_id: int):
    """Fetch events specific to an organizer with three different categories."""
    pool = await db.get_connection()
    async with pool.acquire() as conn:
        # First get the organizer's ID
        organizer_query = """
        SELECT id FROM organizer WHERE user_id = $1;
        """
        organizer_result = await conn.fetchrow(organizer_query, user_id)
        if not organizer_result:
            return JSONResponse({"error": "Organizer not found"}, status_code=404)
        
        organizer_id = organizer_result['id']
        
        # Get all events for this organizer
        events_query = """
        WITH base_event_data AS (
            SELECT 
                e.id as event_id,
                e.event_name,
                e.date,
                e.pre_event_poster,
                e.pre_bio,
                v.id as venue_id,
                v.name as venue_name,
                v.address as venue_address,
                v.city as venue_city,
                v.state as venue_state,
                v.zip as venue_zip,
                v.country as venue_country,
                pe.completed,
                pe.event_poster,
                pe.bio,
                pe.published_at,
                CASE 
                    WHEN pe.event_id IS NOT NULL THEN 'Published'
                    ELSE 'Pending'
                END as status,
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'dj_id', d.id,
                            'dj_name', d.alias
                        )
                    )
                    FROM event_dj ed
                    JOIN dj d ON ed.dj_id = d.id
                    WHERE ed.event_id = e.id
                ) as djs
            FROM event_data e
            JOIN venues v ON e.venue_id = v.id
            LEFT JOIN published_events pe ON e.id = pe.event_id
            WHERE e.organizer_id = $1
        )
        SELECT 
            bed.*,
            cem.completed_at as metrics_completed_at,
            cem.fill_ratio as metrics_fill_ratio,
            cem.ctr as metrics_ctr,
            cem.conversion_rate as metrics_conversion_rate,
            cem.table_rate as metrics_table_rate,
            cem.ticket_revenue as metrics_ticket_revenue,
            cem.table_revenue as metrics_table_revenue,
            cem.avg_age as metrics_avg_age,
            cem.gender_ratio as metrics_gender_ratio,
            cem.english_ratio as metrics_english_ratio,
            cem.spanish_ratio as metrics_spanish_ratio,
            cem.dutch_ratio as metrics_dutch_ratio
        FROM base_event_data bed
        LEFT JOIN completed_event_metrics cem ON bed.event_id = cem.event_id;
        """
        
        events = await conn.fetch(events_query, organizer_id)

        completed_events = []
        published_events = []
        unpublished_events = []
        
        # Process events
        for event in events:
            event_dict = dict(event)
            try:
                djs = json.loads(event_dict["djs"]) if event_dict.get("djs") else []
            except (TypeError, json.JSONDecodeError):
                djs = []
            
            venue_info = {
                "venue_id": event_dict["venue_id"],
                "venue_name": event_dict["venue_name"],
                "venue_address": event_dict["venue_address"],
                "venue_city": event_dict["venue_city"],
                "venue_state": event_dict["venue_state"],
                "venue_zip": event_dict["venue_zip"],
                "venue_country": event_dict["venue_country"]
            }
            
            metrics = None
            if event_dict.get("metrics_completed_at"):
                metrics = {
                    "completed_at": event_dict["metrics_completed_at"],
                    "fill_ratio": event_dict["metrics_fill_ratio"],
                    "ctr": event_dict["metrics_ctr"],
                    "conversion_rate": event_dict["metrics_conversion_rate"],
                    "table_rate": event_dict["metrics_table_rate"],
                    "ticket_revenue": event_dict["metrics_ticket_revenue"],
                    "table_revenue": event_dict["metrics_table_revenue"],
                    "avg_age": event_dict["metrics_avg_age"],
                    "gender_ratio": event_dict["metrics_gender_ratio"],
                    "english_ratio": event_dict["metrics_english_ratio"],
                    "spanish_ratio": event_dict["metrics_spanish_ratio"],
                    "dutch_ratio": event_dict["metrics_dutch_ratio"]
                }
            
            event_info = {
                "event_id": event_dict["event_id"],
                "event_name": event_dict["event_name"],
                "date": event_dict["date"],
                "venue": venue_info,
                "djs": djs,
                "status": event_dict["status"],
                "event_poster": (event_dict["event_poster"] or event_dict["pre_event_poster"]) if event_dict["status"] == "Published" else event_dict["pre_event_poster"],
                "bio": (event_dict["bio"] or event_dict["pre_bio"]) if event_dict["status"] == "Published" else event_dict["pre_bio"]
            }
            
            if event_dict.get("completed"):
                event_info["metrics"] = metrics
                event_info["published_at"] = event_dict["published_at"]
            
            if event_dict.get("completed"):
                completed_events.append(event_info)
            elif event_dict.get("published_at"):
                published_events.append(event_info)
            else:
                unpublished_events.append(event_info)
        
        result = {
            "completed_events": completed_events,
            "published_events": published_events,
            "unpublished_events": unpublished_events
        }
        
        print(f"Organizer events for user {user_id}: {result}")
        json_str = json.dumps(result, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("db_reads:app", host="0.0.0.0", port=8001, reload=True)

