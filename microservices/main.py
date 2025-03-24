from fastapi import FastAPI, Request, HTTPException, Depends, Body, BackgroundTasks, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from jose import jwt, JWTError, ExpiredSignatureError
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from google.protobuf.timestamp_pb2 import Timestamp
from contextlib import asynccontextmanager
from db_writes import write_service_pb2, write_service_pb2_grpc
from dotenv import load_dotenv
from background_writes.celery_worker import publish_metric
from enum import Enum
from asyncio import create_task, TimeoutError
import os
import grpc
import os
import asyncio
import asyncpg
import aiomysql
import requests
import base64
import json
import httpx
import time
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer



load_dotenv(override=True, dotenv_path='/Users/borisgans/personal/yaya/yaya_dev/.env')


# POSTGRE
POSTGRE_DB = os.getenv("POSTGRE_DB")
POSTGRE_USER = os.getenv("POSTGRE_USER")
POSTGRE_PW = os.getenv("POSTGRE_PW")
POSTGRE_HOST = os.getenv("POSTGRE_HOST")
POSTGRE_WRITE_PORT = os.getenv("POSTGRE_WRITE_PORT")
# POSTGRE_READ_PORT = os.getenv("POSTGRE_READ_PORT")
POSTGRE_READ_PORT = POSTGRE_WRITE_PORT
# temp for local db

# JWT
SECRET_KEY = os.getenv("SECRET_KEYS_CURRENT")
SECRET_KEY_PREVIOUS = os.getenv("SECRET_KEYS_PREVIOUS")
ALGORITHM = os.getenv("JWT_ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS"))

# GROC
GRPC_INSC_CHANNEL = os.getenv("GRPC_INSC_CHANNEL")

# ENDPOINTS
DB_READER_SERVICE_URL = os.getenv("DB_READER_SERVICE_URL")
RECOMMENDATION_SERVICE_URL = os.getenv("RECOMMENDATION_SERVICE_URL")


print(f"Connection details: {POSTGRE_DB, POSTGRE_USER, POSTGRE_PW, POSTGRE_HOST, POSTGRE_WRITE_PORT}")


user_data = {}
sensitive_data = {}
body_data = {}
db_pool = None

class MetricType(Enum):
    CLICK = "click"
    IMPRESSION = "impression"
    SHARE = "share"
    SAVE = "save"

ROLE_IDS = {
    "USER": 1,
    "DJ": 2,
    "ORGANIZER": 3,
    "VENUE": 4
}


# gRPC Channel to the write microservice
grpc_channel = grpc.insecure_channel(GRPC_INSC_CHANNEL)
grpc_stub = write_service_pb2_grpc.WriteServiceStub(grpc_channel)


# APP DEFINITION
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(
        database=POSTGRE_DB,
        user=POSTGRE_USER,
        password=POSTGRE_PW,
        host=POSTGRE_HOST,
        port=POSTGRE_READ_PORT,
        min_size=1,
        max_size=3
    )
    print("✅ Database pool created")
    
    yield  # This is where the app runs

    await db_pool.close()
    print("🛑 Database pool closed")

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# --------------- Write DB Operations - GRPC channels ----------------
def handle_event(data):
    date_str = data.get("date")
    date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))  # Handle UTC format
    timestamp = Timestamp()
    timestamp.FromDatetime(date_obj)
    data['date'] = timestamp
    print(f"Sync data: {data}")


    request = write_service_pb2.CreateEventRequest(data=data)
    response = grpc_stub.CreateEvent(request)
    return {"Success": response.success, "Message": response.message}

def handle_venue(data):
    print(f"Sync data: {data}")

    request = write_service_pb2.CreateVenueRequest(data=data)
    response = grpc_stub.CreateVenue(request)
    return {"Success": response.success, "Message": response.message}

def handle_user(data):
    print(f"Sync data: {data}")

    request = write_service_pb2.CreateUserRequest(data=data)
    response = grpc_stub.CreateUser(request)
    return {"Success": response.success, "Message": response.message}

def handle_dj(data):
    print(f"Sync data: {data}")

    request = write_service_pb2.CreateDJRequest(data=data)
    response = grpc_stub.CreateDj(request)
    return {"Success": response.success, "Message": response.message}

def handle_org(data):
    print(f"Sync data: {data}")

    request = write_service_pb2.CreateOrganizerRequest(data=data)
    response = grpc_stub.CreateOrganizer(request)
    return {"Success": response.success, "Message": response.message}

def handle_publish(data):
    print(f"Sync data: {data}")

    request = write_service_pb2.CreatePublishRequest(data=data)
    response = grpc_stub.PublishEvent(request)
    return {"Success": response.success, "Message": response.message}

def handle_dj_event(data):
    print(f"Sync data: {data}")

    request = write_service_pb2.CreateDjEventRequest(data=data)
    response = grpc_stub.AddDjEvent(request)
    return {"Success": response.success, "Message": response.message}

def handle_event_delete(data):
    print(f"Sync data: {data}")

    request = write_service_pb2.DeleteEventRequest(data=data)
    response = grpc_stub.DeleteEvent(request)
    return {"Success": response.success, "Message": response.message}

private_handlers = {
    "event": handle_event,
    "publish_event": handle_publish,
    "dj_event": handle_dj_event,
    "delete_event": handle_event_delete
}

public_handlers = {
    "venue": handle_venue,
    "user": handle_user,
    "dj": handle_dj,
    "org": handle_org, 
}


# --------------- JWT Util Functions ----------------
security = HTTPBearer()

# Decode and validate JWT; return user data encoded in token
async def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)):
    try:
        token = credentials.credentials
        success, payload = decode_jwt(token)
        if not success:
            print("ACCESS DENIED")
            raise HTTPException(status_code=401, detail=payload)
        
        decoded_data = json.loads(base64.b64decode(payload["data"]).decode("utf-8"))
        if not decoded_data.get('username'):
            print("ACCESS DENIED")
            raise HTTPException(status_code=401, detail="Invalid authentication token")
        
        # role_id will now be available in the decoded data
        return decoded_data
    except Exception as e:
        print("ACCESS DENIED")
        raise HTTPException(status_code=401, detail=str(e))

def rotate_keys():
    print("dont use this function")
    # SECRET_KEYS["previous"] = SECRET_KEYS["current"]  # Move current to previous
    # SECRET_KEYS["current"] = os.urandom(32).hex()  # Generate new key
    # print(f"New secret key: {SECRET_KEYS['current']}")

def create_jwt(data: dict, expires_delta: Optional[timedelta] = None):
    # Only include these specific fields in the encoded data
    print(f"Data: {data}")

    to_encode_data = {
        'id': data['id'],
        'username': data['username'],
        'role_id': data.get('role_id'),
        'location': data.get('location'),
        'language': data.get('language')
    }
    if data.get('role_id') != 1:
        to_encode_data['other_id'] = data.get('other_id')
    
    # Create the JWT payload with only exp and the encoded data
    jwt_payload = {}
    
    if expires_delta:
        jwt_payload["exp"] = int((datetime.now(timezone.utc) + expires_delta).timestamp())
    else:
        jwt_payload["exp"] = int((datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)).timestamp())
    
    # Encode only the specified fields
    bytes = base64.b64encode(json.dumps(to_encode_data).encode('utf-8')).decode('utf-8')
    jwt_payload['data'] = bytes

    print(f"Creating jwt with: {jwt_payload}")
    print(f"Encoded data: {to_encode_data}")

    return jwt.encode(jwt_payload, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(data: dict):
    expire = int((datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)).timestamp())
    to_encode = {"exp": expire, **data}
    return jwt.encode(to_encode, SECRET_KEY_PREVIOUS, algorithm=ALGORITHM)

def decode_jwt(token: str) -> Dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        exp = payload.get("exp")
        current_time = int(datetime.now(timezone.utc).timestamp())
        print(f"Time to expiration: {exp - current_time}")
        if current_time > exp:
            return(False, "Token has expired")

        decoded = json.loads(base64.b64decode(payload["data"]).decode("utf-8"))
        print(f"Payload at decoding: {payload}")
        print(f"Base64 data: {decoded}")
        return (True, payload)

    except ExpiredSignatureError:
        return (False, "Token has expired")

    except JWTError:
        try:
            payload = jwt.decode(token, SECRET_KEY_PREVIOUS, algorithms=[ALGORITHM])
            decoded = json.loads(base64.b64decode(payload["data"]).decode("utf-8"))

            print(f"Payload at decoding: {payload}")
            print(f"Base64 data: {decoded}")
            return (True, payload)

        except:
            return (False, "Invalid token")
    
def verify_refresh_token(refresh_token: str):
    try:
        payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        print(f"\nPayload: {payload}")
        return (True, payload)
    except JWTError as err:
        print(f"\nError: {err}")
        try:
            payload = jwt.decode(refresh_token, SECRET_KEY_PREVIOUS, algorithms=[ALGORITHM])
            return (True, payload)
        except:
            return (False, "Invalid token")
        # Do I need to check the previous key for refresh tokens?? 


# -------------------- Auth Functions -----------------------
async def confirm_login_postgres(username_or_email: str, pw: str):
    global db_pool
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database connection failed")

    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT 
                    ud.id, 
                    ud.username, 
                    ud.first_name, 
                    ud.last_name, 
                    ud.email, 
                    ud.location, 
                    ud.language,
                    ur.role_id
                FROM user_data ud
                LEFT JOIN user_roles ur ON ud.id = ur.user_id
                WHERE (ud.username = $1 OR ud.email = $1) AND ud.pw = $2
                """,
                username_or_email, pw
            )
            if not row:
                raise HTTPException(status_code=401, detail="Invalid credentials")
            user_data = dict(row)

            if user_data.get('role_id') == 2:
                dj_id = await conn.fetchrow(
                    """
                    SELECT 
                        id AS other_id
                    FROM dj
                    WHERE user_id = $1
                    """, user_data.get('id')
                )
                if not dj_id:
                    raise HTTPException(status_code=401, detail="Couldn't fetch venue id")
                added_info = dict(dj_id)

                print(added_info)
                user_data['other_id'] = added_info.get('other_id')
            elif user_data.get('role_id') == 3:
                org_id = await conn.fetchrow(
                    """
                    SELECT 
                        id AS other_id
                    FROM organizer
                    WHERE user_id = $1
                    """, user_data.get('id')
                )
                if not org_id:
                    raise HTTPException(status_code=401, detail="Couldn't fetch venue id")
                added_info = dict(org_id)

                print(added_info)
                user_data['other_id'] = added_info.get('other_id')
            elif user_data.get('role_id') == 4:
                ven_id = await conn.fetchrow(
                    """
                    SELECT 
                        id AS other_id
                    FROM venues
                    WHERE user_id = $1
                    """, user_data.get('id')
                )
                if not ven_id:
                    raise HTTPException(status_code=401, detail="Couldn't fetch venue id")
                added_info = dict(ven_id)

                print(added_info)
                user_data['other_id'] = added_info.get('other_id')
            
            return user_data
    except asyncpg.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")


@app.post("/login")
async def login(creds: dict = Body(...)):
    identifier = creds.get("username")  # This could be either username or email
    pw = creds.get("pw")
    if not identifier or not pw:
        raise HTTPException(status_code=401, detail="Missing credentials")
    
    user_data = await confirm_login_postgres(identifier, pw)
    # Make sure username is included in user_data
    if 'username' not in user_data:
        user_data['username'] = user_data.get('email', identifier)
    
    token = create_jwt(user_data, timedelta(minutes=int(ACCESS_TOKEN_EXPIRE_MINUTES)))
    refresh_token = create_refresh_token(user_data)
    return {
        "access_token": token, 
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }

@app.post("/refresh")
def refresh_access_token(body: dict=Body(...)):
    refresh_token = body.get("refresh_token")
    print(f"Refresh token: {refresh_token}")
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    payload = verify_refresh_token(refresh_token)
    if not payload[0]:
        print(f"Error with verifying refresh token")
        raise HTTPException(status_code=401, detail=payload[1])

    new_access_token = create_jwt(payload[1])
    return {"access_token": new_access_token}


# --------------- Write Endpoints ----------------
@app.post("/essential_write/register")
async def essential_write_register(data: dict = Body(...)):
    """
    Handles entity creation/registration (users, DJs, venues, organizers).
    No authentication required.
    """

    obj_type = data.get("type")
    obj_data = data.get("data")
    print(obj_data)
    
    handler = public_handlers.get(obj_type, lambda x: {"error": f"Unknown type: {obj_type}"})  
    response = handler(obj_data)
    print(response)
    if response.get('Success') == 'false':
        raise HTTPException(status_code=500, detail=response.get('Message'))
    return response

@app.post("/essential_write/modify")
async def essential_write_modify(
    data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Handles modifications to existing entities.
    Requires JWT authentication.
    """

    obj_type = data.get("type")
    obj_data = data.get("data")
    
    print(f"\nUser {current_user['id']} modifying DB with operation: {obj_type}")
    handler = private_handlers.get(obj_type, lambda x: {"error": f"Unknown type: {obj_type}"})  
    response = handler(obj_data)
    print(response)
    if response.get('Success') == 'false':
        raise HTTPException(status_code=500, detail=response.get('Message'))
    return response

@app.post("/background_write/")
async def background_write(data: dict):
    """
    Endpoint for background message publishing, this will be for non-essential writes such as: num_clicks, num_impressions, etc.
    """

    if data.get('metric_type') not in [m.value for m in MetricType]:
        raise HTTPException(status_code=400, detail="Invalid metric type")

    # Queue the task in Celery
    task = publish_metric.delay(
        event_id=data.get("event_id"),
        metric_type=data.get("metric_type")
    )
    
    return {
        "message": "Task queued successfully",
        "task_id": task.id
    }


# --------------- Streaming Read Endpoints ----------------
@app.get("/events", response_class=StreamingResponse)
async def proxy_get_events():
    """Proxy request for streaming all events. Public endpoint."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{DB_READER_SERVICE_URL}/events", timeout=30.0)
            
            async def parse_stream():
                # Process the stream line by line
                async for line in response.aiter_lines():
                    if line.strip():  # Skip empty lines
                        try:
                            data = json.loads(line)
                            event_id = data.get("id")
                            if event_id:
                                print(event_id)
                                await background_write(data={"event_id": event_id, "metric_type": "impression"})
                            yield line + "\n"
                        except json.JSONDecodeError:
                            print(f"Skipping invalid json: {line}")

            return StreamingResponse(
                parse_stream(),
                media_type="application/json"
            )
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/djs", response_class=StreamingResponse)
async def proxy_get_djs():
    """Proxy request for streaming all DJs and their socials. Public endpoint."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{DB_READER_SERVICE_URL}/djs", timeout=30.0)
            
            async def parse_stream():
                # Process the stream line by line
                async for line in response.aiter_lines():
                    if line.strip():  # Skip empty lines
                        # print(f"Line: {line}\n")
                        yield line + "\n"

            return StreamingResponse(
                parse_stream(),
                media_type="application/json"
            )
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))


# ----------- Direct Proxy Read Endpoints ---------------
@app.get("/venues")
async def proxy_get_venues():
    """Proxy request for getting all venues grouped by country. Public endpoint."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{DB_READER_SERVICE_URL}/venues", timeout=30.0)
            
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="Venues not found")
            
            return JSONResponse(content=response.json())
            
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/event/{event_id}")
async def proxy_get_event_details(event_id: int):
    """Proxy request for getting detailed event info (venue & organizer). Public endpoint."""

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{DB_READER_SERVICE_URL}/event/{event_id}", timeout=10.0)
            await background_write(data={"event_id": event_id, "metric_type": "click"})
            print(response.json())
            return JSONResponse(content=response.json(), status_code=response.status_code)
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/event/by_entity/{entity_type}/{entity_id}")
async def proxy_get_events_by_entity(entity_type: int, entity_id: int):
    print("ddd")

@app.get("/profile/{user_id}")
async def get_user_profile(
    current_user: dict = Depends(get_current_user)
):
    """Proxy request for getting user profile data. Private endpoint."""
    async with httpx.AsyncClient() as client:
        try:
            print(current_user)
            user_id = current_user.get('id')
            role_id = current_user.get('role_id')

            response = await client.get(
                f"{DB_READER_SERVICE_URL}/profile/{user_id}",
                timeout=10.0
            )
            
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="User not found")
            
            profile_data = response.json()
            print(f"Profile data: {profile_data}")

            # DO THIS CONCURRENTLY
            # Add role-specific data based on role_id
            if role_id == ROLE_IDS["DJ"]:
                dj_response = await client.get(
                    f"{DB_READER_SERVICE_URL}/dj/{user_id}",
                    timeout=10.0
                )
                if dj_response.status_code == 200:
                    profile_data["dj_data"] = dj_response.json()
            
            elif role_id == ROLE_IDS["VENUE"]:
                venue_response = await client.get(
                    f"{DB_READER_SERVICE_URL}/venue/{user_id}",
                    timeout=10.0
                )
                if venue_response.status_code == 200:
                    profile_data["venue_data"] = venue_response.json()
            
            elif role_id == ROLE_IDS["ORGANIZER"]:
                org_response = await client.get(
                    f"{DB_READER_SERVICE_URL}/organizer/{user_id}",
                    timeout=10.0
                )
                if org_response.status_code == 200:
                    profile_data["organizer_data"] = org_response.json()

            return JSONResponse(content=profile_data)
            
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))


# ----------- Recommendation Endpoints ---------------
@app.get("/recommendations/{user_id}")
async def get_user_recommendations(
    current_user: dict = Depends(get_current_user)
):
    """Asynchronously fetch and process recommendations."""
    async with httpx.AsyncClient() as client:
        try:
            user_id = current_user.get('id')

            user_data_task = create_task(
                client.get(
                    f"{DB_READER_SERVICE_URL}/user_recommendation_data/{user_id}",
                    timeout=30.0
                )
            )
            
            # Wait for user data
            user_data_response = await user_data_task
            if user_data_response.status_code != 200:
                raise HTTPException(
                    status_code=user_data_response.status_code,
                    detail="Failed to fetch user data"
                )

            # Start recommendation task
            # recommendation_task = create_task(
            #     client.post(
            #         f"{RECOMMENDATION_SERVICE_URL}/generate",
            #         json=user_data_response.json(),
            #         timeout=30.0
            #     )
            # )
            
            # # Wait for recommendations
            # recommendation_response = await recommendation_task
            return JSONResponse(content=user_data_response.json())

        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/{user_id}")
async def get_user_events(
    current_user: dict = Depends(get_current_user)
):
    """Proxy request for getting user's events based on their role."""
    async with httpx.AsyncClient() as client:
        try:
            user_id = current_user.get('id')
            role_id = current_user.get('role_id')
            if not role_id:
                raise HTTPException(status_code=400, detail="User role not found")

            # Route to appropriate endpoint based on role
            if role_id == ROLE_IDS["DJ"]:
                response = await client.get(
                    f"{DB_READER_SERVICE_URL}/events/dj/{user_id}",
                    timeout=10.0
                )
            elif role_id == ROLE_IDS["VENUE"]:
                response = await client.get(
                    f"{DB_READER_SERVICE_URL}/events/venue/{user_id}",
                    timeout=10.0
                )
            elif role_id == ROLE_IDS["ORGANIZER"]:
                response = await client.get(
                    f"{DB_READER_SERVICE_URL}/events/organizer/{user_id}",
                    timeout=10.0
                )
            else:
                raise HTTPException(status_code=400, detail="Invalid role for event lookup")

            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="Events not found")
            
            print(f"Events: {response.json()}")
            return JSONResponse(content=response.json())
            
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))


# --------------- B.S. Endpoints ----------------
# @app.get("/new_key")
def refresh_key_manual():
    """faking JWT key refresh; should be on timer"""
    rotate_keys()

# @app.get("/protected")
def protected(token: str):
    """temporary endpoint to see whats in the JWT"""
    user = decode_jwt(token)
    if not user[0]:
        raise HTTPException(status_code=401, detail=user[1])

    print(f"Encoded data:\n {user}")
    return {"message": f"Hello, User {user[1]['user_id']}!", "other_data": user[1]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)