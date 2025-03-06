from fastapi import FastAPI, Request, HTTPException, Depends, Body, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from jose import jwt, JWTError, ExpiredSignatureError
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from google.protobuf.timestamp_pb2 import Timestamp
from contextlib import asynccontextmanager
from db_writes import write_service_pb2, write_service_pb2_grpc
from dotenv import load_dotenv
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
from asyncio import create_task, TimeoutError
import time

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

type_handlers = {
    "event": handle_event,
    "venue": handle_venue,
    "user": handle_user,
    "dj": handle_dj,
    "org": handle_org,
    "publish_event": handle_publish,
    "dj_event": handle_dj_event
}


# --------------- JWT Util Functions ----------------
def rotate_keys():
    print("dont use this function")
    # SECRET_KEYS["previous"] = SECRET_KEYS["current"]  # Move current to previous
    # SECRET_KEYS["current"] = os.urandom(32).hex()  # Generate new key
    # print(f"New secret key: {SECRET_KEYS['current']}")

def create_jwt(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = {}
    to_encode['id'] = data['id']
    bytes = base64.b64encode(json.dumps(data).encode('utf-8')).decode('utf-8')
    # encoded_str = bytes.decode('utf-8')

    # notifications, music servce, established, username, email, location, language

    if expires_delta:
        to_encode["exp"] = int((datetime.now(timezone.utc) + expires_delta).timestamp())
    else:
        to_encode["exp"] = int((datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)).timestamp())
    print(f"Creating jwt with: {to_encode}")
    print(f"Encoding: {bytes}")

    to_encode['data'] = bytes

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

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
            # raise HTTPException(status_code=401, detail="Token has expired")

        decoded = json.loads(base64.b64decode(payload["data"]).decode("utf-8"))
        print(f"Payload at decoding: {payload}")
        print(f"Base64 data: {decoded}")
        return (True, payload)
        # return payload
    except ExpiredSignatureError:
        return (False, "Token has expired")
        # raise HTTPException(status_code=401, detail="Token has expired")
    except JWTError:
        try:
            payload = jwt.decode(token, SECRET_KEY_PREVIOUS, algorithms=[ALGORITHM])
            decoded = json.loads(base64.b64decode(payload["data"]).decode("utf-8"))

            print(f"Payload at decoding: {payload}")
            print(f"Base64 data: {decoded}")
            return (True, payload)
            # return payload
        except:
            return (False, "Invalid token")
            # raise HTTPException(status_code=401, detail="Invalid token")
    
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
            # raise HTTPException(status_code=401, detail="Invalid token")
        # Do I need to check the previous key for refresh tokens?? 

# def encode_jwt(data: Dict) -> str:
#     expiry = (datetime.now(timezone.utc) + expires_delta).timestamp()
#     data["exp"] = expiry
#     return jwt.encode(data, SECRET_KEYS, algorithm=ALGORITHM)
# should have same expiry as user's token

# async def create_pool():
#     try:
#         pool = await aiomysql.create_pool(
#             empty
#         )
#         return pool
#     except aiomysql.Error as e:
#         print(f"Database connection error: {e}")
#         return None

async def get_current_user_postgres(username: str, pw: str):
    global db_pool
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database connection failed")

    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, username, first_name, last_name, email, location, language FROM user_data WHERE username = $1 AND pw = $2",
                username, pw
            )
            if not row:
                raise HTTPException(status_code=401, detail="User not found")
            return dict(row)
    except asyncpg.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")


@app.post("/login")
async def login(creds: dict = Body(...)):
    username = creds.get("username")
    pw = creds.get("pw")
    if not username or not pw:
        raise HTTPException(status_code=401, detail="Missing credentials")
    
    user_data = await get_current_user_postgres(username, pw)
    sensitive_data['username'] = username
    sensitive_data['pw'] = pw
    print(f"\nUser data: {user_data}")
    print(f"\nSenitive data: {sensitive_data}")

    token = create_jwt(user_data, timedelta(minutes=int(ACCESS_TOKEN_EXPIRE_MINUTES)))
    refresh_token = create_refresh_token(user_data)
    return {"access_token": token, "refresh_token": refresh_token}

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

    new_access_token = create_jwt(payload)
    return {"access_token": new_access_token}


# --------------- Write Endpoints ----------------
@app.post("/essential_write/")
async def essential_write(data: dict):
    """
    Calls the gRPC service for essential database writes.
    """
    # user = decode_jwt(data.get("token"))
    # if not user[0]:
    #     raise HTTPException(status_code=401, detail=user[1])

    # print(f"\nUser {user[1]['user_id']} writing to DB")
    
    obj_type = data.get("type")
    obj_data = data.get("data")
    
    handler = type_handlers.get(obj_type, lambda x: {"error": f"Unknown type: {obj_type}"})  
    return handler(obj_data)

@app.post("/background_write/")
async def background_write(data: dict):
    """
    Calls the REST API service for background writes.
    """
    print(f"Background data: {data}")
    # response = requests.post(WRITE_SERVICE_REST_URL, json={"data": data["data"], "priority": False})
    # return response.json()


# --------------- Streaming Read Endpoints ----------------
@app.get("/events", response_class=StreamingResponse)
async def proxy_get_events():
    """Proxy request for streaming all events."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{DB_READER_SERVICE_URL}/events", timeout=30.0)
            return StreamingResponse(response.aiter_bytes(), media_type="application/json")
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/djs", response_class=StreamingResponse)
async def proxy_get_djs():
    """Proxy request for streaming all DJs and their socials."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{DB_READER_SERVICE_URL}/djs", timeout=30.0)
            return StreamingResponse(response.aiter_bytes(), media_type="application/json")
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))


# ----------- Direct Proxy Read Endpoints ---------------
@app.get("/event/{event_id}")
async def proxy_get_event_details(event_id: int):
    """Proxy request for getting detailed event info (venue & organizer)."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{DB_READER_SERVICE_URL}/event/{event_id}", timeout=10.0)
            return JSONResponse(content=response.json(), status_code=response.status_code)
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=str(e))


# --------------- B.S. Endpoints ----------------
@app.get("/new_key")
def refresh_key_manual():
    """faking JWT key refresh; should be on timer"""
    rotate_keys()

@app.get("/protected")
def protected(token: str):
    """temporary endpoint to see whats in the JWT"""
    user = decode_jwt(token)
    if not user[0]:
        raise HTTPException(status_code=401, detail=user[1])

    print(f"Encoded data:\n {user}")
    return {"message": f"Hello, User {user[1]['user_id']}!", "other_data": user[1]}

@app.get("/recommendations/{user_id}")
async def get_user_recommendations(user_id: int):
    """Asynchronously fetch and process recommendations."""
    async with httpx.AsyncClient() as client:
        try:
            # Create both tasks immediately
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)