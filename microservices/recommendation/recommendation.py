from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import redis
import json
import os
from typing import Dict

app = FastAPI()

# Redis connection
redis_client = redis.Redis(
    host='driven-robin-54477.upstash.io',
    port=6379,
    password='AdTNAAIjcDE0NjU0YjNkNjU3MmE0NGE0OTAzZWZhNDc0MzRjMDJhN3AxMA',
    ssl=True,
    decode_responses=True
)

@app.post("/generate")
async def generate_recommendations(user_data: Dict):
    """Generate recommendations based on user data."""
    try:
        # This is where you'll implement your recommendation logic
        # For now, we'll just return a placeholder
        recommendations = {
            "user_id": user_data.get("user_id"),
            "recommendations": []  # Your recommendation logic here
        }
        
        # Cache the recommendations
        redis_key = f"recommendations:{user_data.get('user_id')}"
        redis_client.setex(
            redis_key,
            1800,  # 30 minutes expiration
            json.dumps(recommendations)
        )
        
        return JSONResponse(content=recommendations)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002, reload=True)