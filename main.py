import os
import json
import time
import asyncio
import uuid
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, Request, Depends, Response
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# --- Configuration ---
class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://postgres:postgres@db:5432/scalebreeze"
    REDIS_URL: str = "redis://redis:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    DB_POOL_MIN: int = 5
    DB_POOL_MAX: int = 50

    class Config:
        env_file = ".env"

settings = Settings()

# --- Serialization helper ---
def json_serial(obj):
    if isinstance(obj, (datetime, uuid.UUID)):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

# --- Lifecycle Management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize DB pool
    app.state.db_pool = await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=settings.DB_POOL_MIN,
        max_size=settings.DB_POOL_MAX
    )
    
    # Startup: Initialize Redis pool
    app.state.redis_pool = redis.ConnectionPool.from_url(settings.REDIS_URL)
    
    yield
    
    # Shutdown: Close pools
    await app.state.db_pool.close()
    await app.state.redis_pool.disconnect()

# --- Dependencies ---
async def get_db(request: Request):
    async with request.app.state.db_pool.acquire() as connection:
        yield connection

async def get_redis(request: Request):
    client = redis.Redis(connection_pool=request.app.state.redis_pool)
    try:
        yield client
    finally:
        await client.close()

# --- App Definition ---
app = FastAPI(title="feed-service", lifespan=lifespan)

# --- Middleware: Structured Logging ---
@app.middleware("http")
async def structured_logging_middleware(request: Request, call_next):
    start_time = time.perf_counter()
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    
    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as e:
        status_code = 500
        raise e
    finally:
        duration_ms = (time.perf_counter() - start_time) * 1000
        log_line = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "duration_ms": round(duration_ms, 2),
            "client_ip": request.client.host if request.client else None
        }
        print(json.dumps(log_line))

    response.headers["X-Request-ID"] = request_id
    return response

# --- Models ---
class PostCreate(BaseModel):
    content: str = Field(..., max_length=280)
    author: str = Field(..., max_length=50)

class Post(PostCreate):
    id: uuid.UUID
    created_at: datetime

# --- Routes ---
@app.post("/posts", response_model=Post)
async def create_post(
    post: PostCreate, 
    db: asyncpg.Connection = Depends(get_db),
    r: redis.Redis = Depends(get_redis)
):
    post_id = uuid.uuid4()
    row = await db.fetchrow(
        "INSERT INTO posts (id, content, author) VALUES ($1, $2, $3) RETURNING id, content, author, created_at",
        post_id, post.content, post.author
    )
    
    # Cache individual post (Fixed serialization)
    await r.set(f"post:{post_id}", json.dumps(dict(row), default=json_serial), ex=3600)
    
    return dict(row)

@app.get("/posts")
async def list_posts(
    db: asyncpg.Connection = Depends(get_db),
    r: redis.Redis = Depends(get_redis)
):
    cache_key = "posts:all"
    cached_data = await r.get(cache_key)
    
    if cached_data:
        return Response(
            content=cached_data,
            media_type="application/json",
            headers={"X-Cache": "HIT"}
        )

    rows = await db.fetch("SELECT * FROM posts ORDER BY created_at DESC LIMIT 100")
    posts = [dict(row) for row in rows]
    
    # Corrected serialization (UUID handles str() better than .isoformat())
    json_data = json.dumps(posts, default=json_serial)
    await r.set(cache_key, json_data, ex=60)
    
    return Response(
        content=json_data,
        media_type="application/json",
        headers={"X-Cache": "MISS"}
    )

@app.get("/health")
async def health():
    return {"status": "healthy"}
