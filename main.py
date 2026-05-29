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
from fastapi import FastAPI, Request, Depends, Response, HTTPException, Query, status
from fastapi.responses import JSONResponse
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

# --- Error Handling ---
def create_error_response(code: str, message: str, status_code: int, details: dict = None):
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "details": details or {}
            }
        }
    )

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

from fastapi.middleware.cors import CORSMiddleware

# --- App Definition ---
app = FastAPI(title="feed-service", lifespan=lifespan)

# --- Security: CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://localhost:8889"], # Strict origin control
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    expose_headers=["X-Cache", "X-Request-ID"],
)

from fastapi.exceptions import RequestValidationError

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return create_error_response(
        code="API_ERROR",
        message=exc.detail,
        status_code=exc.status_code
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return create_error_response(
        code="VALIDATION_ERROR",
        message="Invalid request data.",
        status_code=422,
        details={"errors": exc.errors()}
    )

@app.exception_handler(status.HTTP_404_NOT_FOUND)
async def not_found_exception_handler(request: Request, exc: HTTPException):
    return create_error_response(
        code="NOT_FOUND",
        message="The requested resource was not found.",
        status_code=404
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return create_error_response(
        code="INTERNAL_SERVER_ERROR",
        message="An unexpected error occurred.",
        status_code=500
    )

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
        # Exception handler will catch this, but we log here
        duration_ms = (time.perf_counter() - start_time) * 1000
        log_line = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "duration_ms": round(duration_ms, 2),
            "error": str(e)
        }
        print(json.dumps(log_line))
        raise e
    
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
@app.post("/posts", response_model=Post, status_code=status.HTTP_201_CREATED)
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
    
    # Cache individual post
    await r.set(f"post:{post_id}", json.dumps(dict(row), default=json_serial), ex=3600)
    
    return dict(row)

@app.get("/posts")
async def list_posts(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: asyncpg.Connection = Depends(get_db),
    r: redis.Redis = Depends(get_redis)
):
    cache_key = f"posts:limit:{limit}:offset:{offset}"
    cached_data = await r.get(cache_key)
    
    if cached_data:
        return Response(
            content=cached_data,
            media_type="application/json",
            headers={"X-Cache": "HIT"}
        )

    rows = await db.fetch(
        "SELECT * FROM posts ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        limit, offset
    )
    posts = [dict(row) for row in rows]
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
