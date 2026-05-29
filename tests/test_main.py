import pytest
import uuid
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, ANY
from contextlib import asynccontextmanager

from main import app, get_db, get_redis, get_kafka_producer
from fastapi.testclient import TestClient

# --- Mocks ---
@pytest.fixture
def mock_db():
    mock = AsyncMock()
    # Mock return value for INSERT
    mock.fetchrow.return_value = {
        "id": uuid.uuid4(),
        "content": "Test content",
        "author": "test_user",
        "created_at": datetime.now(timezone.utc)
    }
    # Mock return value for SELECT
    mock.fetch.return_value = [
        {
            "id": uuid.uuid4(),
            "content": "Post 1",
            "author": "user1",
            "created_at": datetime.now(timezone.utc)
        }
    ]
    return mock

@pytest.fixture
def mock_redis():
    mock = AsyncMock()
    mock.get.return_value = None  # Cache miss by default
    return mock

@pytest.fixture
def mock_kafka():
    mock = AsyncMock()
    return mock

# --- Client with Dependency Overrides ---
@pytest.fixture
def client(mock_db, mock_redis, mock_kafka):
    def override_get_db():
        yield mock_db

    def override_get_redis():
        yield mock_redis

    def override_get_kafka():
        return mock_kafka

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_kafka_producer] = override_get_kafka
    
    # Mock the pools on app.state
    app.state.db_pool = MagicMock()
    app.state.redis_pool = MagicMock()
    app.state.kafka_producer = mock_kafka
    
    # Forcefully bypass the real lifespan
    @asynccontextmanager
    async def dummy_lifespan(app):
        yield

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = dummy_lifespan
    
    with TestClient(app) as c:
        yield c
    
    # Restore and cleanup
    app.router.lifespan_context = original_lifespan
    app.dependency_overrides.clear()

# --- Tests ---
def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_create_post(client, mock_db, mock_kafka):
    post_data = {"content": "Hello World", "author": "tester"}
    response = client.post("/posts", json=post_data)
    
    assert response.status_code == 202  # Updated to 202 Accepted
    data = response.json()
    assert data["status"] == "accepted"
    assert "id" in data
    
    # Verify Kafka event emitted
    mock_kafka.send_and_wait.assert_called_once()
    args, kwargs = mock_kafka.send_and_wait.call_args
    assert args[0] == "post-events"
    event_data = json.loads(args[1].decode("utf-8"))
    assert event_data["author"] == "tester"
    assert event_data["action"] == "created"

def test_list_posts_pagination(client, mock_db):
    # Test default pagination
    response = client.get("/posts")
    assert response.status_code == 200
    mock_db.fetch.assert_called_with(
        "SELECT * FROM posts ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        20, 0
    )

    # Test custom pagination
    response = client.get("/posts?limit=10&offset=5")
    assert response.status_code == 200
    mock_db.fetch.assert_called_with(
        "SELECT * FROM posts ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        10, 5
    )

def test_list_posts_cache_miss(client, mock_db, mock_redis):
    mock_redis.get.return_value = None
    
    response = client.get("/posts")
    
    assert response.status_code == 200
    assert response.headers["X-Cache"] == "MISS"
    assert len(response.json()) == 1

def test_list_posts_cache_hit(client, mock_redis):
    # Simulate cache hit with JSON string
    mock_redis.get.return_value = json.dumps([{
        "id": str(uuid.uuid4()), 
        "content": "Cached", 
        "author": "bot",
        "created_at": datetime.now(timezone.utc).isoformat()
    }])
    
    response = client.get("/posts")
    
    assert response.status_code == 200
    assert response.headers["X-Cache"] == "HIT"
    assert response.json()[0]["content"] == "Cached"

def test_get_user_feed_cache_miss(client, mock_db, mock_redis):
    mock_redis.get.return_value = None
    user_id = uuid.uuid4()
    
    response = client.get(f"/user/{user_id}/feed")
    
    assert response.status_code == 200
    assert response.headers["X-Cache"] == "MISS"
    assert len(response.json()) == 1
    mock_db.fetch.assert_called()
    mock_redis.set.assert_called_with(f"user:{user_id}:feed", ANY, ex=60)

def test_get_user_feed_cache_hit(client, mock_redis):
    user_id = uuid.uuid4()
    mock_redis.get.return_value = json.dumps([{
        "id": str(uuid.uuid4()), 
        "content": "Feed Cache", 
        "author": "system",
        "created_at": datetime.now(timezone.utc).isoformat()
    }])
    
    response = client.get(f"/user/{user_id}/feed")
    
    assert response.status_code == 200
    assert response.headers["X-Cache"] == "HIT"
    assert response.json()[0]["content"] == "Feed Cache"

def test_structured_logging_middleware(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert "X-Request-ID" in response.headers

def test_error_handler_unified_format(client):
    # Trigger a 404
    response = client.get("/non-existent")
    assert response.status_code == 404
    data = response.json()
    assert "error" in data
    assert data["error"]["code"] == "NOT_FOUND"
    assert "message" in data["error"]
