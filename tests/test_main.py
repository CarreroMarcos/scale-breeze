import pytest
import uuid
import json
import jwt
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch, ANY
from contextlib import asynccontextmanager

from main import app, get_db, get_redis, get_kafka_producer, settings
from fastapi.testclient import TestClient

# --- Helpers ---
def create_test_token(user_id: str):
    payload = {
        "sub": user_id,
        "exp": datetime.now(timezone.utc) + timedelta(hours=1)
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

# --- Mocks ---
@pytest.fixture
def mock_db():
    mock = AsyncMock()
    mock.fetchrow.return_value = {
        "id": uuid.uuid4(),
        "content": "Test content",
        "author": "test_user",
        "created_at": datetime.now(timezone.utc)
    }
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
    mock.get.return_value = None
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
    
    app.state.db_pool = MagicMock()
    app.state.redis_pool = MagicMock()
    app.state.kafka_producer = mock_kafka
    
    @asynccontextmanager
    async def dummy_lifespan(app):
        yield

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = dummy_lifespan
    
    with TestClient(app) as c:
        yield c
    
    app.router.lifespan_context = original_lifespan
    app.dependency_overrides.clear()

# --- Tests ---
def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_create_post_unauthorized(client):
    post_data = {"content": "Hello World", "author": "tester"}
    response = client.post("/posts", json=post_data)
    assert response.status_code == 401 # Unauthorized

def test_create_post_success(client, mock_db, mock_kafka):
    user_id = "test-user-123"
    token = create_test_token(user_id)
    post_data = {"content": "Hello World", "author": "tester"}
    
    response = client.post(
        "/posts", 
        json=post_data,
        headers={"Authorization": f"Bearer {token}"}
    )
    
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    
    # Verify Kafka event emitted with request_id
    mock_kafka.send_and_wait.assert_called_once()
    args, _ = mock_kafka.send_and_wait.call_args
    event_data = json.loads(args[1].decode("utf-8"))
    assert "request_id" in event_data
    assert event_data["author"] == "tester"

def test_get_user_feed_authorized(client, mock_redis):
    user_id = str(uuid.uuid4())
    token = create_test_token(user_id)
    mock_redis.get.return_value = json.dumps([])
    
    response = client.get(
        f"/user/{user_id}/feed",
        headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 200

def test_error_handler_unified_format(client):
    response = client.get("/non-existent")
    assert response.status_code == 404
    data = response.json()
    assert "error" in data
    assert data["error"]["code"] == "NOT_FOUND"

def test_structured_logging_middleware(client):
    response = client.get("/health")
    assert "X-Request-ID" in response.headers
