import uuid
import time
import jwt
from locust import HttpUser, task, between, events
from datetime import datetime, timezone, timedelta

# --- Configuration ---
JWT_SECRET = "super-secret-key"
JWT_ALGORITHM = "HS256"

class ScaleBreezeUser(HttpUser):
    # Simulate realistic user think time
    wait_time = between(1, 3)
    
    def on_start(self):
        """Executed when a virtual user starts."""
        self.user_id = str(uuid.uuid4())
        self.token = self._generate_jwt(self.user_id)
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def _generate_jwt(self, user_id):
        payload = {
            "sub": user_id,
            "exp": datetime.now(timezone.utc) + timedelta(hours=1)
        }
        return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    @task(4)  # 80% weight
    def get_feed(self):
        """Simulates a user reading their timeline."""
        # Using the current user's ID for their personal feed
        with self.client.get(
            f"/user/{self.user_id}/feed",
            headers=self.headers,
            name="/user/{id}/feed",
            verify=False  # Disable SSL verification for self-signed local certs
        ) as response:
            if response.status_code != 200:
                response.failure(f"Failed to fetch feed: {response.status_code}")

    @task(1)  # 20% weight
    def create_post(self):
        """Simulates a user creating a new post."""
        payload = {
            "content": f"Load test post from {self.user_id} at {time.time()}",
            "author": f"user_{self.user_id[:8]}"
        }
        with self.client.post(
            "/posts",
            json=payload,
            headers=self.headers,
            name="/posts",
            verify=False
        ) as response:
            # POST /posts returns 202 Accepted in our current implementation
            if response.status_code not in [201, 202]:
                response.failure(f"Failed to create post: {response.status_code}")

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--my-argument", type=str, env_var="MY_ARGUMENT", default="default value")
