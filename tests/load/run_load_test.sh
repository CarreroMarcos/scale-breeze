#!/bin/bash

# Configuration
LOCUST_FILE="tests/load/locustfile.py"
HOST="https://localhost:8889"
USERS=100
SPAWN_RATE=5 # 2 users every 10 seconds (0.2 users/sec)
RUN_TIME="30s"
METRICS_FILE="container_metrics.csv"

echo "🚀 Starting Controlled Stress Test..."
echo "Config: $USERS users, $SPAWN_RATE spawn rate, $RUN_TIME duration"
echo "Target: $HOST"

# Initialize CSV header
echo "Timestamp,Name,CPUPerc,MemUsage" > $METRICS_FILE

# Start Locust in headless mode in the background
uv run locust -f $LOCUST_FILE --headless -u $USERS -r $SPAWN_RATE --run-time $RUN_TIME --host $HOST --only-summary &
LOCUST_PID=$!

echo "📊 Collecting container metrics (PID: $LOCUST_PID)..."

# Collect Docker stats in a loop while Locust is running
while kill -0 $LOCUST_PID 2>/dev/null; do
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    # Append stats to CSV
    docker stats --format "$TIMESTAMP,{{.Name}},{{.CPUPerc}},{{.MemUsage}}" --no-stream >> $METRICS_FILE
    sleep 5
done

echo "✅ Stress test complete. Metrics saved to $METRICS_FILE"
