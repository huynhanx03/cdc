#!/bin/sh

# 1. Start Next.js Frontend (Standalone)
echo "Starting Next.js Frontend on port 9092..."
export PORT=9092
node /app/server.js &

# 2. Wait a bit for external NATS to be ready if needed
# We assume docker-compose links will handle the hostname 'nats'
echo "Starting CDC Backend..."
/app/cdc --config /app/config.yaml
