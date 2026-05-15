#!/usr/bin/env bash
# apply_auth.sh — Wire etl_pass credentials into running Redis and Elasticsearch containers.
# Run from the repo root: bash scripts/apply_auth.sh

set -euo pipefail

REDIS_CONTAINER="redis"
ES_CONTAINER="elasticsearch"
REDIS_PASS="etl_pass"
ES_USER="elastic"
ES_PASS="etl_pass"

echo ""
echo "=== Applying auth to Redis ==="

# Set password on the live Redis instance (no restart needed).
docker exec "$REDIS_CONTAINER" redis-cli CONFIG SET requirepass "$REDIS_PASS"
echo "Redis password set."

# Verify: ping with the new password.
docker exec "$REDIS_CONTAINER" redis-cli -a "$REDIS_PASS" PING
echo "Redis auth verified (PONG above = OK)."


echo ""
echo "=== Applying auth to Elasticsearch ==="
echo "Note: enabling xpack.security requires a container restart."

# Recreate just the ES container using the updated docker-compose.yml.
# This picks up: xpack.security.enabled=true and ELASTIC_PASSWORD=etl_pass
docker compose up -d --force-recreate "$ES_CONTAINER"

# Wait for ES to become healthy (up to 90 s).
echo "Waiting for Elasticsearch to be ready..."
for i in $(seq 1 18); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -u "${ES_USER}:${ES_PASS}" \
    "http://localhost:9200/_cluster/health" 2>/dev/null || true)
  if [ "$STATUS" = "200" ]; then
    echo "Elasticsearch is up and authenticated (HTTP 200)."
    break
  fi
  echo "  Attempt $i/18 — status=${STATUS}, retrying in 5s..."
  sleep 5
done

if [ "$STATUS" != "200" ]; then
  echo "ERROR: Elasticsearch did not become healthy in time. Check: docker logs $ES_CONTAINER"
  exit 1
fi


echo ""
echo "=== Done ==="
echo "Redis  → password: $REDIS_PASS  (no username)"
echo "Elastic → user: $ES_USER  password: $ES_PASS"
echo ""
echo "Quick test commands:"
echo "  redis-cli -a $REDIS_PASS -h 127.0.0.1 PING"
echo "  curl -u $ES_USER:$ES_PASS http://localhost:9200/_cluster/health?pretty"
