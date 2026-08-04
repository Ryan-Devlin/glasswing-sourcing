#!/bin/bash
# Multi-service split: create the 3 real crawler/scraper worker services on
# Railway. Run this from the project root. Each step prints what it did --
# read the output as you go rather than blindly running the whole thing.
set -e
cd "/Users/ryandevlin/Desktop/HTTPS Scraping"

echo "=== 1. Create pipeline-worker ==="
railway add --service pipeline-worker --repo Ryan-Devlin/glasswing-sourcing --branch main

echo "=== 2. Create ai-worker ==="
railway add --service ai-worker --repo Ryan-Devlin/glasswing-sourcing --branch main

echo "=== 3. Create zone-worker ==="
railway add --service zone-worker --repo Ryan-Devlin/glasswing-sourcing --branch main

echo "=== 4. Set pipeline-worker env vars ==="
railway variables --service pipeline-worker \
  --set 'GLASSWING_RUNTIME_MODE=cloud' \
  --set 'PIPELINE_DATABASE_URL=${{web.PIPELINE_DATABASE_URL}}' \
  --set 'RUNTIME_CONTROL_DATABASE_URL=${{web.RUNTIME_CONTROL_DATABASE_URL}}' \
  --set 'AI_SEARCH_DATABASE_URL=${{web.AI_SEARCH_DATABASE_URL}}' \
  --set 'ZONE_UNIVERSE_DATABASE_URL=${{web.ZONE_UNIVERSE_DATABASE_URL}}' \
  --set 'ANTHROPIC_API_KEY=${{web.ANTHROPIC_API_KEY}}' \
  --set 'GLASSWING_DATA_ROOT=/app/data' \
  --set 'GLASSWING_LOG_ROOT=/app/logs' \
  --skip-deploys

echo "=== 5. Set ai-worker env vars ==="
railway variables --service ai-worker \
  --set 'GLASSWING_RUNTIME_MODE=cloud' \
  --set 'DATABASE_URL=${{web.DATABASE_URL}}' \
  --set 'AI_SEARCH_DATABASE_URL=${{web.AI_SEARCH_DATABASE_URL}}' \
  --set 'PIPELINE_DATABASE_URL=${{web.PIPELINE_DATABASE_URL}}' \
  --set 'RUNTIME_CONTROL_DATABASE_URL=${{web.RUNTIME_CONTROL_DATABASE_URL}}' \
  --set 'S3_BUCKET=${{web.S3_BUCKET}}' \
  --set 'S3_ENDPOINT_URL=${{web.S3_ENDPOINT_URL}}' \
  --set 'S3_ACCESS_KEY_ID=${{web.S3_ACCESS_KEY_ID}}' \
  --set 'S3_SECRET_ACCESS_KEY=${{web.S3_SECRET_ACCESS_KEY}}' \
  --set 'ANTHROPIC_API_KEY=${{web.ANTHROPIC_API_KEY}}' \
  --set 'PYTHONPATH=/app/ai-domain-search-engine/src:/app/zone-universe/src:/app' \
  --set 'AI_SEARCH_DB=/app/data/ai-search.sqlite3' \
  --set 'AI_SEARCH_HTML_ARCHIVE=/app/data/ai-html' \
  --set 'GLASSWING_DATA_ROOT=/app/data' \
  --set 'GLASSWING_LOG_ROOT=/app/logs' \
  --skip-deploys

echo "=== 6. Set zone-worker env vars ==="
railway variables --service zone-worker \
  --set 'GLASSWING_RUNTIME_MODE=cloud' \
  --set 'DATABASE_URL=${{web.DATABASE_URL}}' \
  --set 'ZONE_UNIVERSE_DATABASE_URL=${{web.ZONE_UNIVERSE_DATABASE_URL}}' \
  --set 'PIPELINE_DATABASE_URL=${{web.PIPELINE_DATABASE_URL}}' \
  --set 'RUNTIME_CONTROL_DATABASE_URL=${{web.RUNTIME_CONTROL_DATABASE_URL}}' \
  --set 'S3_BUCKET=${{web.S3_BUCKET}}' \
  --set 'S3_ENDPOINT_URL=${{web.S3_ENDPOINT_URL}}' \
  --set 'S3_ACCESS_KEY_ID=${{web.S3_ACCESS_KEY_ID}}' \
  --set 'S3_SECRET_ACCESS_KEY=${{web.S3_SECRET_ACCESS_KEY}}' \
  --set 'ANTHROPIC_API_KEY=${{web.ANTHROPIC_API_KEY}}' \
  --set 'ZONE_UNIVERSE_DATA_ROOT=/app/zone-data' \
  --set 'ZONE_UNIVERSE_DB=/app/zone-data/universe.sqlite3' \
  --set 'ZONE_UNIVERSE_HTML_ARCHIVE=/app/zone-html' \
  --set 'ZONE_UNIVERSE_LOG_ROOT=/app/logs' \
  --set 'ZONE_UNIVERSE_BATCH_SIZE=100' \
  --set 'ZONE_UNIVERSE_PREFLIGHT_ENABLED=1' \
  --set 'ZONE_UNIVERSE_PREFLIGHT_MAX_RESPONSE_BYTES=131072' \
  --set 'ZONE_UNIVERSE_PREFLIGHT_CONCURRENCY=100' \
  --skip-deploys

echo "=== Done creating services + setting variables. ==="
echo "Next: set each service's Start Command in the Railway dashboard (see instructions), then deploy."
