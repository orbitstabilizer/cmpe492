#!/bin/bash

# Deploy to 'yusuf' host
# Usage: ./deploy.sh [destination_dir]
# Default destination: ~/cmpe492

DEST=${1:-"~/cmpe492"}
HOST="yusuf"

echo "🚀 Deploying to $HOST:$DEST..."

rsync -avz --progress --delete \
    --exclude '.git' \
    --exclude '.venv' \
    --exclude 'venv' \
    --exclude '__pycache__' \
    --exclude '.DS_Store' \
    --exclude '*.env' \
    --exclude '.gemini' \
    --exclude 'tmp' \
    --exclude 'cache' \
    --exclude 'logs' \
    --exclude '*.log' \
    --exclude '.vscode' \
    --exclude '.idea' \
    --exclude 'dex-prices/dex-prices' \
    --exclude 'price-index/price-index' \
    --exclude 'price-index/.price_ix.data' \
    ./ $HOST:$DEST

echo "✅ Deployment complete!"
echo "   To restart services:"
echo "   ssh $HOST 'cd $DEST && docker compose down -v && docker compose up --build -d'"
