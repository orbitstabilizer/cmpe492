#!/bin/bash

# Database Setup Script for CMPE 492 Crypto Exchange Analysis
# This script starts the PostgreSQL + TimescaleDB and Adminer containers

set -e

echo "🚀 Starting CMPE 492 Database Infrastructure..."
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    echo "   Visit: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install Docker Compose first."
    echo "   Visit: https://docs.docker.com/compose/install/"
    exit 1
fi

# Start containers
echo "📦 Building and starting containers..."
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 5

# Check if database is ready
for i in {1..30}; do
    if docker-compose exec -T postgres pg_isready -U cmpe492 > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ PostgreSQL failed to start"
        exit 1
    fi
    echo "  Waiting... ($i/30)"
    sleep 1
done

echo ""
echo "✅ Database setup complete!"
echo ""
echo "📊 Access Points:"
echo "   PostgreSQL: localhost:5432"
echo "   Database: crypto_exchange"
echo "   User: cmpe492"
echo "   Password: password123"
echo ""
echo "🌐 Web Interface:"
echo "   Adminer: http://localhost:8080"
echo "     - System: PostgreSQL"
echo "     - Server: postgres"
echo "     - Username: cmpe492"
echo "     - Password: password123"
echo "     - Database: crypto_exchange"
echo ""
echo "📝 Useful commands:"
echo "   docker-compose ps              # Show running containers"
echo "   docker-compose logs postgres   # View PostgreSQL logs"
echo "   docker-compose down            # Stop containers"
echo "   docker-compose down -v         # Stop containers and remove data"
echo ""
echo "🔍 Connect with psql:"
echo "   psql -h localhost -U cmpe492 -d crypto_exchange"
echo ""
