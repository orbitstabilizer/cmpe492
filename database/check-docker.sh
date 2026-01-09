#!/bin/bash

# Quick setup guide for Docker and Database

echo "🐋 CMPE 492 Database Setup Guide"
echo "=================================="
echo ""

# Check if Docker is running
if ! docker ps &> /dev/null; then
    echo "❌ Docker daemon is not running"
    echo ""
    echo "📋 To start Docker on macOS:"
    echo ""
    echo "  Option 1: Using Finder"
    echo "   • Open Applications/Docker.app"
    echo "   • Docker will start automatically"
    echo ""
    echo "  Option 2: Using Homebrew"
    echo "   brew install docker"
    echo "   brew install colima  (or use Docker Desktop)"
    echo "   colima start"
    echo ""
    echo "  Option 3: Using native Docker Desktop"
    echo "   • Download from: https://www.docker.com/products/docker-desktop/"
    echo "   • Install and run Docker.app"
    echo ""
    echo "❓ Once Docker is running, run this again:"
    echo "   ./start-db.sh"
    echo ""
    exit 1
fi

# Rest of the script runs if Docker is running
echo "✅ Docker is running!"
echo ""
echo "🚀 Starting database containers..."
cd "$(dirname "$0")"
docker-compose up -d

echo "✅ Containers started!"
echo ""
docker-compose ps
