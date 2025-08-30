#!/bin/bash

# Disheap Development Environment Setup Script
# This script sets up the complete Disheap development environment using Docker Compose

set -e  # Exit on any error

echo "🚀 Setting up Disheap Development Environment"
echo "=============================================="

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "❌ Docker is required but not installed. Aborting." >&2; exit 1; }
command -v docker compose >/dev/null 2>&1 || { echo "❌ Docker Compose v2 is required but not installed. Aborting." >&2; exit 1; }

# Check if ports are available
check_port() {
    local port=$1
    local service=$2
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "⚠️  Warning: Port $port is already in use (needed for $service)"
        echo "   Please free the port or modify docker-compose.yml"
        return 1
    fi
    return 0
}

echo "🔍 Checking port availability..."
ports_ok=true
check_port 3000 "Disheap API" || ports_ok=false
check_port 5173 "Disheap Frontend" || ports_ok=false  
check_port 8888 "Python SDK Jupyter" || ports_ok=false
check_port 9090 "Disheap Engine gRPC" || ports_ok=false
check_port 9091 "Engine Metrics" || ports_ok=false
check_port 9092 "Prometheus" || ports_ok=false
check_port 16686 "Jaeger UI" || ports_ok=false
check_port 3001 "Grafana" || ports_ok=false

if [ "$ports_ok" = false ]; then
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Setup cancelled"
        exit 1
    fi
fi

# Create necessary directories
echo "📁 Creating configuration directories..."
mkdir -p config/grafana/datasources
mkdir -p config/grafana/dashboards
mkdir -p data/engine
mkdir -p data/api
mkdir -p logs

# Check Docker memory
echo "🔧 Checking Docker resources..."
docker_memory=$(docker system info --format '{{.MemTotal}}' 2>/dev/null || echo "0")
if [ "$docker_memory" -lt 4000000000 ]; then
    echo "⚠️  Warning: Docker has less than 4GB memory available"
    echo "   Consider increasing Docker's memory limit for better performance"
fi

# Start the environment
echo "🐳 Starting Disheap services..."
echo "This may take a few minutes on first run (building images)..."

if docker compose up -d --build; then
    echo "✅ Services started successfully!"
else
    echo "❌ Failed to start services"
    exit 1
fi

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
sleep 10

# Function to check service health
check_service() {
    local service_name=$1
    local url=$2
    local max_attempts=30
    local attempt=1
    
    echo -n "Checking $service_name... "
    
    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo "✅"
            return 0
        fi
        
        sleep 2
        attempt=$((attempt + 1))
        echo -n "."
    done
    
    echo "❌ (timeout)"
    return 1
}

# Check service health
services_ok=true
check_service "Disheap Engine" "http://localhost:8080/health" || services_ok=false
check_service "Disheap API" "http://localhost:3000/health" || services_ok=false
check_service "Frontend" "http://localhost:5173" || services_ok=false
check_service "Jaeger" "http://localhost:16686" || services_ok=false
check_service "Prometheus" "http://localhost:9092" || services_ok=false
check_service "Grafana" "http://localhost:3001" || services_ok=false

if [ "$services_ok" = true ]; then
    echo ""
    echo "🎉 Disheap Development Environment is ready!"
    echo ""
    echo "📋 Access the services:"
    echo "   Frontend:        http://localhost:5173"
    echo "   API:             http://localhost:3000"
    echo "   Python Examples: docker compose exec disheap-python bash"
    echo "   Jupyter:         http://localhost:8888"
    echo "   Jaeger Tracing:  http://localhost:16686"
    echo "   Prometheus:      http://localhost:9092"
    echo "   Grafana:         http://localhost:3001 (admin/admin)"
    echo ""
    echo "🧪 Quick Test:"
    echo "   docker compose exec disheap-python python examples/work_queue.py"
    echo ""
    echo "📊 View logs:"
    echo "   docker compose logs -f"
    echo ""
    echo "🛑 Stop environment:"
    echo "   docker compose down"
    echo ""
    echo "Happy coding! 🚀"
else
    echo ""
    echo "⚠️  Some services may not be fully ready yet."
    echo "Check logs with: docker compose logs"
    echo "You can still try accessing the services manually."
fi

# Create a simple test to verify the setup
echo ""
echo "🧪 Running basic connectivity test..."

# Test if we can connect to the engine from Python SDK
if docker compose exec -T disheap-python python -c "
import asyncio
from disheap import DisheapClient

async def test():
    try:
        client = DisheapClient(
            endpoints=['disheap-engine:9090'],
            api_key='dh_dev_key_for_testing'
        )
        async with client:
            await client.create_topic('test-setup', mode='max')
            print('✅ Successfully connected to Disheap Engine')
            
            async with client.producer() as producer:
                result = await producer.enqueue('test-setup', b'Setup test message', 100)
                print('✅ Successfully enqueued test message')
                
        return True
    except Exception as e:
        print(f'❌ Connection test failed: {e}')
        return False

success = asyncio.run(test())
exit(0 if success else 1)
" 2>/dev/null; then
    echo "✅ Connectivity test passed!"
else
    echo "⚠️  Connectivity test failed - services may still be starting up"
    echo "   Try the test again in a minute:"
    echo "   docker compose exec disheap-python python examples/work_queue.py"
fi

echo ""
echo "📚 Next steps:"
echo "   1. Open http://localhost:5173 to use the web interface"
echo "   2. Run: docker compose exec disheap-python bash"
echo "   3. Try the examples in examples/ directory"
echo "   4. Run tests with: pytest tests/ -v"
echo ""
echo "Environment setup complete! 🎊"
