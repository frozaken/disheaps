# Disheap API

The **disheap-api** is the HTTP/gRPC gateway service for the Disheap priority messaging system. It provides a REST API interface for managing heaps, enqueueing/consuming messages, and handling authentication.

## 📋 Overview

The API serves as the bridge between frontend applications and the disheap-engine, providing:

- **HTTP/JSON REST API** for all disheap operations
- **Authentication & Authorization** via API keys and JWT sessions
- **Admin Operations** with safety confirmations for destructive actions
- **Request Validation** and standardized error handling
- **Rate Limiting** and CORS support
- **Health Checks** and observability endpoints

## 🏗️ Architecture

```
Frontend/SDK → disheap-api → disheap-engine
                    ↓
                 Storage
```

### Key Components

- **gRPC Client**: Manages connections to disheap-engine with retry logic and connection pooling
- **HTTP Server**: Gin-based REST API with comprehensive middleware
- **Authentication**: API key management with Argon2id hashing + JWT sessions
- **Middleware Stack**: CORS, rate limiting, logging, error handling
- **Health Checks**: Readiness, liveness, and dependency health monitoring

## 🚀 Quick Start

### Prerequisites

- Go 1.23+ with modules enabled
- Access to a running `disheap-engine` instance
- Optional: PostgreSQL/SQLite for persistent storage

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd disheaps/disheap-api

# Install dependencies  
go mod download

# Run tests to verify setup
go test ./...

# Build the service
go build -o bin/disheap-api ./cmd/server
```

### Running the Service

```bash
# Run with default configuration
./bin/disheap-api

# Or run directly with Go
go run ./cmd/server

# Run with custom configuration
./bin/disheap-api -config config.yaml

# Run with environment variables
export DISHEAP_ENGINE_ENDPOINT="localhost:9090"
export DISHEAP_API_PORT="8080"
./bin/disheap-api
```

The API will start on `http://localhost:8080` by default.

## 🔑 Authentication

The API supports two authentication methods:

### 1. API Keys (for SDKs and programmatic access)

API keys use the format: `dh.<keyID>.<secret>`

**Example:**
```
Authorization: Bearer dh.YWJjZGVmZ2g.aWprbG1ub3BxcnN0dXZ3eHl6MTIzNA
```

**Security Features:**
- Cryptographically secure generation (32 bytes entropy)
- Argon2id hashing with salt for storage
- Constant-time verification to prevent timing attacks
- Automatic last-used timestamp tracking
- Revocation support

### 2. JWT Sessions (for frontend users)

JWT tokens for browser-based authentication:

```
Authorization: Bearer <jwt-token>
```

*Note: JWT implementation is currently in progress*

## 📖 API Reference

### Base URL
```
http://localhost:8080/v1
```

### Authentication Endpoints

```bash
# User login (JWT) - Coming Soon
POST   /v1/auth/login
POST   /v1/auth/logout  
POST   /v1/auth/refresh
GET    /v1/auth/me
```

### API Key Management

```bash
# Create API key
POST   /v1/keys
{
  "name": "My SDK Key"
}

# List API keys
GET    /v1/keys

# Get specific API key
GET    /v1/keys/{keyId}

# Revoke API key  
DELETE /v1/keys/{keyId}
```

### Heap Management

```bash
# Create heap
POST   /v1/heaps
{
  "topic": "work-queue",
  "mode": "MIN",
  "partitions": 3,
  "replication_factor": 2,
  "top_k_bound": 10
}

# List heaps
GET    /v1/heaps

# Get heap details
GET    /v1/heaps/{topic}

# Update heap configuration  
PATCH  /v1/heaps/{topic}

# Delete heap (requires confirmation)
DELETE /v1/heaps/{topic}?force=true&token=<confirmation_token>

# Purge messages (requires confirmation)
POST   /v1/heaps/{topic}:purge?force=true&token=<confirmation_token>
```

### Message Operations

```bash
# Enqueue single message
POST   /v1/enqueue
{
  "topic": "work-queue",
  "payload": "base64-encoded-data",
  "priority": 100,
  "partition_key": "user-123"
}

# Enqueue message batch
POST   /v1/enqueue:batch
{
  "requests": [
    {
      "topic": "work-queue", 
      "payload": "base64-data-1",
      "priority": 100
    },
    {
      "topic": "work-queue",
      "payload": "base64-data-2", 
      "priority": 200
    }
  ]
}

# Peek at messages
GET    /v1/peek/{topic}?limit=10

# Acknowledge message
POST   /v1/ack
{
  "topic": "work-queue",
  "message_id": "01H5GBVMG3D1SQW1YB4XFSJKHR",
  "lease_token": "token-here"
}

# Negative acknowledge (retry)
POST   /v1/nack  
{
  "topic": "work-queue",
  "message_id": "01H5GBVMG3D1SQW1YB4XFSJKHR", 
  "lease_token": "token-here",
  "reason": "processing failed"
}

# Extend lease
POST   /v1/extend
{
  "topic": "work-queue",
  "lease_token": "token-here",
  "extension": "30s"
}
```

### Statistics & Monitoring

```bash
# Get global statistics
GET    /v1/stats

# Get topic statistics  
GET    /v1/stats/{topic}

# Health check
GET    /health

# Readiness check
GET    /ready

# Metrics (Prometheus format)
GET    /metrics
```

## ⚙️ Configuration

### Environment Variables

```bash
# Engine connection
DISHEAP_ENGINE_ENDPOINT="localhost:9090"
DISHEAP_ENGINE_TLS_ENABLED=false

# API server
DISHEAP_API_HOST="0.0.0.0"
DISHEAP_API_PORT=8080
DISHEAP_API_TLS_CERT_FILE=""
DISHEAP_API_TLS_KEY_FILE=""

# Timeouts
DISHEAP_READ_TIMEOUT=30s
DISHEAP_WRITE_TIMEOUT=30s
DISHEAP_IDLE_TIMEOUT=60s

# Rate limiting
DISHEAP_RATE_LIMIT_ENABLED=true
DISHEAP_RATE_LIMIT_REQUESTS_PER_MINUTE=1000
DISHEAP_RATE_LIMIT_BURST_SIZE=100

# CORS
DISHEAP_CORS_ENABLED=true
DISHEAP_CORS_ALLOWED_ORIGINS="*"
DISHEAP_CORS_ALLOW_CREDENTIALS=false

# Storage (for user/key persistence)
DISHEAP_STORAGE_TYPE="sqlite"
DISHEAP_STORAGE_CONNECTION_STRING="./disheap.db"
```

### Configuration File Example

```yaml
# config.yaml
server:
  host: "0.0.0.0"
  port: 8080
  read_timeout: "30s"
  write_timeout: "30s"
  idle_timeout: "60s"

engine:
  endpoints: ["localhost:9090"]
  connect_timeout: "10s"
  request_timeout: "30s"
  tls_enabled: false
  
auth:
  jwt_secret: "your-secret-key"
  jwt_expiry: "24h"
  
rate_limit:
  enabled: true
  requests_per_minute: 1000
  burst_size: 100

cors:
  enabled: true
  allowed_origins: ["*"]
  allowed_methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
  allow_credentials: false
```

## 🔒 Security Features

### API Key Security
- **Cryptographic Generation**: 32 bytes of entropy using crypto/rand
- **Secure Hashing**: Argon2id with unique salts and secure defaults
- **Timing Attack Protection**: Constant-time comparisons
- **Format**: `dh.<base64-keyid>.<base64-secret>` (dots prevent base64 conflicts)
- **Revocation**: Immediate key deactivation capability

### Request Security  
- **Rate Limiting**: Per-IP and per-key limits with burst allowances
- **CORS Protection**: Configurable origin restrictions
- **Input Validation**: Comprehensive request validation with structured errors
- **Error Sanitization**: No sensitive information in error responses

### Network Security
- **TLS Support**: Optional TLS for API endpoints
- **mTLS Ready**: Infrastructure for mutual TLS with engine
- **Security Headers**: Standard security headers in responses

## 🧪 Development

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run specific package tests
go test ./pkg/auth/...
go test ./pkg/client/...
go test ./pkg/server/...

# Run benchmarks
go test -bench=. ./pkg/auth/...
```

### Code Organization

```
disheap-api/
├── cmd/server/          # Application entry point
├── pkg/
│   ├── auth/           # Authentication & API key management  
│   │   ├── apikey.go   # API key generation & validation
│   │   ├── hash.go     # Argon2id password/key hashing
│   │   ├── middleware.go # Auth middleware
│   │   └── user.go     # User management (in progress)
│   ├── client/         # gRPC client for engine communication
│   │   ├── client.go   # Main client implementation
│   │   ├── retry.go    # Retry logic with backoff
│   │   └── pool.go     # Connection pooling
│   └── server/         # HTTP server implementation
│       ├── server.go   # Main server setup
│       ├── middleware.go # HTTP middleware
│       └── health.go   # Health check handlers
└── test/               # Integration tests
```

### Adding New Endpoints

1. Define handler in appropriate package under `pkg/server/`
2. Add route registration in `server.go`
3. Implement request/response structs with validation tags
4. Add unit tests and integration tests
5. Update this README with API documentation

## 📊 Current Status

### ✅ Completed Features

- **Phase 1: Foundation**
  - [x] gRPC client with retry logic and connection pooling
  - [x] HTTP server with comprehensive middleware stack
  - [x] Health checks and graceful shutdown

- **Phase 2.1: API Key Authentication**
  - [x] Secure API key generation (`dh.<keyID>.<secret>` format)
  - [x] Argon2id hashing with proper salts
  - [x] Authentication middleware with timing attack protection
  - [x] Comprehensive test suite with benchmarks

### 🔄 In Progress

- **Phase 2.2: User Management & JWT**
  - [ ] User registration and login
  - [ ] JWT token generation and validation
  - [ ] Session management middleware

### 📋 Planned Features

- **Phase 3: HTTP Gateway**
  - [ ] grpc-gateway integration for automatic REST endpoints
  - [ ] Request/response translation and validation
  - [ ] OpenAPI/Swagger documentation generation

- **Phase 4: Admin & Data Handlers**
  - [ ] Complete heap management endpoints
  - [ ] Message operation endpoints
  - [ ] Safety confirmations for destructive operations

- **Phase 5: Advanced Features**
  - [ ] WebSocket support for streaming operations
  - [ ] Batch operation optimizations
  - [ ] Advanced monitoring and metrics

## 🐛 Troubleshooting

### Common Issues

**1. Cannot connect to engine**
```bash
# Check engine is running
curl -f http://localhost:9090/health

# Verify configuration
export DISHEAP_ENGINE_ENDPOINT="localhost:9090"
```

**2. API key authentication failing**
```bash
# Verify key format (should start with dh.)
echo $API_KEY | grep "^dh\."

# Check key hasn't been revoked
curl -H "Authorization: Bearer $API_KEY" http://localhost:8080/v1/keys
```

**3. Rate limiting errors** 
```bash
# Check current rate limit headers
curl -I http://localhost:8080/v1/heaps

# Adjust limits in configuration
export DISHEAP_RATE_LIMIT_REQUESTS_PER_MINUTE=2000
```

### Debug Logging

```bash
# Enable debug logging
export GIN_MODE=debug
export DISHEAP_LOG_LEVEL=debug

# Run with verbose output
go run ./cmd/server -v
```

## 📄 License

This project is part of the Disheap distributed priority messaging system.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run the test suite (`go test ./...`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)  
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

---

For more information about the overall Disheap system, see the main repository README and specification documents.
