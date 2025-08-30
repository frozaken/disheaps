# Build stage
FROM golang:1.23-alpine AS builder

# Install required packages
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy repository root files (for protobuf dependencies)
COPY go.mod go.sum ./
COPY *.pb.go ./

# Copy disheap-engine specific files
COPY disheap-engine/go.mod disheap-engine/go.sum ./disheap-engine/

# Change to disheap-engine directory and download dependencies
WORKDIR /app/disheap-engine
RUN go mod download

# Copy disheap-engine source code
COPY disheap-engine/ ./

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o disheap-engine \
    ./cmd

# Production stage
FROM alpine:3.19 AS production

# Install required packages
RUN apk --no-cache add ca-certificates curl bash

# Create non-root user
RUN addgroup -g 1000 disheap && \
    adduser -D -s /bin/sh -u 1000 -G disheap disheap

# Create data directories
RUN mkdir -p /var/lib/disheap/badger /var/lib/disheap/raft /var/log/disheap && \
    chown -R disheap:disheap /var/lib/disheap /var/log/disheap

# Copy the binary
COPY --from=builder /app/disheap-engine/disheap-engine /usr/local/bin/disheap-engine

# Make it executable
RUN chmod +x /usr/local/bin/disheap-engine

# Switch to non-root user
USER disheap

# Set working directory
WORKDIR /home/disheap

# Expose ports
# 9090: gRPC API
# 8080: HTTP health/admin
# 9091: Metrics
# 8300: Raft transport
EXPOSE 9090 8080 9091 8300

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default command
CMD ["/usr/local/bin/disheap-engine"]

# Development stage (for testing)
FROM golang:1.23-alpine AS development

RUN apk add --no-cache git ca-certificates tzdata curl bash

WORKDIR /app

# Install air for hot reloading
RUN go install github.com/cosmtrek/air@latest

# Copy go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Expose development ports
EXPOSE 9090 8080 9091 8300

CMD ["air", "-c", ".air.toml"]
