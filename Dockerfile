# Stage 1: Build binary using official Go Alpine builder
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Download dependencies first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy full source tree
COPY . .

# Build stripped, static zero-cgo binary
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /pocketqueue ./cmd/pocketqueue

# Stage 2: Ultra-minimal scratch runtime container (< 8 MB)
FROM scratch

# Copy TLS certificates for secure connections
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /pocketqueue /pocketqueue

# Default wire ports: 9092 (Kafka), 6379 (Redis Streams)
EXPOSE 9092 6379

ENTRYPOINT ["/pocketqueue", "--headless", "--host", "0.0.0.0"]
