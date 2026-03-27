# --- Stage 1: Build the Go CDC binary ---
FROM golang:1.23-bookworm AS builder-go
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o cdc-bin ./cmd/cdc/main.go

# --- Stage 2: Build the Next.js UI (Standalone) ---
FROM node:20-bookworm AS builder-node
WORKDIR /app/client
COPY client/package*.json ./
RUN npm install
COPY client/ .
RUN npm run build

# --- Final Stage: Clean Application Image ---
FROM node:19-slim
# Install curl for healthchecks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries
COPY --from=builder-go /app/cdc-bin /app/cdc

# Copy config
COPY config.example.yaml /app/config.yaml

# Copy Next.js Standalone build files
# Next.js 'standalone' moves all code to .next/standalone/
COPY --from=builder-node /app/client/.next/standalone /app
COPY --from=builder-node /app/client/.next/static /app/.next/static
COPY --from=builder-node /app/client/public /app/public

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Production settings
ENV NODE_ENV=production
ENV PORT=9092

# Expose app-specific ports
# CDC API: 9090 (gRPC), 9091 (HTTP)
# Next.js UI: 9092
EXPOSE 9090 9091 9092

ENTRYPOINT ["/app/entrypoint.sh"]
