# --- Stage 1: Build the Go CDC binary ---
FROM golang:1.23-bookworm AS builder-go
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/cdc-bin ./cmd/cdc/main.go

# --- Stage 2: Build the Vite UI ---
FROM node:20-bookworm AS builder-node
WORKDIR /app
COPY website/package*.json ./
RUN npm ci --silent
COPY website/ .
RUN npm run build

# --- Final Stage: Minimal runtime image ---
FROM node:20-slim

# Install tini, curl (healthcheck) and bash
RUN apt-get update \
  && apt-get install -y --no-install-recommends tini curl bash \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries
COPY --from=builder-go /out/cdc-bin /app/cdc

# Copy config
COPY config/config.example.yaml /app/config.yaml

# Copy Vite build files and provide a small static server for the SPA.
COPY --from=builder-node /app/dist /app/dist
RUN printf '%s\n' \
  'const http = require("http");' \
  'const fs = require("fs");' \
  'const path = require("path");' \
  'const root = path.join(__dirname, "dist");' \
  'const types = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".json": "application/json; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".ico": "image/x-icon", ".woff": "font/woff", ".woff2": "font/woff2" };' \
  'const server = http.createServer((req, res) => {' \
  '  const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);' \
  '  const safePath = path.normalize(urlPath).replace(/^(\.\.[/\\])+/, "");' \
  '  let file = path.join(root, safePath === "/" ? "index.html" : safePath);' \
  '  if (!file.startsWith(root)) file = path.join(root, "index.html");' \
  '  fs.stat(file, (err, stat) => {' \
  '    if (err || !stat.isFile()) file = path.join(root, "index.html");' \
  '    fs.readFile(file, (readErr, data) => {' \
  '      if (readErr) { res.writeHead(404); res.end("Not found"); return; }' \
  '      res.writeHead(200, { "Content-Type": types[path.extname(file)] || "application/octet-stream" });' \
  '      res.end(data);' \
  '    });' \
  '  });' \
  '});' \
  'server.listen(process.env.PORT || 5173, "0.0.0.0");' \
  > /app/server.js

# Copy entrypoint script
COPY deploy/docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Production settings
ENV NODE_ENV=production
ENV PORT=5173

# Expose app-specific ports
EXPOSE 9090 9091 5173

# Lightweight healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:9091/api/v1/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
