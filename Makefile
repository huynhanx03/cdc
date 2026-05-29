APP_NAME := cdc
BIN_DIR := bin
CONFIG_FILE := config/config.yaml
COMPOSE_FILE := deploy/docker-compose.yaml
PROTO_DIR := proto
PROTO_IMAGE_NAME := cdc-proto-gen
PROTO_DOCKERFILE := $(PROTO_DIR)/Dockerfile
DOCKER_HOST_FOR_TESTS ?= $(shell docker context inspect $$(docker context show 2>/dev/null) --format '{{json .Endpoints.docker.Host}}' 2>/dev/null | tr -d '"')

.PHONY: all help build run test test-unit test-integration test-e2e test-all bench bench-pipeline tidy up down fix-perms clean gen-proto proto-lint proto-breaking .docker-check .proto-image fe-install fe-dev fe-build fe-lint

all: tidy build

help:
	@printf "%s\n" \
		"Targets:" \
		"  build       Build $(APP_NAME) binary" \
		"  run         Build and run using $(CONFIG_FILE)" \
		"  test        Run Go unit tests" \
		"  test-unit   Run fast Go unit tests" \
		"  test-integration Run Docker-backed integration tests" \
		"  test-e2e    Run E2E workflow tests" \
		"  test-all    Run unit, integration, and E2E tests" \
		"  bench       Run all Go benchmarks" \
		"  bench-pipeline Run CDC pipeline benchmarks" \
		"  tidy        Run go mod tidy" \
		"  up          Start docker compose stack" \
		"  down        Stop docker compose stack" \
		"  fe-build    Build Vite frontend" \
		"  fe-lint     Lint frontend" \
		"  gen-proto   Generate protobuf code"

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/cdc

run: build
	./$(BIN_DIR)/$(APP_NAME) -config $(CONFIG_FILE)

test: test-unit

test-unit:
	go test ./...

test-integration: .docker-check
	DOCKER_HOST=$(DOCKER_HOST_FOR_TESTS) TESTCONTAINERS_RYUK_DISABLED=true go test -tags=integration ./tests/integration/...

test-e2e: .docker-check
	DOCKER_HOST=$(DOCKER_HOST_FOR_TESTS) TESTCONTAINERS_RYUK_DISABLED=true go test -tags=e2e ./tests/e2e/...

test-all: test-unit test-integration test-e2e

bench:
	go test -bench=. -benchmem ./...

bench-pipeline:
	go test -bench=. -benchmem ./benchmarks/pipeline/...

tidy:
	go mod tidy

up:
	docker compose --project-directory . -f $(COMPOSE_FILE) up -d

down:
	docker compose --project-directory . -f $(COMPOSE_FILE) down

fix-perms:
	@echo "Fixing nats-data permissions..."
	sudo chown -R $(shell id -u):$(shell id -g) nats-data

clean:
	rm -rf $(BIN_DIR)

.docker-check:
	@docker info > /dev/null 2>&1 || (echo "Error: Docker daemon is not running. Please start Docker and try again." >&2; exit 1)

.proto-image:
	@docker image inspect $(PROTO_IMAGE_NAME) > /dev/null 2>&1 || \
		(echo "Building proto generation image..."; docker build -f $(PROTO_DOCKERFILE) -t $(PROTO_IMAGE_NAME) $(PROTO_DIR))

gen-proto: .docker-check .proto-image
	@docker run --rm \
		-v $(PWD):/workspace \
		--user $(shell id -u):$(shell id -g) \
		-e BUF_CACHE_DIR=/tmp/buf-cache \
		$(PROTO_IMAGE_NAME) sh /workspace/$(PROTO_DIR)/generate.sh

proto-lint: .docker-check .proto-image
	@docker run --rm \
		-v $(PWD):/workspace \
		--user $(shell id -u):$(shell id -g) \
		-e BUF_CACHE_DIR=/tmp/buf-cache \
		$(PROTO_IMAGE_NAME) sh -c "cd /workspace/proto && buf lint"

proto-breaking: .docker-check .proto-image
	@docker run --rm \
		-v $(PWD):/workspace \
		--user $(shell id -u):$(shell id -g) \
		-e BUF_CACHE_DIR=/tmp/buf-cache \
		$(PROTO_IMAGE_NAME) sh -c "cd /workspace/proto && buf breaking --against '.git#subdir=proto'"


# ─── Frontend ────────────────────────────────────────────────────────

fe-install:
	cd website && npm install

fe-dev:
	cd website && npm run dev

fe-build:
	cd website && npm run build

fe-lint:
	cd website && npm run lint
