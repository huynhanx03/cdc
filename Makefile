APP_NAME := cdc
BIN_DIR := bin
CONFIG_FILE := config/config.yaml
COMPOSE_FILE := deploy/docker-compose.yaml
PROTO_DIR := proto
PROTO_IMAGE_NAME := cdc-proto-gen
PROTO_DOCKERFILE := $(PROTO_DIR)/Dockerfile

.PHONY: all help build run test tidy up down fix-perms clean gen-proto proto-lint proto-breaking .docker-check .proto-image fe-install fe-dev fe-build fe-lint

all: tidy build

help:
	@printf "%s\n" \
		"Targets:" \
		"  build       Build $(APP_NAME) binary" \
		"  run         Build and run using $(CONFIG_FILE)" \
		"  test        Run Go tests" \
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

test:
	go test -v ./...

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
