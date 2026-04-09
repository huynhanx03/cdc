APP_NAME := cdc
BIN_DIR := bin
CONFIG_FILE := config.yaml

.PHONY: all build run test tidy up down fix-perms clean gen-proto

all: tidy build

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
	docker-compose up -d

down:
	docker-compose down

fix-perms:
	@echo "Fixing nats-data permissions..."
	sudo chown -R $(shell id -u):$(shell id -g) nats-data

clean:
	rm -rf $(BIN_DIR)

gen-proto:
	buf generate