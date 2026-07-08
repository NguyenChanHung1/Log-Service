.PHONY: build test clean compose-up compose-down demo

BIN_DIR := bin

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/log-api ./cmd/log-api
	go build -o $(BIN_DIR)/log-worker ./cmd/log-worker
	go build -o $(BIN_DIR)/log-generator ./cmd/log-generator

test:
	go test ./...

clean:
	rm -rf $(BIN_DIR)

compose-up:
	docker compose -f deployments/docker-compose.yml up --build

compose-down:
	docker compose -f deployments/docker-compose.yml down

demo:
	./scripts/run-demo.sh
