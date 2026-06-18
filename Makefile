.PHONY: help build run test clean docker-build docker-up docker-down docker-logs fmt lint

# Variables
APP_NAME=nexusws
DOCKER_IMAGE=$(APP_NAME):latest
DOCKER_COMPOSE_FILE=compose.yml
GO_MODULE=github.com/snigdhodutta/nexsus-v2/nexusws
BUILD_DIR=./nexusws
EXAMPLES_DIR=./nexusws/examples

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the application
	@echo "Building $(APP_NAME)..."
	cd $(BUILD_DIR) && go build -o ../bin/$(APP_NAME) ./server.go

build-example: ## Build the example application
	@echo "Building example..."
	cd $(EXAMPLES_DIR) && go build -o ../../bin/example main.go

run: build ## Build and run the application
	@echo "Running $(APP_NAME)..."
	./bin/$(APP_NAME)

run-example: build-example ## Build and run the example
	@echo "Running example..."
	./bin/example

test: ## Run tests
	@echo "Running tests..."
	cd $(BUILD_DIR) && go test -v ./...

test-coverage: ## Run tests with coverage
	@echo "Running tests with coverage..."
	cd $(BUILD_DIR) && go test -v -coverprofile=coverage.out ./...
	cd $(BUILD_DIR) && go tool cover -html=coverage.out -o coverage.html

fmt: ## Format Go code
	@echo "Formatting code..."
	cd $(BUILD_DIR) && go fmt ./...
	cd $(EXAMPLES_DIR) && go fmt ./...

lint: ## Run linter
	@echo "Running linter..."
	cd $(BUILD_DIR) && golangci-lint run ./...
	cd $(EXAMPLES_DIR) && golangci-lint run ./...

tidy: ## Tidy go modules
	@echo "Tidying modules..."
	cd $(BUILD_DIR) && go mod tidy
	cd $(EXAMPLES_DIR) && go mod tidy

clean: ## Clean build artifacts
	@echo "Cleaning..."
	rm -rf ./bin
	rm -f $(BUILD_DIR)/coverage.out $(BUILD_DIR)/coverage.html

docker-build: ## Build Docker image
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE) -f $(BUILD_DIR)/Dockerfile $(BUILD_DIR)

docker-up: ## Start services with Docker Compose
	@echo "Starting services..."
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d

docker-down: ## Stop services with Docker Compose
	@echo "Stopping services..."
	docker compose -f $(DOCKER_COMPOSE_FILE) down

docker-logs: ## Show Docker Compose logs
	docker compose -f $(DOCKER_COMPOSE_FILE) logs -f

docker-restart: docker-down docker-up ## Restart Docker services

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	cd $(BUILD_DIR) && go mod download

.DEFAULT_GOAL := help
