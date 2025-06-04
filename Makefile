# Makefile for Conduit Aggregate Processor Plugin

.PHONY: build test run clean install-deps fmt lint vet

# Variables
BINARY_NAME=conduit-processor-aggregate
GO_FILES=$(shell find . -name "*.go" -type f)
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.version=$(VERSION)"

# Default target
all: fmt vet test build

# Build the plugin binary
build:
	@echo "Building $(BINARY_NAME)..."
	go build $(LDFLAGS) -o bin/$(BINARY_NAME) .
	@echo "Build complete: bin/$(BINARY_NAME)"

# Run tests
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.out ./...
	@echo "Tests complete"

# Run tests with coverage report
test-coverage: test
	@echo "Generating coverage report..."
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run the plugin locally (requires Conduit)
run: build
	@echo "Running $(BINARY_NAME)..."
	@echo "Note: This plugin requires Conduit to run. Use 'conduit run' with a pipeline configuration."
	@echo "See README.md for example configurations."

# Install dependencies
install-deps:
	@echo "Installing dependencies..."
	go mod download
	go mod tidy

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Lint code
lint:
	@echo "Running golint..."
	@command -v golint >/dev/null 2>&1 || { echo "golint not installed. Install with: go install golang.org/x/lint/golint@latest"; exit 1; }
	golint ./...

# Vet code
vet:
	@echo "Running go vet..."
	go vet ./...

# Run static analysis
static-check:
	@echo "Running staticcheck..."
	@command -v staticcheck >/dev/null 2>&1 || { echo "staticcheck not installed. Install with: go install honnef.co/go/tools/cmd/staticcheck@latest"; exit 1; }
	staticcheck ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf bin/
	rm -f coverage.out coverage.html
	go clean

# Run benchmark tests
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Development target with file watching (requires entr)
dev:
	@echo "Starting development mode (requires 'entr')..."
	@command -v entr >/dev/null 2>&1 || { echo "entr not installed. Install with your package manager."; exit 1; }
	find . -name "*.go" | entr -r make test

# Build for multiple platforms
build-all:
	@echo "Building for multiple platforms..."
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-linux-amd64 .
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-darwin-arm64 .
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-windows-amd64.exe .
	@echo "Multi-platform build complete"

# Docker build
docker-build:
	@echo "Building Docker image..."
	docker build -t conduit-processor-aggregate:$(VERSION) .

# Help target
help:
	@echo "Available targets:"
	@echo "  build         Build the plugin binary"
	@echo "  test          Run tests with race detection"
	@echo "  test-coverage Run tests and generate coverage report"
	@echo "  run           Run the plugin (requires Conduit)"
	@echo "  install-deps  Install Go dependencies"
	@echo "  fmt           Format Go code"
	@echo "  lint          Run golint"
	@echo "  vet           Run go vet"
	@echo "  static-check  Run staticcheck"
	@echo "  clean         Clean build artifacts"
	@echo "  bench         Run benchmark tests"
	@echo "  dev           Development mode with file watching"
	@echo "  build-all     Build for multiple platforms"
	@echo "  docker-build  Build Docker image"
	@echo "  help          Show this help message"