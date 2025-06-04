# Makefile for Conduit Aggregate Processor

# Variables
BINARY_NAME := aggregate
WASM_OUTPUT := $(BINARY_NAME).wasm
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

# Default target
.PHONY: all
all: fmt test build

# Build the WASM processor
.PHONY: build
build:
	@echo "Building $(WASM_OUTPUT)..."
	CGO_ENABLED=0 GOOS=wasip1 GOARCH=wasm go build -o $(WASM_OUTPUT) ./cmd/processor
	@echo "✓ Built $(WASM_OUTPUT)"

# Generate paramgen code
.PHONY: generate
generate:
	go generate ./...

# Format code
.PHONY: fmt
fmt:
	go fmt ./...

# Run tests
.PHONY: test
test:
	go test -v -race ./...

# Run tests with coverage
.PHONY: coverage
coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Lint code
.PHONY: lint
lint:
	@if ! command -v golangci-lint >/dev/null; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi
	golangci-lint run

# Clean build artifacts
.PHONY: clean
clean:
	rm -f $(WASM_OUTPUT) coverage.out coverage.html *_paramgen.go
	go clean -testcache

# Development cycle
.PHONY: dev
dev: fmt test build
	@echo "✓ Development cycle complete"

# Pre-commit checks
.PHONY: check
check: fmt lint test build
	@echo "✓ All checks passed"

# Package for release
.PHONY: package
package: clean build
	@mkdir -p dist
	cp $(WASM_OUTPUT) dist/
	@if [ -f README.md ]; then cp README.md dist/; fi
	@if [ -f LICENSE ]; then cp LICENSE dist/; fi
	cd dist && sha256sum $(WASM_OUTPUT) > $(WASM_OUTPUT).sha256
	@echo "Package created in dist/"

# Install dependencies
.PHONY: deps
deps:
	go mod download
	go mod tidy

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build     Build WASM processor"
	@echo "  test      Run tests"
	@echo "  coverage  Run tests with coverage"
	@echo "  fmt       Format code"
	@echo "  lint      Run linter"
	@echo "  clean     Clean artifacts"
	@echo "  dev       Quick development cycle"
	@echo "  check     Run all checks"
	@echo "  package   Package for release"
	@echo "  deps      Install dependencies"