# Add these targets to your existing Makefile

# Test targets
.PHONY: test test-unit test-integration test-coverage test-performance test-config test-examples

# Run all tests
test: test-unit test-integration

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	go test -v -run "^Test[^I]" ./...

# Run integration tests only  
test-integration:
	@echo "Running integration tests..."
	go test -v -run "^TestIntegration" ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run performance tests
test-performance:
	@echo "Running performance tests..."
	go test -v -run "^TestIntegration_PerformanceUnderLoad" ./...

# Test configuration parsing specifically
test-config:
	@echo "Testing configuration parsing..."
	go test -v -run "TestProcessor_Configure.*" ./...

# Test README examples
test-examples:
	@echo "Testing README examples..."
	go test -v -run "TestIntegration_READMEExamples" ./...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Run specific test with verbose output
test-verbose:
	@echo "Running tests with verbose output..."
	go test -v -race ./...

# Test with race detection
test-race:
	@echo "Running tests with race detection..."
	go test -race ./...

# Continuous testing (watches for file changes)
test-watch:
	@echo "Starting continuous testing..."
	@which fswatch > /dev/null || (echo "fswatch not installed. Install with: brew install fswatch" && exit 1)
	@fswatch -o . | xargs -n1 -I{} make test-unit

# Clean test artifacts
test-clean:
	@echo "Cleaning test artifacts..."
	rm -f coverage.out coverage.html
	go clean -testcache

# Test pipeline configurations
test-pipelines: build
	@echo "Testing pipeline configurations..."
	@mkdir -p plugins test-output
	@cp $(WASM_OUTPUT) plugins/
	@echo "Testing basic pipeline..."
	@echo '{"user_id": "alice", "amount": 100}' > test-data.jsonl
	@echo '{"user_id": "bob", "amount": 200}' >> test-data.jsonl
	@echo '{"user_id": "alice", "amount": 150}' >> test-data.jsonl
	@echo "version: 2.2" > test-pipeline.yaml
	@echo "pipelines:" >> test-pipeline.yaml
	@echo "  - id: test-aggregate" >> test-pipeline.yaml
	@echo "    status: running" >> test-pipeline.yaml
	@echo "    connectors:" >> test-pipeline.yaml
	@echo "      - id: input" >> test-pipeline.yaml
	@echo "        type: source" >> test-pipeline.yaml
	@echo "        plugin: builtin:file" >> test-pipeline.yaml
	@echo "        settings:" >> test-pipeline.yaml
	@echo "          path: test-data.jsonl" >> test-pipeline.yaml
	@echo "      - id: output" >> test-pipeline.yaml
	@echo "        type: destination" >> test-pipeline.yaml
	@echo "        plugin: builtin:file" >> test-pipeline.yaml
	@echo "        settings:" >> test-pipeline.yaml
	@echo "          path: test-output/aggregated.jsonl" >> test-pipeline.yaml
	@echo "    processors:" >> test-pipeline.yaml
	@echo "      - id: test-aggregator" >> test-pipeline.yaml
	@echo "        plugin: standalone:aggregate" >> test-pipeline.yaml
	@echo "        settings:" >> test-pipeline.yaml
	@echo "          group_by: user_id" >> test-pipeline.yaml
	@echo "          window_size: 1s" >> test-pipeline.yaml
	@echo "          aggregations: count,sum,avg" >> test-pipeline.yaml
	@echo "          fields: amount" >> test-pipeline.yaml
	@echo "          output_format: per_group" >> test-pipeline.yaml
	@echo "Pipeline configuration created: test-pipeline.yaml"
	@echo "To test manually: conduit run test-pipeline.yaml"

# Full test suite
test-all: test-clean generate test-coverage test-integration test-performance bench
	@echo "✓ All tests completed successfully"

# Help for test targets
test-help:
	@echo "Available test targets:"
	@echo "  test           - Run all tests (unit + integration)"
	@echo "  test-unit      - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  test-performance - Run performance tests"
	@echo "  test-config    - Test configuration parsing"
	@echo "  test-examples  - Test README examples"
	@echo "  test-race      - Run tests with race detection"
	@echo "  test-verbose   - Run tests with verbose output"
	@echo "  test-watch     - Continuous testing (requires fswatch)"
	@echo "  test-pipelines - Test with real pipeline configurations"
	@echo "  test-clean     - Clean test artifacts"
	@echo "  test-all       - Run full test suite"
	@echo "  bench          - Run benchmarks"