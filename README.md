# Conduit Aggregate Processor

A windowed aggregation processor for [Conduit](https://conduit.io) that groups streaming messages by time windows and field values, then applies aggregation functions like count, sum, average, min, max, and more.

## Quick Start

1. **Get the processor**:
   ```bash
   # Clone and build
   git clone <repo-url>
   cd conduit-processor-aggregate
   make build
   ```

2. **Place in Conduit plugins directory**:
   ```bash
   mkdir -p ./plugins && mv aggregate.wasm ./plugins/
   ```

3. **Create a pipeline**:
   ```yaml
   # pipeline.yaml
   version: 2.2
   pipelines:
     - id: sales-analytics
       status: running
       connectors:
         - id: orders-source
           type: source
           plugin: builtin:generator
           settings:
             rate: 10
             format.type: structured
             format.options.user_id: string
             format.options.amount: float
             format.options.product: string
         
         - id: analytics-sink
           type: destination
           plugin: builtin:log
       
       processors:
         - id: sales-aggregator
           plugin: standalone:aggregate
           settings:
             group_by: "user_id"
             window_size: "30s"
             aggregations: "count,sum,avg"
             fields: "amount"
   ```

4. **Run it**:
   ```bash
   conduit run pipeline.yaml
   ```

You'll see aggregated results every 30 seconds showing count, sum, and average amounts per user.

## Building

### Prerequisites
- Go 1.21+
- Git

### Build Steps
```bash
# Clone and setup
git clone <repo-url>
cd conduit-processor-aggregate

# Install dependencies
make deps

# Generate required code and build
make

# Or build just the WASM processor
make build
```

The `aggregate.wasm` file is your processor binary.

### Development Commands
```bash
make           # Format, test, and build (default)
make build     # Build WASM processor only
make test      # Run all tests
make coverage  # Generate HTML coverage report
make fmt       # Format Go code
make lint      # Run code linter
make clean     # Remove build artifacts
make dev       # Quick development cycle (fmt + test + build)
make check     # All quality checks (fmt + lint + test + build)
make package   # Package for distribution
make help      # Show all available targets
```

## Testing

### Unit Tests
```bash
# Run all tests
make test

# Run with coverage report
make coverage
# Opens coverage.html showing test coverage
```

### Development Testing
```bash
# Quick development cycle (format, test, build)
make dev

# Full quality checks before committing
make check
```

### Manual Testing
Create test data and pipeline:

```bash
# Create test data
echo '{"user_id": "alice", "amount": 100}' > test-data.jsonl
echo '{"user_id": "bob", "amount": 200}' >> test-data.jsonl
echo '{"user_id": "alice", "amount": 150}' >> test-data.jsonl

# Build processor
make build

# Run test pipeline
conduit run test-pipeline.yaml
```

### Test Pipeline
Create `test-pipeline.yaml`:
```yaml
version: 2.2
pipelines:
  - id: test-aggregate
    status: running
    connectors:
      - id: input
        type: source
        plugin: builtin:file
        settings:
          path: "test-data.jsonl"
      - id: output
        type: destination
        plugin: builtin:file
        settings:
          path: "aggregated.jsonl"
    processors:
      - id: test-aggregator
        plugin: standalone:aggregate
        settings:
          group_by: "category"
          window_size: "10s"
          aggregations: "count,sum"
          fields: "price"
```

## Running

### Basic Usage
```yaml
processors:
  - id: my-aggregator
    plugin: standalone:aggregate
    settings:
      group_by: "customer_id"           # Field to group by
      window_size: "5m"                 # Window duration
      aggregations: "count,sum,avg"     # Functions to apply
      fields: "order_total"             # Fields to aggregate
```

### Advanced Configuration
```yaml
processors:
  - id: advanced-aggregator
    plugin: standalone:aggregate
    settings:
      # Grouping
      group_by: "product.category"      # Supports nested fields
      
      # Windowing
      window_size: "1h"                 # 1 hour windows
      window_type: "sliding"            # or "tumbling" (default)
      slide_by: "15m"                   # Slide every 15 minutes
      
      # Time handling
      timestamp_field: "event_time"     # Use event time
      allowed_lateness: "5m"            # Accept late messages
      
      # Aggregations
      aggregations: "count,sum,avg,min,max,unique_count,collect"
      fields: "amount,quantity,user_id"
      
      # Output
      output_format: "per_group"        # or "single"
```

## Example Use Cases

### E-commerce Order Analytics
Track orders by product category with 5-minute windows:

```yaml
processors:
  - id: order-analytics
    plugin: standalone:aggregate
    settings:
      group_by: "product_category"
      window_size: "5m"
      aggregations: "count,sum,avg"
      fields: "order_total,quantity"
      timestamp_field: "order_time"
      output_format: "per_group"
```

**Input**:
```json
{"product_category": "electronics", "order_total": 299.99, "quantity": 1, "order_time": "2024-01-15T10:00:00Z"}
{"product_category": "electronics", "order_total": 199.99, "quantity": 2, "order_time": "2024-01-15T10:02:00Z"}
```

**Output** (after 5 minutes):
```json
{
  "window_start": "2024-01-15T10:00:00Z",
  "window_end": "2024-01-15T10:05:00Z",
  "window_type": "tumbling",
  "group_key": "electronics",
  "group_value": {
    "count": 2,
    "sum": {"order_total": 499.98, "quantity": 3},
    "avg": {"order_total": 249.99, "quantity": 1.5}
  },
  "count": 2
}
```

### IoT Sensor Monitoring
Monitor sensor readings with 30-second windows:

```yaml
processors:
  - id: sensor-monitor
    plugin: standalone:aggregate
    settings:
      group_by: "sensor_location"
      window_size: "30s"
      aggregations: "count,avg,min,max"
      fields: "temperature,humidity"
      timestamp_field: "reading_time"
      allowed_lateness: "10s"
```

### User Activity Tracking
Track user actions with sliding windows:

```yaml
processors:
  - id: user-activity
    plugin: standalone:aggregate
    settings:
      group_by: "user_id"
      window_size: "10m"
      window_type: "sliding"
      slide_by: "2m"
      aggregations: "count,unique_count,collect"
      fields: "action_type,session_id"
      timestamp_field: "event_timestamp"
      output_format: "single"
```

### Financial Transaction Analysis
Analyze transactions with multiple aggregations:

```yaml
processors:
  - id: transaction-analysis
    plugin: standalone:aggregate
    settings:
      group_by: "account_type"
      window_size: "1h"
      aggregations: "count,sum,avg,min,max,unique_count"
      fields: "amount,transaction_fee,merchant_id"
      timestamp_field: "transaction_time"
      allowed_lateness: "2m"
```

## Configuration Reference

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `group_by` | string | Field to group messages by | `"user_id"`, `"product.category"` |

### Window Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `window_size` | string | `"1m"` | Window duration | `"30s"`, `"5m"`, `"1h"` |
| `window_type` | string | `"tumbling"` | Window type | `"tumbling"`, `"sliding"` |
| `slide_by` | string | `"30s"` | Slide interval (sliding windows only) | `"10s"`, `"1m"` |

### Time Handling

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timestamp_field` | string | `""` | Field containing event timestamp | `"created_at"`, `"event.time"` |
| `allowed_lateness` | string | `"0s"` | Maximum lateness for messages | `"30s"`, `"5m"` |

### Aggregation Functions

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `aggregations` | []string | `["count"]` | Functions to apply | See functions below |
| `fields` | []string | `[]` | Fields to aggregate (required for numeric functions) | `["amount", "quantity"]` |

#### Available Functions

| Function | Description | Requires Fields |
|----------|-------------|-----------------|
| `count` | Count messages in group | No |
| `sum` | Sum numeric values | Yes |
| `avg` | Average numeric values | Yes |
| `min` | Minimum value | Yes |
| `max` | Maximum value | Yes |
| `unique_count` | Count unique values | Yes |
| `collect` | Collect all values into array | Yes |

### Output Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `output_format` | string | `"single"` | Output format | `"single"`, `"per_group"` |

#### Output Formats

- **`single`**: One record containing all groups
- **`per_group`**: Separate record for each group

### Window Types

#### Tumbling Windows
Non-overlapping windows where each message belongs to exactly one window.

```yaml
window_size: "5m"
window_type: "tumbling"
```

Timeline: `[10:00-10:05] [10:05-10:10] [10:10-10:15]`

#### Sliding Windows
Overlapping windows that advance by `slide_by` interval.

```yaml
window_size: "10m"
window_type: "sliding"
slide_by: "2m"
```

Timeline: `[10:00-10:10] [10:02-10:12] [10:04-10:14]`

### Nested Field Access

Use dot notation for nested JSON fields:

```yaml
group_by: "user.profile.id"
timestamp_field: "event.metadata.timestamp"
fields: ["transaction.amount", "product.price"]
```

**Example data**:
```json
{
  "user": {
    "profile": {"id": "user123"}
  },
  "transaction": {"amount": 99.99},
  "event": {
    "metadata": {"timestamp": "2024-01-15T10:00:00Z"}
  }
}
```

## Output Format Examples

### Single Format
One record with all groups:

```json
{
  "window_start": "2024-01-15T10:00:00Z",
  "window_end": "2024-01-15T10:05:00Z",
  "window_type": "tumbling",
  "groups": {
    "user1": {"count": 5, "sum": {"amount": 150.0}},
    "user2": {"count": 3, "sum": {"amount": 89.99}}
  },
  "total_count": 8
}
```

### Per-Group Format
Separate record for each group:

```json
{
  "window_start": "2024-01-15T10:00:00Z",
  "window_end": "2024-01-15T10:05:00Z",
  "window_type": "tumbling",
  "group_key": "user1",
  "group_value": {"count": 5, "sum": {"amount": 150.0}},
  "count": 5
}
```

## Troubleshooting

### Common Issues

**Processor not found**:
```
Error: plugin "standalone:aggregate" not found
```
- Ensure `aggregate.wasm` is in your plugins directory
- Check file permissions: `chmod +x aggregate.wasm`

**Group field missing**:
```
Error: group_by field 'user_id' not found
```
- Verify field exists in your data
- Check nested field syntax: `user.profile.id`

**No aggregated output**:
- Check if window duration has elapsed
- Verify timestamp format if using `timestamp_field`
- Check if messages are being filtered due to lateness

**Memory usage growing**:
- Add `allowed_lateness` limit
- Consider shorter window sizes
- Check for high cardinality in group keys

### Debug Mode

Enable debug logging to see processor internals:
```bash
conduit run --log.level=debug pipeline.yaml
```

You can also check your build and system setup:
```bash
# Verify everything is working
make check

# Clean and rebuild if needed
make clean build
```

Look for these log messages:
```
DEBUG processor window created window_key=tumbling_1705312800
DEBUG processor message added group=user1 window=tumbling_1705312800
DEBUG processor window completed group_count=5 message_count=25
```

## Roadmap

### v1.1.0 - Enhanced Aggregations
- **Array processing**: Aggregate values within JSON arrays
- **Conditional filters**: Include/exclude messages based on expressions
- **Multi-field operations**: Aggregate multiple fields in single pass
- **Custom expressions**: SQL-like aggregation expressions (`SUM(price * quantity)`)

### v1.2.0 - Advanced Windows
- **Rolling statistics**: Moving averages and rolling calculations
- **Session windows**: Group by activity sessions with timeouts
- **Window alignment**: Custom window start times and offsets
- **Late data recovery**: Configurable strategies for handling late arrivals

### v2.0.0 - Performance & Scale
- **Parallel processing**: Multi-threaded aggregation for high throughput
- **Memory optimization**: Reduced allocations and garbage collection
- **State compression**: Efficient storage for large aggregation states
- **Hot/cold separation**: Tiered storage for active vs. historical data

### v2.1.0 - Analytics Extensions
- **Percentile calculations**: P50, P90, P95, P99 aggregations
- **Statistical functions**: Variance, standard deviation, skewness
- **Approximate algorithms**: HyperLogLog cardinality, Count-Min Sketch frequency
- **Time-series analysis**: Trend detection and seasonal decomposition

### v3.0.0 - Advanced Features
- **Geo-spatial aggregation**: Location-based windowing and spatial functions
- **Pattern detection**: Complex event processing within windows
- **Multi-tenant support**: Isolated state per tenant
- **Custom plugins**: User-defined aggregation functions
- **Stream replay**: Historical reprocessing capabilities

### Enterprise Features
- **Schema evolution**: Backward-compatible state format changes
- **Output templates**: Configurable result structure and naming
- **Join operations**: Combine with reference data
- **Monitoring integration**: Metrics and health checks

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run quality checks: `make check`
5. Submit pull request

### Development Setup
```bash
git clone <your-fork>
cd conduit-processor-aggregate

# Install dependencies and verify setup
make deps

# Start development cycle
make dev

# Before committing, run all checks
make check
```

### Running Tests During Development
```bash
# Quick test cycle
make test

# Test with coverage
make coverage

# Full development workflow
make  # Formats, tests, and builds
```

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.