package aggregate

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_READMEExamples tests all the examples shown in the README
func TestIntegration_READMEExamples(t *testing.T) {
	t.Run("basic_sales_analytics", func(t *testing.T) {
		// This matches the example from the README
		p := setupTestProcessor(t, map[string]string{
			"group_by":     "user_id",
			"window_size":  "30s",
			"aggregations": "count,sum,avg",
			"fields":       "amount",
		})

		ctx := context.Background()

		// Simulate the generator data
		records := []opencdc.Record{
			createTestRecord(t, map[string]interface{}{
				"user_id": "alice",
				"amount":  100.0,
				"product": "laptop",
			}),
			createTestRecord(t, map[string]interface{}{
				"user_id": "alice",
				"amount":  150.0,
				"product": "mouse",
			}),
			createTestRecord(t, map[string]interface{}{
				"user_id": "bob",
				"amount":  200.0,
				"product": "keyboard",
			}),
		}

		results := p.Process(ctx, records)

		// All records should be filtered (waiting for window completion)
		for _, result := range results {
			assert.IsType(t, sdk.FilterRecord{}, result)
		}

		// Verify window state
		assert.Len(t, p.windows, 1)

		var window *WindowState
		for _, w := range p.windows {
			window = w
			break
		}

		require.NotNil(t, window)
		assert.Equal(t, 3, window.MessageCount)
		assert.Len(t, window.Groups, 2) // alice and bob
		assert.Len(t, window.Groups["alice"], 2)
		assert.Len(t, window.Groups["bob"], 1)
	})

	t.Run("ecommerce_order_analytics", func(t *testing.T) {
		// Example from README: E-commerce Order Analytics
		p := setupTestProcessor(t, map[string]string{
			"group_by":        "product_category",
			"window_size":     "5m",
			"aggregations":    "count,sum,avg",
			"fields":          "order_total,quantity",
			"timestamp_field": "order_time",
			"output_format":   "per_group",
		})

		ctx := context.Background()
		now := time.Now()

		records := []opencdc.Record{
			createTestRecord(t, map[string]interface{}{
				"product_category": "electronics",
				"order_total":      299.99,
				"quantity":         1,
				"order_time":       now.Format(time.RFC3339),
			}),
			createTestRecord(t, map[string]interface{}{
				"product_category": "electronics",
				"order_total":      199.99,
				"quantity":         2,
				"order_time":       now.Add(2 * time.Minute).Format(time.RFC3339),
			}),
			createTestRecord(t, map[string]interface{}{
				"product_category": "books",
				"order_total":      29.99,
				"quantity":         1,
				"order_time":       now.Add(3 * time.Minute).Format(time.RFC3339),
			}),
		}

		results := p.Process(ctx, records)

		// All records should be filtered (waiting for window completion)
		for _, result := range results {
			assert.IsType(t, sdk.FilterRecord{}, result)
		}

		// Verify configuration parsing
		assert.Equal(t, []string{"count", "sum", "avg"}, p.aggregations)
		assert.Equal(t, []string{"order_total", "quantity"}, p.fields)
		assert.Equal(t, "per_group", p.config.OutputFormat)

		// Verify window state
		assert.Len(t, p.windows, 1)
		var window *WindowState
		for _, w := range p.windows {
			window = w
			break
		}

		require.NotNil(t, window)
		assert.Equal(t, 3, window.MessageCount)
		assert.Len(t, window.Groups, 2) // electronics and books

		// Test aggregation calculations
		electronicsAgg := p.calculateAggregations(window.Groups["electronics"])
		assert.Equal(t, 2, electronicsAgg["count"])

		sums := electronicsAgg["sum"].(map[string]float64)
		assert.InDelta(t, 499.98, sums["order_total"], 0.01)
		assert.Equal(t, 3.0, sums["quantity"])
	})

	t.Run("iot_sensor_monitoring", func(t *testing.T) {
		// Example from README: IoT Sensor Monitoring
		p := setupTestProcessor(t, map[string]string{
			"group_by":         "sensor_location",
			"window_size":      "30s",
			"aggregations":     "count,avg,min,max",
			"fields":           "temperature,humidity",
			"timestamp_field":  "reading_time",
			"allowed_lateness": "10s",
		})

		ctx := context.Background()
		now := time.Now()

		records := []opencdc.Record{
			createTestRecord(t, map[string]interface{}{
				"sensor_location": "warehouse_a",
				"temperature":     22.5,
				"humidity":        45.0,
				"reading_time":    now.Format(time.RFC3339),
			}),
			createTestRecord(t, map[string]interface{}{
				"sensor_location": "warehouse_a",
				"temperature":     23.1,
				"humidity":        47.2,
				"reading_time":    now.Add(15 * time.Second).Format(time.RFC3339),
			}),
			// Late message within threshold
			createTestRecord(t, map[string]interface{}{
				"sensor_location": "warehouse_a",
				"temperature":     21.8,
				"humidity":        44.1,
				"reading_time":    now.Add(-5 * time.Second).Format(time.RFC3339),
			}),
		}

		results := p.Process(ctx, records)

		// Verify configuration
		assert.Equal(t, []string{"count", "avg", "min", "max"}, p.aggregations)
		assert.Equal(t, []string{"temperature", "humidity"}, p.fields)
		assert.Equal(t, 10*time.Second, p.lateness)

		// All messages should be processed (none too late)
		for _, result := range results {
			assert.IsType(t, sdk.FilterRecord{}, result)
		}

		// Verify aggregations
		var window *WindowState
		for _, w := range p.windows {
			window = w
			break
		}

		warehouseAgg := p.calculateAggregations(window.Groups["warehouse_a"])
		assert.Equal(t, 3, warehouseAgg["count"])

		// Check temperature min/max
		mins := warehouseAgg["min"].(map[string]float64)
		maxs := warehouseAgg["max"].(map[string]float64)
		assert.Equal(t, 21.8, mins["temperature"])
		assert.Equal(t, 23.1, maxs["temperature"])
	})

	t.Run("sliding_window_user_activity", func(t *testing.T) {
		// Example from README: User Activity Tracking with sliding windows
		p := setupTestProcessor(t, map[string]string{
			"group_by":        "user_id",
			"window_size":     "10m",
			"window_type":     "sliding",
			"slide_by":        "2m",
			"aggregations":    "count,unique_count,collect",
			"fields":          "action_type,session_id",
			"timestamp_field": "event_timestamp",
			"output_format":   "single",
		})

		ctx := context.Background()
		now := time.Now()

		records := []opencdc.Record{
			createTestRecord(t, map[string]interface{}{
				"user_id":         "user123",
				"action_type":     "click",
				"session_id":      "session_abc",
				"event_timestamp": now.Format(time.RFC3339),
			}),
			createTestRecord(t, map[string]interface{}{
				"user_id":         "user123",
				"action_type":     "scroll",
				"session_id":      "session_abc",
				"event_timestamp": now.Add(1 * time.Minute).Format(time.RFC3339),
			}),
			createTestRecord(t, map[string]interface{}{
				"user_id":         "user456",
				"action_type":     "click",
				"session_id":      "session_def",
				"event_timestamp": now.Add(30 * time.Second).Format(time.RFC3339),
			}),
		}

		results := p.Process(ctx, records)

		// All records should be filtered (waiting for window completion)
		for _, result := range results {
			assert.IsType(t, sdk.FilterRecord{}, result)
		}

		// Verify configuration
		assert.Equal(t, "sliding", p.config.WindowType)
		assert.Equal(t, 2*time.Minute, p.slideDur)
		assert.Equal(t, []string{"count", "unique_count", "collect"}, p.aggregations)
		assert.Equal(t, "single", p.config.OutputFormat)

		// Should create multiple sliding windows
		assert.GreaterOrEqual(t, len(p.windows), 1)
	})
}

// TestIntegration_FullPipelineSimulation simulates a complete window lifecycle
func TestIntegration_FullPipelineSimulation(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":      "category",
		"window_size":   "100ms", // Short for testing
		"aggregations":  "count,sum,avg,min,max,unique_count",
		"fields":        "price,quantity",
		"output_format": "per_group",
	})

	ctx := context.Background()

	// Phase 1: Add records to window
	records := []opencdc.Record{
		createTestRecord(t, map[string]interface{}{
			"category": "electronics",
			"price":    299.99,
			"quantity": 1,
		}),
		createTestRecord(t, map[string]interface{}{
			"category": "electronics",
			"price":    199.99,
			"quantity": 2,
		}),
		createTestRecord(t, map[string]interface{}{
			"category": "books",
			"price":    29.99,
			"quantity": 1,
		}),
		createTestRecord(t, map[string]interface{}{
			"category": "books",
			"price":    39.99,
			"quantity": 1,
		}),
	}

	// Process records
	results := p.Process(ctx, records)

	// All should be filtered initially
	assert.Len(t, results, 4)
	for _, result := range results {
		assert.IsType(t, sdk.FilterRecord{}, result)
	}

	// Verify window state before completion
	assert.Len(t, p.windows, 1)
	var window *WindowState
	for _, w := range p.windows {
		window = w
		break
	}

	assert.Equal(t, 4, window.MessageCount)
	assert.Len(t, window.Groups, 2)
	assert.Len(t, window.Groups["electronics"], 2)
	assert.Len(t, window.Groups["books"], 2)

	// Phase 2: Wait for window to complete and process completion
	time.Sleep(150 * time.Millisecond)
	completionResults := p.emitCompletedWindows(ctx)

	// Should produce 2 records (per_group format)
	assert.Len(t, completionResults, 2)

	// Verify output structure
	var electronicsResult, booksResult map[string]interface{}
	for _, result := range completionResults {
		singleRecord := result.(sdk.SingleRecord)
		record := opencdc.Record(singleRecord)

		var payload map[string]interface{}
		err := json.Unmarshal(record.Payload.After.Bytes(), &payload)
		require.NoError(t, err)

		groupKey := payload["group_key"].(string)
		if groupKey == "electronics" {
			electronicsResult = payload
		} else if groupKey == "books" {
			booksResult = payload
		}
	}

	// Verify electronics aggregations
	require.NotNil(t, electronicsResult)
	assert.Equal(t, "electronics", electronicsResult["group_key"])
	assert.Equal(t, 2, electronicsResult["count"])

	electronicsGroupValue := electronicsResult["group_value"].(map[string]interface{})
	assert.Equal(t, 2, electronicsGroupValue["count"])

	electronicsSum := electronicsGroupValue["sum"].(map[string]interface{})
	assert.InDelta(t, 499.98, electronicsSum["price"], 0.01)
	assert.Equal(t, 3.0, electronicsSum["quantity"])

	// Verify books aggregations
	require.NotNil(t, booksResult)
	assert.Equal(t, "books", booksResult["group_key"])
	assert.Equal(t, 2, booksResult["count"])

	booksGroupValue := booksResult["group_value"].(map[string]interface{})
	booksSum := booksGroupValue["sum"].(map[string]interface{})
	assert.InDelta(t, 69.98, booksSum["price"], 0.01)
	assert.Equal(t, 2.0, booksSum["quantity"])

	// Verify window was cleaned up
	assert.Len(t, p.windows, 0)
}

// TestIntegration_ConfigurationCompatibility tests backward compatibility
func TestIntegration_ConfigurationCompatibility(t *testing.T) {
	// Test that old-style individual parameters still work
	tests := []struct {
		name   string
		config map[string]string
	}{
		{
			name: "minimal_config",
			config: map[string]string{
				"group_by": "user_id",
			},
		},
		{
			name: "all_defaults",
			config: map[string]string{
				"group_by":      "user_id",
				"window_size":   "1m",
				"window_type":   "tumbling",
				"aggregations":  "count",
				"output_format": "single",
			},
		},
		{
			name: "readme_style_comma_separated",
			config: map[string]string{
				"group_by":     "product_category",
				"window_size":  "5m",
				"aggregations": "count,sum,avg",
				"fields":       "order_total,quantity",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Processor{}
			cfg := config.Config{}
			for k, v := range tt.config {
				cfg[k] = v
			}

			err := p.Configure(context.Background(), cfg)
			require.NoError(t, err)

			// Verify defaults are applied
			if tt.config["aggregations"] == "" {
				assert.Equal(t, []string{"count"}, p.aggregations)
			}
			if tt.config["window_size"] == "" || tt.config["window_size"] == "1m" {
				assert.Equal(t, time.Minute, p.windowDur)
			}
		})
	}
}

// TestIntegration_ErrorRecovery tests error handling and recovery
func TestIntegration_ErrorRecovery(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":     "user_id",
		"window_size":  "1m",
		"aggregations": "count",
	})

	ctx := context.Background()

	// Mix valid and invalid records
	records := []opencdc.Record{
		createTestRecord(t, map[string]interface{}{"user_id": "valid1"}),
		// Invalid JSON
		{
			Payload: opencdc.Change{After: opencdc.RawData("invalid json")},
		},
		createTestRecord(t, map[string]interface{}{"user_id": "valid2"}),
		// Missing group field
		createTestRecord(t, map[string]interface{}{"other_field": "value"}),
		createTestRecord(t, map[string]interface{}{"user_id": "valid3"}),
	}

	results := p.Process(ctx, records)

	// Should have 5 results
	assert.Len(t, results, 5)

	// Check result types
	errorCount := 0
	filterCount := 0
	for _, result := range results {
		switch result.(type) {
		case sdk.ErrorRecord:
			errorCount++
		case sdk.FilterRecord:
			filterCount++
		}
	}

	assert.Equal(t, 2, errorCount)  // 2 invalid records
	assert.Equal(t, 3, filterCount) // 3 valid records

	// Verify valid records were processed
	assert.Len(t, p.windows, 1)
	var window *WindowState
	for _, w := range p.windows {
		window = w
		break
	}
	assert.Equal(t, 3, window.MessageCount) // Only valid records
}

// TestIntegration_PerformanceUnderLoad tests processor under heavy load
func TestIntegration_PerformanceUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	p := setupTestProcessor(t, map[string]string{
		"group_by":     "user_id",
		"window_size":  "1s",
		"aggregations": "count,sum,avg",
		"fields":       "amount",
	})

	ctx := context.Background()

	// Generate many records
	numRecords := 10000
	numUsers := 100
	records := make([]opencdc.Record, numRecords)

	for i := 0; i < numRecords; i++ {
		userID := fmt.Sprintf("user%d", i%numUsers)
		records[i] = createTestRecord(t, map[string]interface{}{
			"user_id": userID,
			"amount":  float64(i % 1000),
		})
	}

	start := time.Now()

	// Process in batches
	batchSize := 1000
	for i := 0; i < numRecords; i += batchSize {
		end := i + batchSize
		if end > numRecords {
			end = numRecords
		}

		batch := records[i:end]
		results := p.Process(ctx, batch)

		// All should be filtered (no errors)
		for _, result := range results {
			assert.IsType(t, sdk.FilterRecord{}, result)
		}
	}

	duration := time.Since(start)

	// Performance assertions
	recordsPerSecond := float64(numRecords) / duration.Seconds()
	t.Logf("Processed %d records in %v (%.0f records/sec)", numRecords, duration, recordsPerSecond)

	// Should process at least 1000 records per second
	assert.Greater(t, recordsPerSecond, 1000.0)

	// Verify final state
	assert.NotEmpty(t, p.windows)
	totalMessages := 0
	for _, window := range p.windows {
		totalMessages += window.MessageCount
	}
	assert.Equal(t, numRecords, totalMessages)
}
