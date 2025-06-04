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

func TestProcessor_Specification(t *testing.T) {
	p := &Processor{}
	spec, err := p.Specification()

	require.NoError(t, err)
	assert.Equal(t, "aggregate", spec.Name)
	assert.Equal(t, "v1.0.0", spec.Version)
	assert.Equal(t, "Devaris Brown", spec.Author)
	assert.NotEmpty(t, spec.Summary)
	assert.NotEmpty(t, spec.Description)
	assert.NotEmpty(t, spec.Parameters)

	// Verify all expected parameters are present
	params := spec.Parameters
	assert.Contains(t, params, "group_by")
	assert.Contains(t, params, "window_size")
	assert.Contains(t, params, "window_type")
	assert.Contains(t, params, "aggregations")
	assert.Contains(t, params, "fields")
	assert.Contains(t, params, "output_format")
}

func TestProcessor_Configure_Basic(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]string
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid basic config",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"window_type":  "tumbling",
				"aggregations": "count",
			},
			wantErr: false,
		},
		{
			name: "missing required group_by",
			config: map[string]string{
				"window_size":  "1m",
				"aggregations": "count",
			},
			wantErr:     true,
			expectedErr: "group_by: required parameter is not provided",
		},
		{
			name: "invalid window_size",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "invalid",
				"aggregations": "count",
			},
			wantErr:     true,
			expectedErr: "invalid window_size: time: invalid duration \"invalid\"",
		},
		{
			name: "invalid aggregation",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "invalid_agg",
			},
			wantErr:     true,
			expectedErr: "unsupported aggregation: invalid_agg",
		},
		{
			name: "invalid sliding window - slide_by >= window_size",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"window_type":  "sliding",
				"slide_by":     "2m", // Greater than window_size
				"aggregations": "count",
			},
			wantErr:     true,
			expectedErr: "slide_by must be less than window_size for sliding windows",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Processor{}
			cfg := make(config.Config)
			for k, v := range tt.config {
				cfg[k] = v
			}

			err := p.Configure(context.Background(), cfg)
			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErr != "" {
					assert.Contains(t, err.Error(), tt.expectedErr, "Error message does not contain expected text")
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessor_Configure_CommaSeparatedParsing(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]string
		wantErr        bool
		expectedAggs   []string
		expectedFields []string
		expectedErr    string
	}{
		{
			name: "comma-separated aggregations and fields",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "count,sum,avg",
				"fields":       "amount,quantity",
			},
			wantErr:        false,
			expectedAggs:   []string{"count", "sum", "avg"},
			expectedFields: []string{"amount", "quantity"},
		},
		{
			name: "comma-separated with spaces",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "count, sum, avg",
				"fields":       "amount, quantity, price",
			},
			wantErr:        false,
			expectedAggs:   []string{"count", "sum", "avg"},
			expectedFields: []string{"amount", "quantity", "price"},
		},
		{
			name: "single aggregation",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "count",
			},
			wantErr:        false,
			expectedAggs:   []string{"count"},
			expectedFields: []string{},
		},
		{
			name: "empty aggregations defaults to count",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "",
			},
			wantErr:      false,
			expectedAggs: []string{"count"},
		},
		{
			name: "numeric aggregation without fields",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "sum",
				"fields":       "", // Empty fields
			},
			wantErr:     true,
			expectedErr: "fields must be specified for aggregations: [sum]",
		},
		{
			name: "mixed aggregations needing fields",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "count,sum,unique_count",
				"fields":       "amount",
			},
			wantErr:        false,
			expectedAggs:   []string{"count", "sum", "unique_count"},
			expectedFields: []string{"amount"},
		},
		{
			name: "all aggregations with fields",
			config: map[string]string{
				"group_by":     "category",
				"window_size":  "1m",
				"aggregations": "count,sum,avg,min,max,unique_count,collect",
				"fields":       "price,quantity",
			},
			wantErr:        false,
			expectedAggs:   []string{"count", "sum", "avg", "min", "max", "unique_count", "collect"},
			expectedFields: []string{"price", "quantity"},
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
			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErr != "" {
					assert.Contains(t, err.Error(), tt.expectedErr)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedAggs, p.aggregations)
				if len(tt.expectedFields) > 0 {
					assert.Equal(t, tt.expectedFields, p.fields)
				}
			}
		})
	}
}

func TestProcessor_Configure_SlidingWindows(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]string
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid sliding window",
			config: map[string]string{
				"group_by":     "traffic_light",
				"window_size":  "1h",
				"window_type":  "sliding",
				"slide_by":     "30m",
				"aggregations": "count,sum",
				"fields":       "passengers",
			},
			wantErr: false,
		},
		{
			name: "sliding window with equal slide_by and window_size",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"window_type":  "sliding",
				"slide_by":     "1m", // Equal to window_size
				"aggregations": "count",
			},
			wantErr:     true, // Should fail
			expectedErr: "slide_by must be less than window_size for sliding windows",
		},
		{
			name: "sliding window with slide_by > window_size",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"window_type":  "sliding",
				"slide_by":     "2m", // Greater than window_size
				"aggregations": "count",
			},
			wantErr:     true, // Should fail
			expectedErr: "slide_by must be less than window_size for sliding windows",
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
			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErr != "" {
					assert.Contains(t, err.Error(), tt.expectedErr, "Error message does not contain expected text")
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, "sliding", p.config.WindowType)
			}
		})
	}
}

func TestProcessor_TumblingWindow_EndToEnd(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":      "traffic_light",
		"window_size":   "1m",
		"window_type":   "tumbling",
		"aggregations":  "count,sum,avg",
		"fields":        "passengers",
		"output_format": "per_group",
	})

	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Minute)

	// Create test records within the same window
	records := []opencdc.Record{
		createTestRecord(t, map[string]interface{}{
			"traffic_light": "light_1",
			"passengers":    3,
			"timestamp":     baseTime.Add(10 * time.Second).Format(time.RFC3339),
		}),
		createTestRecord(t, map[string]interface{}{
			"traffic_light": "light_1",
			"passengers":    5,
			"timestamp":     baseTime.Add(30 * time.Second).Format(time.RFC3339),
		}),
		createTestRecord(t, map[string]interface{}{
			"traffic_light": "light_2",
			"passengers":    2,
			"timestamp":     baseTime.Add(45 * time.Second).Format(time.RFC3339),
		}),
	}

	// Process records
	results := p.Process(ctx, records)

	// All should be filtered initially (waiting for window to complete)
	for _, result := range results {
		assert.IsType(t, sdk.FilterRecord{}, result)
	}

	// Verify window state
	assert.Len(t, p.windows, 1)

	for _, window := range p.windows {
		assert.Equal(t, 3, window.MessageCount)
		assert.Len(t, window.Groups, 2)
		assert.Len(t, window.Groups["light_1"], 2)
		assert.Len(t, window.Groups["light_2"], 1)
	}

	// Verify parsed configuration
	assert.Equal(t, []string{"count", "sum", "avg"}, p.aggregations)
	assert.Equal(t, []string{"passengers"}, p.fields)
}

func TestProcessor_SlidingWindow_EndToEnd(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":     "user_id",
		"window_size":  "2m",
		"window_type":  "sliding",
		"slide_by":     "1m",
		"aggregations": "count",
	})

	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Minute)

	// Create test record
	record := createTestRecord(t, map[string]interface{}{
		"user_id":   "user_1",
		"action":    "click",
		"timestamp": baseTime.Add(30 * time.Second).Format(time.RFC3339),
	})

	// Process record
	results := p.Process(ctx, []opencdc.Record{record})

	// Should be filtered initially
	assert.Len(t, results, 1)
	assert.IsType(t, sdk.FilterRecord{}, results[0])

	// Should create multiple windows (sliding)
	assert.GreaterOrEqual(t, len(p.windows), 1)
}

func TestProcessor_MultipleAggregations_AllTypes(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":      "category",
		"window_size":   "1m",
		"aggregations":  "count,sum,avg,min,max,unique_count,collect",
		"fields":        "price,quantity,user_id",
		"output_format": "per_group",
	})

	ctx := context.Background()

	// Create test records with different values
	records := []opencdc.Record{
		createTestRecord(t, map[string]interface{}{
			"category": "electronics",
			"price":    100.0,
			"quantity": 2,
			"user_id":  "user1",
		}),
		createTestRecord(t, map[string]interface{}{
			"category": "electronics",
			"price":    200.0,
			"quantity": 1,
			"user_id":  "user2",
		}),
		createTestRecord(t, map[string]interface{}{
			"category": "electronics",
			"price":    150.0,
			"quantity": 3,
			"user_id":  "user1", // Duplicate user for unique_count test
		}),
	}

	// Process records
	processor_results := p.Process(ctx, records)

	// Should all be filtered initially
	for _, result := range processor_results {
		assert.IsType(t, sdk.FilterRecord{}, result)
	}

	// Get the window and verify aggregations
	assert.Len(t, p.windows, 1)
	var window *WindowState
	for _, w := range p.windows {
		window = w
		break
	}

	require.NotNil(t, window)
	group := window.Groups["electronics"]
	require.NotNil(t, group)

	// Test aggregation calculation
	aggregations := p.calculateAggregations(group)

	// Verify count
	assert.Equal(t, float64(3), aggregations["count"])
	// Verify sum
	sums := aggregations["sum"].(map[string]float64)
	assert.Equal(t, 450.0, sums["price"])  // 100 + 200 + 150
	assert.Equal(t, 6.0, sums["quantity"]) // 2 + 1 + 3

	// Verify avg
	avgs := aggregations["avg"].(map[string]float64)
	assert.Equal(t, 150.0, avgs["price"])  // 450 / 3
	assert.Equal(t, 2.0, avgs["quantity"]) // 6 / 3

	// Verify min
	mins := aggregations["min"].(map[string]float64)
	assert.Equal(t, 100.0, mins["price"])
	assert.Equal(t, 1.0, mins["quantity"])

	// Verify max
	maxs := aggregations["max"].(map[string]float64)
	assert.Equal(t, 200.0, maxs["price"])
	assert.Equal(t, 3.0, maxs["quantity"])

	// Verify unique_count
	uniqueCounts := aggregations["unique_count"].(map[string]float64)
	assert.Equal(t, float64(3), uniqueCounts["price"])    // All unique
	assert.Equal(t, float64(3), uniqueCounts["quantity"]) // All unique
	assert.Equal(t, float64(2), uniqueCounts["user_id"])  // user1 appears twice

	// Verify collect
	collections := aggregations["collect"].(map[string][]interface{})
	assert.Len(t, collections["price"], 3)
	assert.Len(t, collections["user_id"], 3)
	assert.Contains(t, collections["user_id"], "user1")
	assert.Contains(t, collections["user_id"], "user2")
}

func TestProcessor_NestedFieldAccess(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":        "user.profile.id",
		"window_size":     "1m",
		"aggregations":    "sum,unique_count",
		"fields":          "transaction.amount,product.category",
		"timestamp_field": "event.timestamp",
	})

	ctx := context.Background()

	record := createTestRecord(t, map[string]interface{}{
		"user": map[string]interface{}{
			"profile": map[string]interface{}{
				"id": "user123",
			},
		},
		"transaction": map[string]interface{}{
			"amount": 100.50,
		},
		"product": map[string]interface{}{
			"category": "electronics",
		},
		"event": map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339),
		},
	})

	results := p.Process(ctx, []opencdc.Record{record})

	// Should be filtered initially
	assert.Len(t, results, 1)
	assert.IsType(t, sdk.FilterRecord{}, results[0])

	// Verify window contains the record with correct group key
	assert.Len(t, p.windows, 1)
	for _, window := range p.windows {
		assert.Equal(t, 1, window.MessageCount)
		assert.Contains(t, window.Groups, "user123")
	}
}

func TestProcessor_LateMessages(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":         "user_id",
		"window_size":      "1m",
		"allowed_lateness": "30s",
		"aggregations":     "count",
		"timestamp_field":  "timestamp",
	})

	ctx := context.Background()
	now := time.Now()

	records := []opencdc.Record{
		// Current message
		createTestRecord(t, map[string]interface{}{
			"user_id":   "user1",
			"timestamp": now.Format(time.RFC3339),
		}),
		// Late message within threshold
		createTestRecord(t, map[string]interface{}{
			"user_id":   "user1",
			"timestamp": now.Add(-20 * time.Second).Format(time.RFC3339),
		}),
		// Late message beyond threshold
		createTestRecord(t, map[string]interface{}{
			"user_id":   "user1",
			"timestamp": now.Add(-45 * time.Second).Format(time.RFC3339),
		}),
	}

	results := p.Process(ctx, records)

	// Should have 3 results: all FilterRecord type
	assert.Len(t, results, 3)
	for _, result := range results {
		assert.IsType(t, sdk.FilterRecord{}, result)
	}

	// Should have 2 messages in window (late one dropped)
	totalMessages := 0
	for _, window := range p.windows {
		totalMessages += window.MessageCount
	}
	assert.Equal(t, 2, totalMessages)
}

func TestProcessor_OutputFormats(t *testing.T) {
	tests := []struct {
		name         string
		outputFormat string
		expectSingle bool
	}{
		{
			name:         "single output format",
			outputFormat: "single",
			expectSingle: true,
		},
		{
			name:         "per_group output format",
			outputFormat: "per_group",
			expectSingle: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := setupTestProcessor(t, map[string]string{
				"group_by":      "category",
				"window_size":   "1m",
				"aggregations":  "count,sum",
				"fields":        "amount",
				"output_format": tt.outputFormat,
			})

			// Create a window with test data
			window := &WindowState{
				WindowStart:  time.Now(),
				WindowEnd:    time.Now().Add(time.Minute),
				Groups:       make(map[string][]map[string]interface{}),
				MessageCount: 2,
			}

			// Add test data
			window.Groups["electronics"] = []map[string]interface{}{
				{"amount": 100.0},
			}
			window.Groups["books"] = []map[string]interface{}{
				{"amount": 50.0},
			}

			records := p.aggregateWindow(window)

			if tt.expectSingle {
				// Single format should produce one record
				assert.Len(t, records, 1)

				// Parse the payload to verify structure
				var payload map[string]interface{}
				err := json.Unmarshal(records[0].Payload.After.Bytes(), &payload)
				require.NoError(t, err)

				assert.Contains(t, payload, "groups")
				assert.Contains(t, payload, "total_count")
			} else {
				// Per-group format should produce one record per group
				assert.Len(t, records, 2)

				// Parse both payloads to verify structure
				for _, record := range records {
					var payload map[string]interface{}
					err := json.Unmarshal(record.Payload.After.Bytes(), &payload)
					require.NoError(t, err)

					assert.Contains(t, payload, "group_key")
					assert.Contains(t, payload, "group_value")
					assert.Contains(t, payload, "count")
				}
			}
		})
	}
}

func TestProcessor_ErrorHandling(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":     "user_id",
		"window_size":  "1m",
		"aggregations": "count",
	})

	ctx := context.Background()

	tests := []struct {
		name           string
		record         opencdc.Record
		expectError    bool
		expectFiltered bool
	}{
		{
			name:           "valid record",
			record:         createTestRecord(t, map[string]interface{}{"user_id": "user1"}),
			expectError:    false,
			expectFiltered: true,
		},
		{
			name: "missing group_by field",
			record: createTestRecord(t, map[string]interface{}{
				"other_field": "value",
			}),
			expectError:    true,
			expectFiltered: false,
		},
		{
			name: "invalid JSON payload",
			record: opencdc.Record{
				Payload: opencdc.Change{
					After: opencdc.RawData("invalid json"),
				},
			},
			expectError:    true,
			expectFiltered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := p.Process(ctx, []opencdc.Record{tt.record})
			require.Len(t, results, 1)

			if tt.expectError {
				assert.IsType(t, sdk.ErrorRecord{}, results[0])
			} else if tt.expectFiltered {
				assert.IsType(t, sdk.FilterRecord{}, results[0])
			}
		})
	}
}

func TestProcessor_WindowCompletion(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":      "user_id",
		"window_size":   "100ms", // Very short for testing
		"aggregations":  "count,sum",
		"fields":        "amount",
		"output_format": "per_group",
	})

	ctx := context.Background()

	// Create and process a record
	record := createTestRecord(t, map[string]interface{}{
		"user_id": "user1",
		"amount":  100.0,
	})
	p.Process(ctx, []opencdc.Record{record})

	// Wait for window to complete
	time.Sleep(200 * time.Millisecond)

	// Process window tick manually
	results := p.emitCompletedWindows(ctx)

	// Should produce output now
	require.NotEmpty(t, results)

	// Verify the output record
	foundSingleRecord := false
	for _, result := range results {
		if singleRecord, ok := result.(sdk.SingleRecord); ok {
			foundSingleRecord = true

			// Verify the record structure
			record := opencdc.Record(singleRecord)
			var payload map[string]interface{}
			err := json.Unmarshal(record.Payload.After.Bytes(), &payload)
			require.NoError(t, err)

			// Check required fields
			assert.Equal(t, "user1", payload["group_key"])
			assert.Equal(t, float64(1), payload["count"])

			// Check group_value
			groupValue, ok := payload["group_value"].(map[string]interface{})
			require.True(t, ok)
			assert.Equal(t, float64(1), groupValue["count"])
		}
	}

	assert.True(t, foundSingleRecord)
}

// Benchmark tests
func BenchmarkProcessor_Process(b *testing.B) {
	p := setupTestProcessor(b, map[string]string{
		"group_by":     "user_id",
		"window_size":  "5m",
		"aggregations": "count,sum,avg",
		"fields":       "amount",
	})

	record := createTestRecord(b, map[string]interface{}{
		"user_id": "user1",
		"amount":  100.0,
	})
	records := []opencdc.Record{record}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Process(context.Background(), records)
	}
}

func BenchmarkProcessor_ProcessMultipleGroups(b *testing.B) {
	p := setupTestProcessor(b, map[string]string{
		"group_by":     "user_id",
		"window_size":  "5m",
		"aggregations": "count,sum,avg,min,max",
		"fields":       "amount",
	})

	// Create records for multiple users
	records := make([]opencdc.Record, 100)
	for i := 0; i < 100; i++ {
		userID := fmt.Sprintf("user%d", i%10) // 10 different users
		records[i] = createTestRecord(b, map[string]interface{}{
			"user_id": userID,
			"amount":  float64(i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Process(context.Background(), records)
	}
}

func BenchmarkProcessor_CommaSeparatedParsing(b *testing.B) {
	configMap := map[string]string{
		"group_by":     "user_id",
		"window_size":  "1m",
		"aggregations": "count,sum,avg,min,max,unique_count,collect",
		"fields":       "amount,quantity,price,score,rating",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := &Processor{}
		cfg := make(config.Config)
		for k, v := range configMap {
			cfg[k] = v
		}
		p.Configure(context.Background(), cfg)
	}
}

// Helper functions

func setupTestProcessor(tb testing.TB, configMap map[string]string) *Processor {
	p := &Processor{}

	// Ensure required fields are present with defaults if not provided
	defaultConfig := map[string]string{
		"window_size": "1m",
	}

	// Merge defaults with provided config
	cfg := make(config.Config)
	for k, v := range defaultConfig {
		cfg[k] = v
	}
	for k, v := range configMap {
		cfg[k] = v
	}

	err := p.Configure(context.Background(), cfg)
	require.NoError(tb, err)

	err = p.Open(context.Background())
	require.NoError(tb, err)

	return p
}

func createTestRecord(tb testing.TB, payload map[string]interface{}) opencdc.Record {
	payloadBytes, err := json.Marshal(payload)
	require.NoError(tb, err)

	return opencdc.Record{
		Payload: opencdc.Change{
			After: opencdc.RawData(payloadBytes),
		},
		Metadata: make(opencdc.Metadata),
	}
}
