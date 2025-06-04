package aggregate

import (
	"context"
	"encoding/json"
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
	assert.NotEmpty(t, spec.Summary)
	assert.NotEmpty(t, spec.Description)
	assert.NotEmpty(t, spec.Parameters)
}

func TestProcessor_Configure(t *testing.T) {
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
				"aggregations": "count",
			},
			wantErr: false,
		},
		{
			name: "valid sliding window config",
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
			name: "missing group_by",
			config: map[string]string{
				"window_size":  "1m",
				"aggregations": "count",
			},
			wantErr:     true,
			expectedErr: "group_by",
		},
		{
			name: "invalid window_size",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "invalid",
				"aggregations": "count",
			},
			wantErr:     true,
			expectedErr: "invalid window_size",
		},
		{
			name: "invalid aggregation",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "invalid_agg",
			},
			wantErr:     true,
			expectedErr: "unsupported aggregation",
		},
		{
			name: "sum without fields",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"aggregations": "sum",
			},
			wantErr:     true,
			expectedErr: "fields must be specified",
		},
		{
			name: "invalid sliding window",
			config: map[string]string{
				"group_by":     "user_id",
				"window_size":  "1m",
				"window_type":  "sliding",
				"slide_by":     "2m", // Greater than window_size
				"aggregations": "count",
			},
			wantErr:     true,
			expectedErr: "slide_by must be less than window_size",
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
			}
		})
	}
}

func TestProcessor_TumblingWindow(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":     "traffic_light",
		"window_size":  "1m",
		"window_type":  "tumbling",
		"aggregations": "count,sum",
		"fields":       "passengers",
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
}

func TestProcessor_SlidingWindow(t *testing.T) {
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

func TestProcessor_Aggregations(t *testing.T) {
	// Test different aggregation functions
	tests := []struct {
		name          string
		aggregations  string
		fields        string
		records       []map[string]interface{}
		expectedCount int
	}{
		{
			name:         "count aggregation",
			aggregations: "count",
			fields:       "",
			records: []map[string]interface{}{
				{"group": "A", "value": 10},
				{"group": "A", "value": 20},
				{"group": "B", "value": 30},
			},
			expectedCount: 3,
		},
		{
			name:         "sum aggregation",
			aggregations: "sum",
			fields:       "value",
			records: []map[string]interface{}{
				{"group": "A", "value": 10},
				{"group": "A", "value": 20},
			},
			expectedCount: 2,
		},
		{
			name:         "multiple aggregations",
			aggregations: "count,sum,avg",
			fields:       "value",
			records: []map[string]interface{}{
				{"group": "A", "value": 10},
				{"group": "A", "value": 20},
				{"group": "A", "value": 30},
			},
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := map[string]string{
				"group_by":     "group",
				"window_size":  "1m",
				"aggregations": tt.aggregations,
			}
			if tt.fields != "" {
				config["fields"] = tt.fields
			}

			p := setupTestProcessor(t, config)

			// Create test records
			records := make([]opencdc.Record, len(tt.records))
			for i, data := range tt.records {
				records[i] = createTestRecord(t, data)
			}

			// Process records
			results := p.Process(context.Background(), records)

			// Verify all records are filtered initially
			assert.Len(t, results, len(records))
			for _, result := range results {
				assert.IsType(t, sdk.FilterRecord{}, result)
			}

			// Verify window contains expected message count
			totalMessages := 0
			for _, window := range p.windows {
				totalMessages += window.MessageCount
			}
			assert.Equal(t, tt.expectedCount, totalMessages)
		})
	}
}

func TestProcessor_NestedFields(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":        "user.profile.id",
		"window_size":     "1m",
		"aggregations":    "sum",
		"fields":          "transaction.amount",
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
		"event": map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339),
		},
	})

	results := p.Process(ctx, []opencdc.Record{record})

	// Should be filtered initially
	assert.Len(t, results, 1)
	assert.IsType(t, sdk.FilterRecord{}, results[0])

	// Verify window contains the record
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

	// Should have 3 results: 2 filtered (accepted), 1 filtered (late drop)
	assert.Len(t, results, 3)

	// All should be FilterRecord type
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

func TestCalculateAggregations(t *testing.T) {
	p := setupTestProcessor(t, map[string]string{
		"group_by":     "test",
		"window_size":  "1m",
		"aggregations": "count,sum,avg,min,max,unique_count",
		"fields":       "amount,score",
	})

	messages := []map[string]interface{}{
		{"amount": 100.0, "score": 85},
		{"amount": 200.0, "score": 90},
		{"amount": 150.0, "score": 85}, // duplicate score
	}

	result := p.calculateAggregations(messages)

	// Test count
	assert.Equal(t, 3, result["count"])

	// Test sum
	sums := result["sum"].(map[string]float64)
	assert.Equal(t, 450.0, sums["amount"])
	assert.Equal(t, 260.0, sums["score"])

	// Test avg
	avgs := result["avg"].(map[string]float64)
	assert.Equal(t, 150.0, avgs["amount"])
	assert.InDelta(t, 86.67, avgs["score"], 0.01)

	// Test min
	mins := result["min"].(map[string]float64)
	assert.Equal(t, 100.0, mins["amount"])
	assert.Equal(t, 85.0, mins["score"])

	// Test max
	maxs := result["max"].(map[string]float64)
	assert.Equal(t, 200.0, maxs["amount"])
	assert.Equal(t, 90.0, maxs["score"])

	// Test unique_count
	uniqueCounts := result["unique_count"].(map[string]int)
	assert.Equal(t, 3, uniqueCounts["amount"]) // all unique
	assert.Equal(t, 2, uniqueCounts["score"])  // 85 appears twice
}

// Helper functions

func setupTestProcessor(t *testing.T, configMap map[string]string) *Processor {
	p := &Processor{}

	cfg := config.Config{}
	for k, v := range configMap {
		cfg[k] = v
	}

	err := p.Configure(context.Background(), cfg)
	require.NoError(t, err)

	err = p.Open(context.Background())
	require.NoError(t, err)

	return p
}

func createTestRecord(t *testing.T, payload map[string]interface{}) opencdc.Record {
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	return opencdc.Record{
		Payload: opencdc.Change{
			After: opencdc.RawData(payloadBytes),
		},
		Metadata: make(opencdc.Metadata),
	}
}
