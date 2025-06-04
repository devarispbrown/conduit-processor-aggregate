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

// MockStateStore implements sdk.StateStore for testing.
type MockStateStore struct {
	data map[string][]byte
}

// NewMockStateStore creates a new mock state store.
func NewMockStateStore() *MockStateStore {
	return &MockStateStore{
		data: make(map[string][]byte),
	}
}

// Get retrieves a value by key from the mock store.
func (m *MockStateStore) Get(ctx context.Context, key string) ([]byte, error) {
	if data, exists := m.data[key]; exists {
		return data, nil
	}
	return nil, sdk.ErrKeyNotFound
}

// Set stores a value by key in the mock store.
func (m *MockStateStore) Set(ctx context.Context, key string, value []byte) error {
	m.data[key] = value
	return nil
}

// Delete removes a value by key from the mock store.
func (m *MockStateStore) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}

// TestSpecification verifies the processor specification.
func TestSpecification(t *testing.T) {
	spec := Specification()

	assert.Equal(t, "aggregate", spec.Name)
	assert.Equal(t, "v2.1.0", spec.Version)
	assert.NotEmpty(t, spec.Summary)
	assert.NotEmpty(t, spec.Description)

	// Verify all required parameters are present including new ones
	requiredParams := []string{
		"key_field", "value_field", "flush_count", "flush_timeout", "flush_size",
		"aggregation_functions", "aggregation_expressions", "window_type", "window_size",
		"window_offset", "slide_interval", "session_timeout", "allowed_lateness",
		"timestamp_field", "config_reload_enabled",
	}

	for _, param := range requiredParams {
		assert.Contains(t, spec.Parameters, param, "Missing parameter: %s", param)
	}
}

// TestProcessor_Configure tests the enhanced configuration validation.
func TestProcessor_Configure(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]string
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid basic config with new features",
			config: map[string]string{
				"key_field":             "user.id",
				"value_field":           "metrics.amount",
				"flush_count":           "5",
				"aggregation_functions": "sum,avg,distinct",
				"allowed_lateness":      "5s",
				"window_offset":         "30s",
				"config_reload_enabled": "true",
			},
			wantErr: false,
		},
		{
			name: "valid config with expressions",
			config: map[string]string{
				"key_field":               "user_id",
				"value_field":             "amount",
				"flush_count":             "5",
				"aggregation_functions":   "sum,expr",
				"aggregation_expressions": "revenue=sum(price * quantity),avg_score=avg(rating)",
			},
			wantErr: false,
		},
		{
			name: "invalid expression format",
			config: map[string]string{
				"key_field":               "user_id",
				"value_field":             "amount",
				"flush_count":             "5",
				"aggregation_expressions": "invalid_format",
			},
			wantErr:     true,
			expectedErr: "invalid expression format",
		},
		{
			name: "valid nested field config",
			config: map[string]string{
				"key_field":       "user.profile.id",
				"value_field":     "transaction.details.amount",
				"timestamp_field": "event.metadata.timestamp",
				"flush_count":     "10",
			},
			wantErr: false,
		},
		{
			name: "invalid aggregation function",
			config: map[string]string{
				"key_field":             "user_id",
				"value_field":           "amount",
				"flush_count":           "5",
				"aggregation_functions": "sum,invalid_function",
			},
			wantErr:     true,
			expectedErr: "unsupported aggregation function",
		},
		{
			name: "valid watermark config",
			config: map[string]string{
				"key_field":        "user_id",
				"value_field":      "amount",
				"window_size":      "1m",
				"allowed_lateness": "30s",
			},
			wantErr: false,
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

// TestNestedFieldAccess tests the nested field access functionality.
func TestNestedFieldAccess(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]interface{}
		path     string
		expected interface{}
	}{
		{
			name: "simple field access",
			data: map[string]interface{}{
				"user_id": "123",
			},
			path:     "user_id",
			expected: "123",
		},
		{
			name: "nested field access",
			data: map[string]interface{}{
				"user": map[string]interface{}{
					"profile": map[string]interface{}{
						"id": "nested123",
					},
				},
			},
			path:     "user.profile.id",
			expected: "nested123",
		},
		{
			name: "deep nested field access",
			data: map[string]interface{}{
				"event": map[string]interface{}{
					"metadata": map[string]interface{}{
						"context": map[string]interface{}{
							"user": map[string]interface{}{
								"id": "deep123",
							},
						},
					},
				},
			},
			path:     "event.metadata.context.user.id",
			expected: "deep123",
		},
		{
			name: "non-existent field",
			data: map[string]interface{}{
				"user_id": "123",
			},
			path:     "user.profile.id",
			expected: nil,
		},
		{
			name: "non-existent nested field",
			data: map[string]interface{}{
				"user": map[string]interface{}{
					"name": "John",
				},
			},
			path:     "user.profile.id",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getNestedValue(tt.data, tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExpressionEngine tests the expression evaluation functionality.
func TestExpressionEngine(t *testing.T) {
	expressions := []ExpressionDef{
		{Name: "revenue", Expression: "sum(price * quantity)"},
		{Name: "avg_score", Expression: "avg(rating)"},
		{Name: "max_temp", Expression: "max(temperature)"},
		{Name: "total_count", Expression: "count()"},
	}

	engine := NewExpressionEngine(expressions)

	values := []AggregateValue{
		{
			Value: 10.0,
			RawData: map[string]interface{}{
				"price":       5.0,
				"quantity":    2.0,
				"rating":      4.5,
				"temperature": 25.5,
			},
		},
		{
			Value: 20.0,
			RawData: map[string]interface{}{
				"price":       10.0,
				"quantity":    2.0,
				"rating":      3.5,
				"temperature": 27.0,
			},
		},
		{
			Value: 15.0,
			RawData: map[string]interface{}{
				"price":       7.5,
				"quantity":    2.0,
				"rating":      5.0,
				"temperature": 24.0,
			},
		},
	}

	tests := []struct {
		name     string
		expr     string
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "sum expression",
			expr:     "revenue",
			expected: 45.0, // (5*2) + (10*2) + (7.5*2)
			wantErr:  false,
		},
		{
			name:     "average expression",
			expr:     "avg_score",
			expected: 4.333333333333333, // (4.5 + 3.5 + 5.0) / 3
			wantErr:  false,
		},
		{
			name:     "max expression",
			expr:     "max_temp",
			expected: 27.0,
			wantErr:  false,
		},
		{
			name:     "count expression",
			expr:     "total_count",
			expected: 3,
			wantErr:  false,
		},
		{
			name:    "non-existent expression",
			expr:    "unknown",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Evaluate(tt.expr, values)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.InDelta(t, tt.expected, result, 0.000001)
			}
		})
	}
}

// TestWatermarkHandling tests late data handling with watermarks.
func TestWatermarkHandling(t *testing.T) {
	p := setupTestProcessor(t, Config{
		KeyField:        "user_id",
		ValueField:      "amount",
		FlushCount:      5,
		AllowedLateness: 10 * time.Second,
		WindowType:      WindowTypeTumbling,
	})

	now := time.Now()

	tests := []struct {
		name       string
		eventTime  time.Time
		expectDrop bool
	}{
		{
			name:       "current event",
			eventTime:  now,
			expectDrop: false,
		},
		{
			name:       "slightly late event within threshold",
			eventTime:  now.Add(-5 * time.Second),
			expectDrop: false,
		},
		{
			name:       "event at threshold boundary",
			eventTime:  now.Add(-10 * time.Second),
			expectDrop: false,
		},
		{
			name:       "late event beyond threshold",
			eventTime:  now.Add(-15 * time.Second),
			expectDrop: true,
		},
		{
			name:       "very late event",
			eventTime:  now.Add(-1 * time.Minute),
			expectDrop: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withinWatermark := p.isWithinWatermark(tt.eventTime)
			if tt.expectDrop {
				assert.False(t, withinWatermark, "Event should be dropped (outside watermark)")
			} else {
				assert.True(t, withinWatermark, "Event should be accepted (within watermark)")
			}
		})
	}
}

// TestWindowOffset tests window alignment with custom offsets.
func TestWindowOffset(t *testing.T) {
	p := &Processor{}
	p.config = Config{
		WindowType:   WindowTypeTumbling,
		WindowSize:   time.Minute,
		WindowOffset: 30 * time.Second,
	}

	// Test timestamp at 10:00:45
	timestamp := time.Date(2024, 1, 15, 10, 0, 45, 0, time.UTC)

	windowKey := p.getTumblingWindowKey(timestamp)

	// With 30s offset, window should start at 10:00:30 (not 10:00:00)
	expectedWindowStart := time.Date(2024, 1, 15, 10, 0, 30, 0, time.UTC)
	expectedKey := fmt.Sprintf("tumbling_%d", expectedWindowStart.Unix())

	assert.Equal(t, expectedKey, windowKey)
}

// TestDistinctAggregation tests distinct value counting.
func TestDistinctAggregation(t *testing.T) {
	p := setupTestProcessor(t, Config{
		KeyField:             "category",
		ValueField:           "amount",
		FlushCount:           5,
		AggregationFunctions: []AggregationFunction{AggSum, AggCount, AggDistinct},
		WindowType:           WindowTypeTumbling,
	})

	ctx := context.Background()

	// Create records with some duplicate values for distinct counting
	records := []opencdc.Record{
		createTestRecordWithTimestamp(t, "electronics", 100.0, "user1", time.Now()),
		createTestRecordWithTimestamp(t, "electronics", 200.0, "user2", time.Now()),
		createTestRecordWithTimestamp(t, "electronics", 150.0, "user1", time.Now()), // duplicate user
		createTestRecordWithTimestamp(t, "electronics", 300.0, "user3", time.Now()),
		createTestRecordWithTimestamp(t, "electronics", 250.0, "user2", time.Now()), // duplicate user
	}

	results := p.Process(ctx, records)

	// Should get one aggregated result
	var aggregatedResult *AggregationResult
	for _, result := range results {
		if sr, ok := result.(sdk.SingleRecord); ok {
			var ar AggregationResult
			err := json.Unmarshal(sr.Record.Payload.After.Bytes(), &ar)
			require.NoError(t, err)
			aggregatedResult = &ar
			break
		}
	}

	require.NotNil(t, aggregatedResult)
	assert.Equal(t, "electronics", aggregatedResult.Key)
	assert.Equal(t, 5, aggregatedResult.Count)
	assert.Equal(t, 1000.0, aggregatedResult.Results[AggSum]) // 100+200+150+300+250
	assert.Equal(t, 5, aggregatedResult.Results[AggCount])

	// Check distinct count - should be less than total count due to duplicates
	distinctCount, exists := aggregatedResult.Results[AggDistinct]
	assert.True(t, exists)
	assert.Equal(t, 3, distinctCount) // 3 distinct users despite 5 records
}

// TestExpressionAggregation tests custom expression evaluation.
func TestExpressionAggregation(t *testing.T) {
	p := setupTestProcessor(t, Config{
		KeyField:             "product",
		ValueField:           "base_amount",
		FlushCount:           3,
		AggregationFunctions: []AggregationFunction{AggSum, AggExpr},
		AggregationExpressions: []ExpressionDef{
			{Name: "total_revenue", Expression: "sum(price * quantity)"},
			{Name: "avg_rating", Expression: "avg(rating)"},
		},
		WindowType: WindowTypeTumbling,
	})

	ctx := context.Background()

	records := []opencdc.Record{
		createTestRecordWithComplexData(t, map[string]interface{}{
			"product":     "laptop",
			"base_amount": 10.0,
			"price":       1000.0,
			"quantity":    2.0,
			"rating":      4.5,
		}),
		createTestRecordWithComplexData(t, map[string]interface{}{
			"product":     "laptop",
			"base_amount": 20.0,
			"price":       1200.0,
			"quantity":    1.0,
			"rating":      4.0,
		}),
		createTestRecordWithComplexData(t, map[string]interface{}{
			"product":     "laptop",
			"base_amount": 30.0,
			"price":       800.0,
			"quantity":    3.0,
			"rating":      5.0,
		}),
	}

	results := p.Process(ctx, records)

	var aggregatedResult *AggregationResult
	for _, result := range results {
		if sr, ok := result.(sdk.SingleRecord); ok {
			var ar AggregationResult
			err := json.Unmarshal(sr.Record.Payload.After.Bytes(), &ar)
			require.NoError(t, err)
			aggregatedResult = &ar
			break
		}
	}

	require.NotNil(t, aggregatedResult)
	assert.Equal(t, "laptop", aggregatedResult.Key)
	assert.Equal(t, 60.0, aggregatedResult.Results[AggSum]) // 10+20+30

	// Check expression results
	exprResults, exists := aggregatedResult.Results[AggExpr]
	assert.True(t, exists)

	exprMap, ok := exprResults.(map[string]interface{})
	require.True(t, ok)

	// total_revenue = (1000*2) + (1200*1) + (800*3) = 2000 + 1200 + 2400 = 5600
	assert.Equal(t, 5600.0, exprMap["total_revenue"])

	// avg_rating = (4.5 + 4.0 + 5.0) / 3 = 4.5
	assert.InDelta(t, 4.5, exprMap["avg_rating"], 0.001)
}

// TestHotConfigurationReload tests runtime configuration updates.
func TestHotConfigurationReload(t *testing.T) {
	p := &Processor{}

	// Initial configuration
	initialConfig := config.Config{
		"key_field":             "user_id",
		"value_field":           "amount",
		"flush_count":           "5",
		"config_reload_enabled": "true",
		"aggregation_functions": "sum,count",
	}

	err := p.Configure(context.Background(), initialConfig)
	require.NoError(t, err)

	// Verify initial config
	assert.Equal(t, "user_id", p.config.KeyField)
	assert.Equal(t, 5, p.config.FlushCount)
	assert.Len(t, p.config.AggregationFunctions, 2)

	// Updated configuration
	updatedConfig := config.Config{
		"key_field":             "customer_id",
		"value_field":           "total",
		"flush_count":           "10",
		"config_reload_enabled": "true",
		"aggregation_functions": "sum,avg,min,max",
	}

	// Test hot reload
	err = p.ReloadConfig(context.Background(), updatedConfig)
	require.NoError(t, err)

	// Verify updated config
	assert.Equal(t, "customer_id", p.config.KeyField)
	assert.Equal(t, "total", p.config.ValueField)
	assert.Equal(t, 10, p.config.FlushCount)
	assert.Len(t, p.config.AggregationFunctions, 4)

	// Test reload when disabled
	p.config.ConfigReloadEnabled = false
	err = p.ReloadConfig(context.Background(), updatedConfig)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hot configuration reload is disabled")
}

// TestNestedFieldProcessing tests end-to-end processing with nested fields.
func TestNestedFieldProcessing(t *testing.T) {
	p := setupTestProcessor(t, Config{
		KeyField:             "user.profile.id",
		ValueField:           "transaction.amount",
		TimestampField:       "event.timestamp",
		FlushCount:           2,
		AggregationFunctions: []AggregationFunction{AggSum, AggCount},
		WindowType:           WindowTypeTumbling,
	})

	ctx := context.Background()

	records := []opencdc.Record{
		createTestRecordWithComplexData(t, map[string]interface{}{
			"user": map[string]interface{}{
				"profile": map[string]interface{}{
					"id": "user123",
				},
			},
			"transaction": map[string]interface{}{
				"amount": 100.0,
			},
			"event": map[string]interface{}{
				"timestamp": "2024-01-15T10:00:00Z",
			},
		}),
		createTestRecordWithComplexData(t, map[string]interface{}{
			"user": map[string]interface{}{
				"profile": map[string]interface{}{
					"id": "user123",
				},
			},
			"transaction": map[string]interface{}{
				"amount": 200.0,
			},
			"event": map[string]interface{}{
				"timestamp": "2024-01-15T10:00:30Z",
			},
		}),
	}

	results := p.Process(ctx, records)

	// Should get one aggregated result after 2 records
	var aggregatedResult *AggregationResult
	for _, result := range results {
		if sr, ok := result.(sdk.SingleRecord); ok {
			var ar AggregationResult
			err := json.Unmarshal(sr.Record.Payload.After.Bytes(), &ar)
			require.NoError(t, err)
			aggregatedResult = &ar
			break
		}
	}

	require.NotNil(t, aggregatedResult)
	assert.Equal(t, "user123", aggregatedResult.Key)
	assert.Equal(t, 2, aggregatedResult.Count)
	assert.Equal(t, 300.0, aggregatedResult.Results[AggSum])
}

// TestLateDataHandlingInProcessing tests late data handling during processing.
func TestLateDataHandlingInProcessing(t *testing.T) {
	p := setupTestProcessor(t, Config{
		KeyField:        "user_id",
		ValueField:      "amount",
		FlushCount:      5,
		AllowedLateness: 5 * time.Second,
		TimestampField:  "timestamp",
		WindowType:      WindowTypeTumbling,
	})

	ctx := context.Background()
	now := time.Now()

	records := []opencdc.Record{
		// Current time record
		createTestRecordWithTimestamp(t, "user1", 100.0, now.Format(time.RFC3339), now),
		// Late record within threshold
		createTestRecordWithTimestamp(t, "user1", 200.0, now.Add(-3*time.Second).Format(time.RFC3339), now.Add(-3*time.Second)),
		// Late record beyond threshold (should be dropped)
		createTestRecordWithTimestamp(t, "user1", 300.0, now.Add(-10*time.Second).Format(time.RFC3339), now.Add(-10*time.Second)),
	}

	results := p.Process(ctx, records)

	// Should get 2 filter records (accepted) and 1 filter record (dropped)
	assert.Len(t, results, 3)

	filterCount := 0
	for _, result := range results {
		if _, ok := result.(sdk.FilterRecord); ok {
			filterCount++
		}
	}
	assert.Equal(t, 3, filterCount) // All filtered (none flushed yet)

	// Verify state contains only 2 values (late one was dropped)
	state, err := p.getState(ctx, "user1")
	require.NoError(t, err)

	totalValues := 0
	for _, window := range state.Windows {
		totalValues += len(window.Values)
	}
	assert.Equal(t, 2, totalValues) // Only 2 values should be stored
}

// TestComplexWindowingWithAllFeatures tests all features working together.
func TestComplexWindowingWithAllFeatures(t *testing.T) {
	p := setupTestProcessor(t, Config{
		KeyField:        "category",
		ValueField:      "base_value",
		TimestampField:  "timestamp",
		WindowType:      WindowTypeTumbling,
		WindowSize:      time.Minute,
		WindowOffset:    30 * time.Second,
		AllowedLateness: 10 * time.Second,
		FlushCount:      3,
		AggregationFunctions: []AggregationFunction{
			AggSum, AggCount, AggDistinct, AggExpr,
		},
		AggregationExpressions: []ExpressionDef{
			{Name: "weighted_sum", Expression: "sum(value * weight)"},
		},
	})

	ctx := context.Background()
	baseTime := time.Date(2024, 1, 15, 10, 0, 45, 0, time.UTC) // Window starts at 10:00:30

	records := []opencdc.Record{
		createTestRecordWithComplexTimestamp(t, map[string]interface{}{
			"category":   "electronics",
			"base_value": 100.0,
			"value":      50.0,
			"weight":     2.0,
			"user_id":    "user1",
			"timestamp":  baseTime.Format(time.RFC3339),
		}),
		createTestRecordWithComplexTimestamp(t, map[string]interface{}{
			"category":   "electronics",
			"base_value": 200.0,
			"value":      100.0,
			"weight":     1.5,
			"user_id":    "user2",
			"timestamp":  baseTime.Add(10 * time.Second).Format(time.RFC3339),
		}),
		createTestRecordWithComplexTimestamp(t, map[string]interface{}{
			"category":   "electronics",
			"base_value": 150.0,
			"value":      75.0,
			"weight":     3.0,
			"user_id":    "user1", // duplicate for distinct test
			"timestamp":  baseTime.Add(20 * time.Second).Format(time.RFC3339),
		}),
	}

	results := p.Process(ctx, records)

	// Should get one aggregated result
	var aggregatedResult *AggregationResult
	for _, result := range results {
		if sr, ok := result.(sdk.SingleRecord); ok {
			var ar AggregationResult
			err := json.Unmarshal(sr.Record.Payload.After.Bytes(), &ar)
			require.NoError(t, err)
			aggregatedResult = &ar
			break
		}
	}

	require.NotNil(t, aggregatedResult)

	// Verify basic aggregations
	assert.Equal(t, "electronics", aggregatedResult.Key)
	assert.Equal(t, 3, aggregatedResult.Count)
	assert.Equal(t, 450.0, aggregatedResult.Results[AggSum])  // 100+200+150
	assert.Equal(t, 2, aggregatedResult.Results[AggDistinct]) // 2 distinct users

	// Verify expression result
	exprResults := aggregatedResult.Results[AggExpr].(map[string]interface{})
	// weighted_sum = (50*2) + (100*1.5) + (75*3) = 100 + 150 + 225 = 475
	assert.Equal(t, 475.0, exprResults["weighted_sum"])

	// Verify window alignment (should start at 10:00:30 due to offset)
	expectedStart := time.Date(2024, 1, 15, 10, 0, 30, 0, time.UTC)
	assert.True(t, aggregatedResult.WindowStart.Equal(expectedStart))
}

// TestMetadataEnhancements tests enhanced metadata in output records.
func TestMetadataEnhancements(t *testing.T) {
	p := setupTestProcessor(t, Config{
		KeyField:             "user_id",
		ValueField:           "amount",
		FlushCount:           1,
		AggregationFunctions: []AggregationFunction{AggSum, AggDistinct},
		WindowType:           WindowTypeTumbling,
		WindowSize:           time.Minute,
		AllowedLateness:      5 * time.Second,
	})

	ctx := context.Background()
	record := createTestRecord(t, "user1", 50.0)

	results := p.Process(ctx, []opencdc.Record{record})
	require.Len(t, results, 1)

	result, ok := results[0].(sdk.SingleRecord)
	require.True(t, ok)

	metadata := result.Record.Metadata

	// Verify enhanced metadata
	assert.Equal(t, "user1", metadata["conduit.aggregate.key"])
	assert.Equal(t, "1", metadata["conduit.aggregate.count"])
	assert.Equal(t, "tumbling", metadata["conduit.aggregate.window.type"])
	assert.Equal(t, "sum,distinct", metadata["conduit.aggregate.functions"])
	assert.NotEmpty(t, metadata["conduit.aggregate.window.start"])
	assert.NotEmpty(t, metadata["conduit.aggregate.window.end"])
	assert.NotEmpty(t, metadata["conduit.aggregate.watermark"])
	assert.Equal(t, "1", metadata["conduit.aggregate.distinct.count"])
}

// Benchmark tests for performance validation

// BenchmarkNestedFieldAccess benchmarks nested field access performance.
func BenchmarkNestedFieldAccess(b *testing.B) {
	data := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3": map[string]interface{}{
					"value": "target",
				},
			},
		},
	}

	path := "level1.level2.level3.value"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getNestedValue(data, path)
	}
}

// BenchmarkExpressionEvaluation benchmarks expression evaluation performance.
func BenchmarkExpressionEvaluation(b *testing.B) {
	expressions := []ExpressionDef{
		{Name: "revenue", Expression: "sum(price * quantity)"},
	}
	engine := NewExpressionEngine(expressions)

	values := make([]AggregateValue, 100)
	for i := range values {
		values[i] = AggregateValue{
			RawData: map[string]interface{}{
				"price":    float64(i + 1),
				"quantity": 2.0,
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Evaluate("revenue", values)
	}
}

// BenchmarkDistinctTracking benchmarks distinct value tracking performance.
func BenchmarkDistinctTracking(b *testing.B) {
	p := &Processor{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := map[string]interface{}{
			"user_id": fmt.Sprintf("user_%d", i%1000), // 1000 distinct users
			"value":   float64(i),
		}
		p.generateDistinctID(data)
	}
}

// Helper functions

func setupTestProcessor(t testing.TB, cfg Config) *Processor {
	p := &Processor{}
	p.config = cfg
	p.store = NewMockStateStore()
	p.flushTimers = make(map[string]*time.Timer)
	p.stopChan = make(chan struct{})
	p.expressionEngine = NewExpressionEngine(cfg.AggregationExpressions)
	return p
}

func createTestRecord(t testing.TB, key string, value float64) opencdc.Record {
	payload := map[string]interface{}{
		"user_id": key,
		"amount":  value,
	}
	return createTestRecordWithPayload(t, payload)
}

func createTestRecordWithTimestamp(t testing.TB, key string, value float64, timestamp interface{}, eventTime time.Time) opencdc.Record {
	payload := map[string]interface{}{
		"user_id":   key,
		"amount":    value,
		"timestamp": timestamp,
		"user_info": key, // For distinct counting
	}
	return createTestRecordWithPayload(t, payload)
}

func createTestRecordWithComplexData(t testing.TB, payload map[string]interface{}) opencdc.Record {
	return createTestRecordWithPayload(t, payload)
}

func createTestRecordWithComplexTimestamp(t testing.TB, payload map[string]interface{}) opencdc.Record {
	return createTestRecordWithPayload(t, payload)
}

func createTestRecordWithPayload(t testing.TB, payload map[string]interface{}) opencdc.Record {
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	return opencdc.Record{
		Payload: opencdc.Change{
			After: opencdc.RawData(payloadBytes),
		},
		Metadata: make(opencdc.Metadata),
	}
}
