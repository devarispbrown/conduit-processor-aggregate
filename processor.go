package aggregate

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

//go:generate paramgen -output=paramgen_proc.go ProcessorConfig

// ProcessorConfig holds the processor configuration.
type ProcessorConfig struct {
	// GroupBy is the field path to group messages by (e.g., "user_id" or "user.profile.id")
	GroupBy string `json:"group_by" validate:"required"`

	// WindowSize defines the time window duration (e.g., "1m", "5s", "1h")
	WindowSize string `json:"window_size" default:"1m"`

	// WindowType defines the window type: "tumbling" or "sliding"
	WindowType string `json:"window_type" default:"tumbling" validate:"inclusion[tumbling,sliding]"`

	// SlideBy defines the slide interval for sliding windows (e.g., "30s")
	// Only used when WindowType is "sliding"
	SlideBy string `json:"slide_by" default:"30s"`

	// AllowedLateness allows late messages within this duration
	AllowedLateness string `json:"allowed_lateness" default:"0s"`

	// TimestampField is the field to use for event time (empty = processing time)
	TimestampField string `json:"timestamp_field"`

	// Aggregations defines what aggregations to perform on the grouped data
	// Supported: sum, count, avg, min, max, unique_count, collect
	Aggregations []string `json:"aggregations" default:"count"`

	// Fields defines which fields to aggregate (used with sum, avg, min, max)
	Fields []string `json:"fields"`

	// OutputFormat defines how to output results: "single" or "per_group"
	OutputFormat string `json:"output_format" default:"single" validate:"inclusion[single,per_group]"`
}

// WindowState holds the state for a single window
type WindowState struct {
	WindowStart  time.Time                           `json:"window_start"`
	WindowEnd    time.Time                           `json:"window_end"`
	Groups       map[string][]map[string]interface{} `json:"groups"`
	MessageCount int                                 `json:"message_count"`
	LastActivity time.Time                           `json:"last_activity"`
}

// Processor implements the aggregate processor
type Processor struct {
	sdk.UnimplementedProcessor
	config    ProcessorConfig
	windowDur time.Duration
	slideDur  time.Duration
	lateness  time.Duration
	windows   map[string]*WindowState
}

// Specification returns the processor specification
func (p *Processor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:    "aggregate",
		Summary: "Window-based aggregation processor with grouping capabilities",
		Description: `Aggregates messages within time windows, similar to Redpanda Connect's windowing.
Supports tumbling and sliding windows with configurable grouping and aggregation functions.
Provides count, sum, avg, min, max, unique_count, and collect aggregations.`,
		Version:    "v1.0.0",
		Author:     "Conduit",
		Parameters: ProcessorConfig{}.Parameters(),
	}, nil
}

// Configure configures the processor
func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	var config ProcessorConfig
	err := sdk.ParseConfig(ctx, cfg, &config, ProcessorConfig{}.Parameters())
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Parse durations
	p.windowDur, err = time.ParseDuration(config.WindowSize)
	if err != nil {
		return fmt.Errorf("invalid window_size: %w", err)
	}

	p.slideDur, err = time.ParseDuration(config.SlideBy)
	if err != nil {
		return fmt.Errorf("invalid slide_by: %w", err)
	}

	p.lateness, err = time.ParseDuration(config.AllowedLateness)
	if err != nil {
		return fmt.Errorf("invalid allowed_lateness: %w", err)
	}

	// Validate sliding window configuration
	if config.WindowType == "sliding" && p.slideDur >= p.windowDur {
		return fmt.Errorf("slide_by must be less than window_size for sliding windows")
	}

	// Validate aggregations
	validAggs := map[string]bool{
		"sum": true, "count": true, "avg": true, "min": true,
		"max": true, "unique_count": true, "collect": true,
	}
	for _, agg := range config.Aggregations {
		if !validAggs[agg] {
			return fmt.Errorf("unsupported aggregation: %s", agg)
		}
	}

	// Validate that fields are specified for numeric aggregations
	numericAggs := []string{"sum", "avg", "min", "max"}
	for _, agg := range config.Aggregations {
		for _, numAgg := range numericAggs {
			if agg == numAgg && len(config.Fields) == 0 {
				return fmt.Errorf("fields must be specified for %s aggregation", agg)
			}
		}
	}

	p.config = config
	p.windows = make(map[string]*WindowState)
	return nil
}

// Open initializes the processor
func (p *Processor) Open(ctx context.Context) error {
	// Start background cleanup routine
	go p.cleanupRoutine(ctx)
	return nil
}

// Process processes a batch of records
func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	results := make([]sdk.ProcessedRecord, 0)
	logger := sdk.Logger(ctx)

	for _, record := range records {
		// Extract timestamp
		eventTime := p.extractTimestamp(record)

		// Skip late messages if configured
		if p.lateness > 0 && time.Since(eventTime) > p.lateness {
			logger.Trace().
				Time("event_time", eventTime).
				Dur("lateness", time.Since(eventTime)).
				Msg("Dropping late message")
			results = append(results, sdk.FilterRecord{})
			continue
		}

		// Extract payload
		payload, err := p.extractPayload(record)
		if err != nil {
			results = append(results, sdk.ErrorRecord{Error: err})
			continue
		}

		// Extract group key
		groupKey, err := p.extractGroupKey(payload)
		if err != nil {
			results = append(results, sdk.ErrorRecord{Error: err})
			continue
		}

		// Get or create window(s) for this message
		windowKeys := p.getWindowKeys(eventTime)
		for _, windowKey := range windowKeys {
			window := p.getOrCreateWindow(windowKey, eventTime)

			// Add message to window
			if window.Groups[groupKey] == nil {
				window.Groups[groupKey] = make([]map[string]interface{}, 0)
			}
			window.Groups[groupKey] = append(window.Groups[groupKey], payload)
			window.MessageCount++
			window.LastActivity = time.Now()
		}

		// Always filter individual messages - aggregated results are emitted by cleanup routine
		results = append(results, sdk.FilterRecord{})
	}

	// Check for completed windows and emit results
	completedResults := p.emitCompletedWindows(ctx)
	results = append(results, completedResults...)

	return results
}

// Teardown cleans up resources
func (p *Processor) Teardown(ctx context.Context) error {
	// Emit any remaining windows
	p.emitCompletedWindows(ctx)
	return nil
}

// MiddlewareOptions returns middleware options
func (p *Processor) MiddlewareOptions() []sdk.ProcessorMiddlewareOption {
	return nil
}

// extractTimestamp extracts the timestamp from a record
func (p *Processor) extractTimestamp(record opencdc.Record) time.Time {
	if p.config.TimestampField == "" {
		return time.Now() // Use processing time
	}

	payload, err := p.extractPayload(record)
	if err != nil {
		return time.Now()
	}

	timestampValue := getNestedValue(payload, p.config.TimestampField)
	if timestampValue == nil {
		return time.Now()
	}

	// Try to parse various timestamp formats
	switch v := timestampValue.(type) {
	case string:
		// Try RFC3339 first
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t
		}
		// Try ISO 8601
		if t, err := time.Parse("2006-01-02T15:04:05", v); err == nil {
			return t
		}
		// Try other formats...
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}
	case int64:
		return time.Unix(v, 0)
	case float64:
		return time.Unix(int64(v), 0)
	}

	return time.Now()
}

// extractPayload extracts the payload from a record
func (p *Processor) extractPayload(record opencdc.Record) (map[string]interface{}, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return payload, nil
}

// extractGroupKey extracts the group key from payload
func (p *Processor) extractGroupKey(payload map[string]interface{}) (string, error) {
	value := getNestedValue(payload, p.config.GroupBy)
	if value == nil {
		return "", fmt.Errorf("group_by field '%s' not found", p.config.GroupBy)
	}
	return fmt.Sprintf("%v", value), nil
}

// getNestedValue retrieves a value from nested data using a path
func getNestedValue(data map[string]interface{}, path string) interface{} {
	if !strings.Contains(path, ".") {
		return data[path]
	}

	parts := strings.Split(path, ".")
	current := data

	for i, part := range parts {
		if current == nil {
			return nil
		}

		if i == len(parts)-1 {
			return current[part]
		}

		next, ok := current[part]
		if !ok {
			return nil
		}

		switch v := next.(type) {
		case map[string]interface{}:
			current = v
		default:
			return nil
		}
	}

	return nil
}

// getWindowKeys returns the window keys for a given timestamp
func (p *Processor) getWindowKeys(eventTime time.Time) []string {
	var keys []string

	switch p.config.WindowType {
	case "tumbling":
		// Tumbling window - each event belongs to exactly one window
		windowStart := eventTime.Truncate(p.windowDur)
		keys = append(keys, p.formatWindowKey(windowStart))

	case "sliding":
		// Sliding window - event may belong to multiple windows
		// Calculate how many windows this event should belong to
		windowCount := int(p.windowDur / p.slideDur)

		for i := 0; i < windowCount; i++ {
			windowStart := eventTime.Truncate(p.slideDur).Add(-time.Duration(i) * p.slideDur)
			windowEnd := windowStart.Add(p.windowDur)

			// Check if event falls within this window
			if !eventTime.Before(windowStart) && eventTime.Before(windowEnd) {
				keys = append(keys, p.formatWindowKey(windowStart))
			}
		}
	}

	return keys
}

// formatWindowKey creates a string key for a window
func (p *Processor) formatWindowKey(windowStart time.Time) string {
	return fmt.Sprintf("%s_%d", p.config.WindowType, windowStart.Unix())
}

// getOrCreateWindow gets or creates a window
func (p *Processor) getOrCreateWindow(windowKey string, eventTime time.Time) *WindowState {
	if window, exists := p.windows[windowKey]; exists {
		return window
	}

	// Parse window start from key
	parts := strings.Split(windowKey, "_")
	if len(parts) != 2 {
		// Fallback
		windowStart := eventTime.Truncate(p.windowDur)
		return p.createWindow(windowKey, windowStart)
	}

	unixTime, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		// Fallback
		windowStart := eventTime.Truncate(p.windowDur)
		return p.createWindow(windowKey, windowStart)
	}

	windowStart := time.Unix(unixTime, 0)
	return p.createWindow(windowKey, windowStart)
}

// createWindow creates a new window
func (p *Processor) createWindow(windowKey string, windowStart time.Time) *WindowState {
	window := &WindowState{
		WindowStart:  windowStart,
		WindowEnd:    windowStart.Add(p.windowDur),
		Groups:       make(map[string][]map[string]interface{}),
		MessageCount: 0,
		LastActivity: time.Now(),
	}
	p.windows[windowKey] = window
	return window
}

// emitCompletedWindows checks for completed windows and emits results
func (p *Processor) emitCompletedWindows(ctx context.Context) []sdk.ProcessedRecord {
	results := make([]sdk.ProcessedRecord, 0)
	now := time.Now()

	for windowKey, window := range p.windows {
		// Check if window is complete (end time + allowed lateness has passed)
		if now.After(window.WindowEnd.Add(p.lateness)) {
			// Create aggregated records for this window
			aggregatedRecords := p.aggregateWindow(window)

			for _, record := range aggregatedRecords {
				results = append(results, sdk.SingleRecord(record))
			}

			// Remove completed window
			delete(p.windows, windowKey)
		}
	}

	return results
}

// aggregateWindow performs aggregations on a completed window
func (p *Processor) aggregateWindow(window *WindowState) []opencdc.Record {
	records := make([]opencdc.Record, 0)

	if p.config.OutputFormat == "single" {
		// Single record with all groups
		record := p.createSingleAggregateRecord(window)
		records = append(records, record)
	} else {
		// One record per group
		for groupKey, messages := range window.Groups {
			record := p.createGroupAggregateRecord(window, groupKey, messages)
			records = append(records, record)
		}
	}

	return records
}

// createSingleAggregateRecord creates a single record with all groups aggregated
func (p *Processor) createSingleAggregateRecord(window *WindowState) opencdc.Record {
	result := map[string]interface{}{
		"window_start": window.WindowStart.Format(time.RFC3339),
		"window_end":   window.WindowEnd.Format(time.RFC3339),
		"window_type":  p.config.WindowType,
		"groups":       make(map[string]interface{}),
		"total_count":  window.MessageCount,
	}

	// Aggregate each group
	groups := result["groups"].(map[string]interface{})
	for groupKey, messages := range window.Groups {
		groups[groupKey] = p.calculateAggregations(messages)
	}

	payload, _ := json.Marshal(result)
	return opencdc.Record{
		Payload: opencdc.Change{
			After: opencdc.RawData(payload),
		},
		Metadata: opencdc.Metadata{
			"conduit.aggregate.window.start": window.WindowStart.Format(time.RFC3339),
			"conduit.aggregate.window.end":   window.WindowEnd.Format(time.RFC3339),
			"conduit.aggregate.window.type":  p.config.WindowType,
			"conduit.aggregate.group.count":  fmt.Sprintf("%d", len(window.Groups)),
		},
	}
}

// createGroupAggregateRecord creates a record for a specific group
func (p *Processor) createGroupAggregateRecord(window *WindowState, groupKey string, messages []map[string]interface{}) opencdc.Record {
	aggregations := p.calculateAggregations(messages)

	result := map[string]interface{}{
		"window_start": window.WindowStart.Format(time.RFC3339),
		"window_end":   window.WindowEnd.Format(time.RFC3339),
		"window_type":  p.config.WindowType,
		"group_key":    groupKey,
		"group_value":  aggregations,
		"count":        len(messages),
	}

	payload, _ := json.Marshal(result)
	return opencdc.Record{
		Payload: opencdc.Change{
			After: opencdc.RawData(payload),
		},
		Metadata: opencdc.Metadata{
			"conduit.aggregate.window.start": window.WindowStart.Format(time.RFC3339),
			"conduit.aggregate.window.end":   window.WindowEnd.Format(time.RFC3339),
			"conduit.aggregate.window.type":  p.config.WindowType,
			"conduit.aggregate.group.key":    groupKey,
		},
	}
}

// calculateAggregations performs the configured aggregations on a set of messages
func (p *Processor) calculateAggregations(messages []map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for _, agg := range p.config.Aggregations {
		switch agg {
		case "count":
			result["count"] = len(messages)

		case "sum":
			sums := make(map[string]float64)
			for _, field := range p.config.Fields {
				for _, msg := range messages {
					if value := getNestedValue(msg, field); value != nil {
						if num, err := parseFloat(value); err == nil {
							sums[field] += num
						}
					}
				}
			}
			result["sum"] = sums

		case "avg":
			avgs := make(map[string]float64)
			for _, field := range p.config.Fields {
				var sum float64
				var count int
				for _, msg := range messages {
					if value := getNestedValue(msg, field); value != nil {
						if num, err := parseFloat(value); err == nil {
							sum += num
							count++
						}
					}
				}
				if count > 0 {
					avgs[field] = sum / float64(count)
				}
			}
			result["avg"] = avgs

		case "min":
			mins := make(map[string]float64)
			for _, field := range p.config.Fields {
				var min *float64
				for _, msg := range messages {
					if value := getNestedValue(msg, field); value != nil {
						if num, err := parseFloat(value); err == nil {
							if min == nil || num < *min {
								min = &num
							}
						}
					}
				}
				if min != nil {
					mins[field] = *min
				}
			}
			result["min"] = mins

		case "max":
			maxs := make(map[string]float64)
			for _, field := range p.config.Fields {
				var max *float64
				for _, msg := range messages {
					if value := getNestedValue(msg, field); value != nil {
						if num, err := parseFloat(value); err == nil {
							if max == nil || num > *max {
								max = &num
							}
						}
					}
				}
				if max != nil {
					maxs[field] = *max
				}
			}
			result["max"] = maxs

		case "unique_count":
			uniques := make(map[string]map[interface{}]bool)
			for _, field := range p.config.Fields {
				uniques[field] = make(map[interface{}]bool)
				for _, msg := range messages {
					if value := getNestedValue(msg, field); value != nil {
						uniques[field][value] = true
					}
				}
			}
			counts := make(map[string]int)
			for field, unique := range uniques {
				counts[field] = len(unique)
			}
			result["unique_count"] = counts

		case "collect":
			collections := make(map[string][]interface{})
			for _, field := range p.config.Fields {
				for _, msg := range messages {
					if value := getNestedValue(msg, field); value != nil {
						collections[field] = append(collections[field], value)
					}
				}
			}
			result["collect"] = collections
		}
	}

	return result
}

// parseFloat attempts to parse various numeric types to float64
func parseFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("unsupported type: %T", value)
	}
}

// cleanupRoutine periodically cleans up old windows
func (p *Processor) cleanupRoutine(ctx context.Context) {
	ticker := time.NewTicker(p.windowDur / 4) // Clean up 4 times per window
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for windowKey, window := range p.windows {
				// Remove windows that are well past their end time
				if now.After(window.WindowEnd.Add(p.lateness).Add(p.windowDur)) {
					delete(p.windows, windowKey)
				}
			}
		}
	}
}

// NewProcessor creates a new aggregate processor instance
func NewProcessor() sdk.Processor {
	return &Processor{}
}
