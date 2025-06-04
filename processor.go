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
	"github.com/rs/zerolog"
)

type Processor struct {
	sdk.UnimplementedProcessor
	config       ProcessorConfig
	aggregations []string
	fields       []string
	windows      map[string]*WindowState
	logger       *zerolog.Logger
}

type ProcessorConfig struct {
	GroupBy         string        `json:"group_by"`
	WindowSize      time.Duration `json:"window_size"`
	WindowType      string        `json:"window_type"`
	SlideBy         time.Duration `json:"slide_by"`
	TimestampField  string        `json:"timestamp_field"`
	AllowedLateness time.Duration `json:"allowed_lateness"`
	Aggregations    string        `json:"aggregations"`
	Fields          string        `json:"fields"`
	OutputFormat    string        `json:"output_format"`
}

type WindowState struct {
	WindowStart  time.Time
	WindowEnd    time.Time
	Groups       map[string][]map[string]interface{}
	MessageCount int
}

// NewProcessor creates a new instance of the aggregate processor
func NewProcessor() *Processor {
	return &Processor{}
}

func (p *Processor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:        "aggregate",
		Version:     "v1.0.0",
		Author:      "Devaris Brown",
		Summary:     "Windowed aggregation processor for streaming data",
		Description: "Groups streaming messages by time windows and field values, then applies aggregation functions like count, sum, average, min, max, and more.",
		Parameters: config.Parameters{
			"group_by": {
				Default:     "",
				Type:        config.ParameterTypeString,
				Description: "Field to group messages by (supports nested fields like user.profile.id)",
				Validations: []config.Validation{
					config.ValidationRequired{},
				},
			},
			"window_size": {
				Default:     "1m",
				Type:        config.ParameterTypeDuration,
				Description: "Window duration (e.g., 1m, 30s, 1h)",
			},
			"window_type": {
				Default:     "tumbling",
				Type:        config.ParameterTypeString,
				Description: "Window type: tumbling or sliding",
				Validations: []config.Validation{
					config.ValidationInclusion{List: []string{"tumbling", "sliding"}},
				},
			},
			"slide_by": {
				Default:     "30s",
				Type:        config.ParameterTypeDuration,
				Description: "Slide interval for sliding windows",
			},
			"timestamp_field": {
				Default:     "",
				Type:        config.ParameterTypeString,
				Description: "Field containing event timestamp (uses processing time if empty)",
			},
			"allowed_lateness": {
				Default:     "0s",
				Type:        config.ParameterTypeDuration,
				Description: "Maximum lateness for messages",
			},
			"aggregations": {
				Default:     "count",
				Type:        config.ParameterTypeString,
				Description: "Comma-separated list of aggregation functions: count,sum,avg,min,max,unique_count,collect",
			},
			"fields": {
				Default:     "",
				Type:        config.ParameterTypeString,
				Description: "Comma-separated list of fields to aggregate (required for numeric functions)",
			},
			"output_format": {
				Default:     "single",
				Type:        config.ParameterTypeString,
				Description: "Output format: single (all groups in one record) or per_group (separate record per group)",
				Validations: []config.Validation{
					config.ValidationInclusion{List: []string{"single", "per_group"}},
				},
			},
		},
	}, nil
}

func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	logger := sdk.Logger(ctx)
	p.logger = logger
	p.logger.Debug().Msg("sanitizing configuration and applying defaults")

	// Apply defaults manually since we're not using SDK parsing
	if cfg["window_size"] == "" {
		cfg["window_size"] = "1m"
	}
	if cfg["window_type"] == "" {
		cfg["window_type"] = "tumbling"
	}
	if cfg["slide_by"] == "" {
		cfg["slide_by"] = "30s"
	}
	if cfg["allowed_lateness"] == "" {
		cfg["allowed_lateness"] = "0s"
	}
	if cfg["aggregations"] == "" {
		cfg["aggregations"] = "count"
	}
	if cfg["output_format"] == "" {
		cfg["output_format"] = "single"
	}

	p.logger.Debug().Msg("validating configuration according to the specifications")

	// Validate required fields
	if cfg["group_by"] == "" {
		return fmt.Errorf("group_by: required parameter is not provided")
	}

	// Parse window_size
	windowSize, err := time.ParseDuration(cfg["window_size"])
	if err != nil {
		return fmt.Errorf("invalid window_size: %w", err)
	}

	// Parse slide_by
	slideBy, err := time.ParseDuration(cfg["slide_by"])
	if err != nil {
		return fmt.Errorf("invalid slide_by: %w", err)
	}

	// Parse allowed_lateness
	allowedLateness, err := time.ParseDuration(cfg["allowed_lateness"])
	if err != nil {
		return fmt.Errorf("invalid allowed_lateness: %w", err)
	}

	// Validate window_type
	windowType := cfg["window_type"]
	if windowType != "tumbling" && windowType != "sliding" {
		return fmt.Errorf("invalid window_type: must be 'tumbling' or 'sliding'")
	}

	// Validate sliding window configuration
	if windowType == "sliding" && slideBy >= windowSize {
		return fmt.Errorf("slide_by must be less than window_size for sliding windows")
	}

	// Validate output_format
	outputFormat := cfg["output_format"]
	if outputFormat != "single" && outputFormat != "per_group" {
		return fmt.Errorf("invalid output_format: must be 'single' or 'per_group'")
	}

	// Set configuration
	p.config = ProcessorConfig{
		GroupBy:         cfg["group_by"],
		WindowSize:      windowSize,
		WindowType:      windowType,
		SlideBy:         slideBy,
		TimestampField:  cfg["timestamp_field"],
		AllowedLateness: allowedLateness,
		Aggregations:    cfg["aggregations"],
		Fields:          cfg["fields"],
		OutputFormat:    outputFormat,
	}

	// Parse comma-separated aggregations
	p.aggregations = parseCommaSeparated(p.config.Aggregations)
	if len(p.aggregations) == 0 {
		p.aggregations = []string{"count"}
	}

	// Parse comma-separated fields
	p.fields = parseCommaSeparated(p.config.Fields)

	// Validate aggregations
	validAggregations := map[string]bool{
		"count": true, "sum": true, "avg": true, "min": true, "max": true,
		"unique_count": true, "collect": true,
	}

	numericAggregations := map[string]bool{
		"sum": true, "avg": true, "min": true, "max": true, "unique_count": true, "collect": true,
	}

	var needsFields []string
	for _, agg := range p.aggregations {
		if !validAggregations[agg] {
			return fmt.Errorf("unsupported aggregation: %s", agg)
		}
		if numericAggregations[agg] {
			needsFields = append(needsFields, agg)
		}
	}

	// Check if fields are required but missing
	if len(needsFields) > 0 && len(p.fields) == 0 {
		return fmt.Errorf("fields must be specified for aggregations: %v", needsFields)
	}

	p.logger.Debug().Interface("target", &p.config).Msg("decoding configuration into the target object")

	p.logger.Info().
		Strs("aggregations", p.aggregations).
		Strs("fields", p.fields).
		Str("group_by", p.config.GroupBy).
		Str("output_format", p.config.OutputFormat).
		Dur("window_size", p.config.WindowSize).
		Str("window_type", p.config.WindowType).
		Msg("Processor configured successfully")

	return nil
}

func (p *Processor) Open(ctx context.Context) error {
	p.windows = make(map[string]*WindowState)
	return nil
}

func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	results := make([]sdk.ProcessedRecord, 0, len(records))
	now := time.Now()

	for _, record := range records {
		// Extract timestamp
		timestamp, err := p.extractTimestamp(record)
		if err != nil {
			p.logger.Debug().Err(err).Msg("failed to extract timestamp, using current time")
			timestamp = now
		}

		// Check if message is late
		if p.isLateMessage(timestamp) {
			lateness := now.Sub(timestamp).Seconds()
			p.logger.Trace().
				Time("event_time", timestamp).
				Float64("lateness", lateness).
				Msg("Dropping late message")

			// Return FilterRecord for late messages
			results = append(results, sdk.FilterRecord{})
			continue
		}

		// Extract group key
		groupKey, err := p.extractGroupKey(record)
		if err != nil {
			p.logger.Debug().Err(err).Msg("failed to extract group key")
			// Return ErrorRecord for invalid records
			results = append(results, sdk.ErrorRecord{Error: err})
			continue
		}

		// Extract payload for aggregation
		payload, err := p.extractPayload(record)
		if err != nil {
			p.logger.Debug().Err(err).Msg("failed to extract payload")
			// Return ErrorRecord for invalid JSON
			results = append(results, sdk.ErrorRecord{Error: err})
			continue
		}

		// Add to appropriate windows
		p.addToWindows(record, payload, groupKey, timestamp)

		// Always return FilterRecord for individual records
		// Aggregated output happens only when windows complete
		results = append(results, sdk.FilterRecord{})
	}

	return results
}

func (p *Processor) Teardown(ctx context.Context) error {
	return nil
}

func (p *Processor) MiddlewareOptions() []sdk.ProcessorMiddlewareOption {
	return nil
}

// Helper method to emit completed windows (called by tests)
func (p *Processor) emitCompletedWindows(ctx context.Context) []sdk.ProcessedRecord {
	var results []sdk.ProcessedRecord
	now := time.Now()

	for windowKey, window := range p.windows {
		if now.After(window.WindowEnd) {
			// Window completed, generate output
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

func (p *Processor) extractTimestamp(record opencdc.Record) (time.Time, error) {
	if p.config.TimestampField == "" {
		return time.Now(), nil
	}

	payload, err := p.extractPayload(record)
	if err != nil {
		return time.Time{}, err
	}

	timestampValue, err := p.getNestedField(payload, p.config.TimestampField)
	if err != nil {
		return time.Time{}, err
	}

	timestampStr, ok := timestampValue.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("timestamp field is not a string")
	}

	return time.Parse(time.RFC3339, timestampStr)
}

func (p *Processor) isLateMessage(eventTime time.Time) bool {
	if p.config.AllowedLateness == 0 {
		return false
	}

	now := time.Now()
	lateness := now.Sub(eventTime)
	return lateness > p.config.AllowedLateness
}

func (p *Processor) extractGroupKey(record opencdc.Record) (string, error) {
	payload, err := p.extractPayload(record)
	if err != nil {
		return "", err
	}

	groupValue, err := p.getNestedField(payload, p.config.GroupBy)
	if err != nil {
		return "", fmt.Errorf("group_by field '%s' not found", p.config.GroupBy)
	}

	return fmt.Sprintf("%v", groupValue), nil
}

func (p *Processor) extractPayload(record opencdc.Record) (map[string]interface{}, error) {
	var payload map[string]interface{}

	if record.Payload.After != nil {
		err := json.Unmarshal(record.Payload.After.Bytes(), &payload)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
		}
	} else {
		return nil, fmt.Errorf("no payload found")
	}

	return payload, nil
}

func (p *Processor) getNestedField(data map[string]interface{}, fieldPath string) (interface{}, error) {
	fields := strings.Split(fieldPath, ".")
	current := data

	for i, field := range fields {
		if i == len(fields)-1 {
			// Last field, return the value
			value, exists := current[field]
			if !exists {
				return nil, fmt.Errorf("field '%s' not found", field)
			}
			return value, nil
		} else {
			// Intermediate field, must be a map
			value, exists := current[field]
			if !exists {
				return nil, fmt.Errorf("field '%s' not found", field)
			}

			if nestedMap, ok := value.(map[string]interface{}); ok {
				current = nestedMap
			} else {
				return nil, fmt.Errorf("field '%s' is not an object", field)
			}
		}
	}

	return nil, fmt.Errorf("field path '%s' is empty", fieldPath)
}

func (p *Processor) addToWindows(record opencdc.Record, payload map[string]interface{}, groupKey string, timestamp time.Time) {
	if p.config.WindowType == "tumbling" {
		p.addToTumblingWindow(payload, groupKey, timestamp)
	} else {
		p.addToSlidingWindows(payload, groupKey, timestamp)
	}
}

func (p *Processor) addToTumblingWindow(payload map[string]interface{}, groupKey string, timestamp time.Time) {
	// Calculate window start time
	windowStart := timestamp.Truncate(p.config.WindowSize)
	windowEnd := windowStart.Add(p.config.WindowSize)

	windowKey := fmt.Sprintf("tumbling_%d", windowStart.Unix())

	window, exists := p.windows[windowKey]
	if !exists {
		window = &WindowState{
			WindowStart:  windowStart,
			WindowEnd:    windowEnd,
			Groups:       make(map[string][]map[string]interface{}),
			MessageCount: 0,
		}
		p.windows[windowKey] = window
	}

	// Add to group
	if window.Groups[groupKey] == nil {
		window.Groups[groupKey] = make([]map[string]interface{}, 0)
	}
	window.Groups[groupKey] = append(window.Groups[groupKey], payload)
	window.MessageCount++
}

func (p *Processor) addToSlidingWindows(payload map[string]interface{}, groupKey string, timestamp time.Time) {
	// For sliding windows, create multiple overlapping windows
	windowStart := timestamp.Truncate(p.config.SlideBy)

	// Create windows that this record should belong to
	for i := 0; i < int(p.config.WindowSize/p.config.SlideBy); i++ {
		currentWindowStart := windowStart.Add(time.Duration(-i) * p.config.SlideBy)
		windowEnd := currentWindowStart.Add(p.config.WindowSize)

		// Only create window if timestamp falls within it
		if timestamp.After(currentWindowStart) && !timestamp.After(windowEnd) {
			windowKey := fmt.Sprintf("sliding_%d", currentWindowStart.Unix())

			window, exists := p.windows[windowKey]
			if !exists {
				window = &WindowState{
					WindowStart:  currentWindowStart,
					WindowEnd:    windowEnd,
					Groups:       make(map[string][]map[string]interface{}),
					MessageCount: 0,
				}
				p.windows[windowKey] = window
			}

			// Add to group
			if window.Groups[groupKey] == nil {
				window.Groups[groupKey] = make([]map[string]interface{}, 0)
			}
			window.Groups[groupKey] = append(window.Groups[groupKey], payload)
			window.MessageCount++
		}
	}
}

func (p *Processor) aggregateWindow(window *WindowState) []opencdc.Record {
	if p.config.OutputFormat == "per_group" {
		return p.generatePerGroupOutput(window)
	} else {
		return p.generateSingleOutput(window)
	}
}

func (p *Processor) generatePerGroupOutput(window *WindowState) []opencdc.Record {
	var records []opencdc.Record

	for groupKey, groupData := range window.Groups {
		aggregations := p.calculateAggregations(groupData)

		payload := map[string]interface{}{
			"window_start": window.WindowStart.Format(time.RFC3339),
			"window_end":   window.WindowEnd.Format(time.RFC3339),
			"window_type":  p.config.WindowType,
			"group_key":    groupKey,
			"group_value":  aggregations,
			"count":        float64(len(groupData)), // Use float64 for JSON consistency
		}

		payloadBytes, _ := json.Marshal(payload)

		record := opencdc.Record{
			Operation: opencdc.OperationCreate,
			Payload: opencdc.Change{
				After: opencdc.RawData(payloadBytes),
			},
			Metadata: opencdc.Metadata{
				"conduit.aggregate.group.key":    groupKey,
				"conduit.aggregate.window.start": window.WindowStart.Format(time.RFC3339),
				"conduit.aggregate.window.end":   window.WindowEnd.Format(time.RFC3339),
				"conduit.aggregate.window.type":  p.config.WindowType,
			},
		}

		records = append(records, record)
	}

	return records
}

func (p *Processor) generateSingleOutput(window *WindowState) []opencdc.Record {
	allGroups := make(map[string]interface{})
	totalCount := 0

	for groupKey, groupData := range window.Groups {
		allGroups[groupKey] = p.calculateAggregations(groupData)
		totalCount += len(groupData)
	}

	payload := map[string]interface{}{
		"window_start": window.WindowStart.Format(time.RFC3339),
		"window_end":   window.WindowEnd.Format(time.RFC3339),
		"window_type":  p.config.WindowType,
		"groups":       allGroups,
		"total_count":  float64(totalCount), // Use float64 for JSON consistency
	}

	payloadBytes, _ := json.Marshal(payload)

	record := opencdc.Record{
		Operation: opencdc.OperationCreate,
		Payload: opencdc.Change{
			After: opencdc.RawData(payloadBytes),
		},
		Metadata: opencdc.Metadata{
			"conduit.aggregate.window.start": window.WindowStart.Format(time.RFC3339),
			"conduit.aggregate.window.end":   window.WindowEnd.Format(time.RFC3339),
			"conduit.aggregate.window.type":  p.config.WindowType,
		},
	}

	return []opencdc.Record{record}
}

func (p *Processor) calculateAggregations(groupData []map[string]interface{}) map[string]interface{} {
	results := make(map[string]interface{})

	for _, agg := range p.aggregations {
		switch agg {
		case "count":
			// Return count as float64 to be consistent with JSON number handling
			results["count"] = float64(len(groupData))
		case "sum":
			results["sum"] = p.calculateSum(groupData)
		case "avg":
			results["avg"] = p.calculateAvg(groupData)
		case "min":
			results["min"] = p.calculateMin(groupData)
		case "max":
			results["max"] = p.calculateMax(groupData)
		case "unique_count":
			results["unique_count"] = p.calculateUniqueCount(groupData)
		case "collect":
			results["collect"] = p.calculateCollect(groupData)
		}
	}

	return results
}

func (p *Processor) calculateSum(groupData []map[string]interface{}) map[string]float64 {
	sums := make(map[string]float64)

	for _, field := range p.fields {
		sum := 0.0
		for _, record := range groupData {
			if value, exists := record[field]; exists {
				if floatVal, ok := value.(float64); ok {
					sum += floatVal
				} else if intVal, ok := value.(int); ok {
					sum += float64(intVal)
				} else if strVal, ok := value.(string); ok {
					if parsed, err := strconv.ParseFloat(strVal, 64); err == nil {
						sum += parsed
					}
				}
			}
		}
		sums[field] = sum
	}

	return sums
}

func (p *Processor) calculateAvg(groupData []map[string]interface{}) map[string]float64 {
	avgs := make(map[string]float64)
	sums := p.calculateSum(groupData)

	for field, sum := range sums {
		if len(groupData) > 0 {
			avgs[field] = sum / float64(len(groupData))
		}
	}

	return avgs
}

func (p *Processor) calculateMin(groupData []map[string]interface{}) map[string]float64 {
	mins := make(map[string]float64)

	for _, field := range p.fields {
		var min *float64
		for _, record := range groupData {
			if value, exists := record[field]; exists {
				var floatVal float64
				var ok bool

				if floatVal, ok = value.(float64); ok {
					// Already float64
				} else if intVal, ok := value.(int); ok {
					floatVal = float64(intVal)
					ok = true
				} else if strVal, ok := value.(string); ok {
					if parsed, err := strconv.ParseFloat(strVal, 64); err == nil {
						floatVal = parsed
						ok = true
					} else {
						ok = false
					}
				}

				if ok {
					if min == nil || floatVal < *min {
						min = &floatVal
					}
				}
			}
		}
		if min != nil {
			mins[field] = *min
		}
	}

	return mins
}

func (p *Processor) calculateMax(groupData []map[string]interface{}) map[string]float64 {
	maxs := make(map[string]float64)

	for _, field := range p.fields {
		var max *float64
		for _, record := range groupData {
			if value, exists := record[field]; exists {
				var floatVal float64
				var ok bool

				if floatVal, ok = value.(float64); ok {
					// Already float64
				} else if intVal, ok := value.(int); ok {
					floatVal = float64(intVal)
					ok = true
				} else if strVal, ok := value.(string); ok {
					if parsed, err := strconv.ParseFloat(strVal, 64); err == nil {
						floatVal = parsed
						ok = true
					} else {
						ok = false
					}
				}

				if ok {
					if max == nil || floatVal > *max {
						max = &floatVal
					}
				}
			}
		}
		if max != nil {
			maxs[field] = *max
		}
	}

	return maxs
}

func (p *Processor) calculateUniqueCount(groupData []map[string]interface{}) map[string]float64 {
	uniqueCounts := make(map[string]float64)

	for _, field := range p.fields {
		unique := make(map[interface{}]bool)
		for _, record := range groupData {
			if value, exists := record[field]; exists {
				unique[value] = true
			}
		}
		uniqueCounts[field] = float64(len(unique))
	}

	return uniqueCounts
}

func (p *Processor) calculateCollect(groupData []map[string]interface{}) map[string][]interface{} {
	collections := make(map[string][]interface{})

	for _, field := range p.fields {
		var values []interface{}
		for _, record := range groupData {
			if value, exists := record[field]; exists {
				values = append(values, value)
			}
		}
		collections[field] = values
	}

	return collections
}

func parseCommaSeparated(input string) []string {
	if input == "" {
		return []string{}
	}

	parts := strings.Split(input, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
