package aggregate

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

// getProcessorSpecification returns the processor specification.
func getProcessorSpecification() sdk.Specification {
	return sdk.Specification{
		Name:        "aggregate",
		Summary:     "Comprehensive aggregation processor with windowing, expressions, and data processing",
		Description: "Collects messages that share a key and emits aggregated results based on configurable flush conditions. Supports windowing, late data handling, nested field access, expression-based aggregation, and hot configuration reload.",
		Version:     "v2.1.0",
		Author:      "Conduit",
		Parameters: map[string]config.Parameter{
			"key_field": {
				Default:     "key",
				Type:        config.ParameterTypeString,
				Description: "Field in the payload to group messages by (supports JSON paths like 'user.profile.id')",
			},
			"value_field": {
				Default:     "value",
				Type:        config.ParameterTypeString,
				Description: "Numeric field in the payload to aggregate (supports JSON paths like 'metrics.cpu.usage')",
			},
			"flush_count": {
				Default:     "0",
				Type:        config.ParameterTypeInt,
				Description: "Number of messages to collect before emitting (0 = disabled)",
			},
			"flush_timeout": {
				Default:     "0s",
				Type:        config.ParameterTypeString,
				Description: "Time window to collect messages before emitting (e.g., '30s', '5m'). 0s = disabled",
			},
			"flush_size": {
				Default:     "0",
				Type:        config.ParameterTypeInt,
				Description: "Maximum size in bytes of accumulated data before emitting (0 = disabled)",
			},
			"aggregation_functions": {
				Default:     "sum",
				Type:        config.ParameterTypeString,
				Description: "Comma-separated list: sum, avg, min, max, count, first, last, distinct, expr",
			},
			"aggregation_expressions": {
				Default:     "",
				Type:        config.ParameterTypeString,
				Description: "Custom expressions for 'expr' function (e.g., 'total_revenue=sum(price*quantity)')",
			},
			"window_type": {
				Default:     "tumbling",
				Type:        config.ParameterTypeString,
				Description: "Window type: tumbling, sliding, or session",
			},
			"window_size": {
				Default:     "0s",
				Type:        config.ParameterTypeString,
				Description: "Window size for sliding/tumbling windows (e.g., '1m'). 0s = disabled",
			},
			"window_offset": {
				Default:     "0s",
				Type:        config.ParameterTypeString,
				Description: "Window start offset for custom alignment (e.g., '30s')",
			},
			"slide_interval": {
				Default:     "0s",
				Type:        config.ParameterTypeString,
				Description: "Slide interval for sliding windows (e.g., '30s'). 0s = window_size/2",
			},
			"session_timeout": {
				Default:     "5m",
				Type:        config.ParameterTypeString,
				Description: "Session timeout for session windows (e.g., '5m')",
			},
			"allowed_lateness": {
				Default:     "0s",
				Type:        config.ParameterTypeString,
				Description: "Maximum allowed lateness for out-of-order events (watermarks)",
			},
			"timestamp_field": {
				Default:     "",
				Type:        config.ParameterTypeString,
				Description: "Field containing event timestamp for windowing (supports JSON paths, empty = use processing time)",
			},
			"config_reload_enabled": {
				Default:     "false",
				Type:        config.ParameterTypeBool,
				Description: "Enable hot configuration reload without restart",
			},
		},
	}
}

// WindowType represents the type of window for aggregation.
type WindowType string

const (
	WindowTypeTumbling WindowType = "tumbling"
	WindowTypeSliding  WindowType = "sliding"
	WindowTypeSession  WindowType = "session"
)

// AggregationFunction represents an aggregation function type.
type AggregationFunction string

const (
	AggSum      AggregationFunction = "sum"
	AggAvg      AggregationFunction = "avg"
	AggMin      AggregationFunction = "min"
	AggMax      AggregationFunction = "max"
	AggCount    AggregationFunction = "count"
	AggFirst    AggregationFunction = "first"
	AggLast     AggregationFunction = "last"
	AggDistinct AggregationFunction = "distinct"
	AggExpr     AggregationFunction = "expr"
)

// ExpressionDef represents a custom aggregation expression.
type ExpressionDef struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
}

// ProcessorConfig for generated paramgen code
type ProcessorConfig struct {
	Field     string `json:"field"`
	Threshold int    `json:"threshold"`
}

// Config holds the processor configuration.
type Config struct {
	KeyField               string                `json:"key_field"`
	ValueField             string                `json:"value_field"`
	FlushCount             int                   `json:"flush_count"`
	FlushTimeout           time.Duration         `json:"flush_timeout"`
	FlushSize              int64                 `json:"flush_size"`
	AggregationFunctions   []AggregationFunction `json:"aggregation_functions"`
	AggregationExpressions []ExpressionDef       `json:"aggregation_expressions"`
	WindowType             WindowType            `json:"window_type"`
	WindowSize             time.Duration         `json:"window_size"`
	WindowOffset           time.Duration         `json:"window_offset"`
	SlideInterval          time.Duration         `json:"slide_interval"`
	SessionTimeout         time.Duration         `json:"session_timeout"`
	AllowedLateness        time.Duration         `json:"allowed_lateness"`
	TimestampField         string                `json:"timestamp_field"`
	ConfigReloadEnabled    bool                  `json:"config_reload_enabled"`
}

// AggregateValue represents a single value in the aggregation with metadata.
type AggregateValue struct {
	Value      float64                `json:"value"`
	Timestamp  time.Time              `json:"timestamp"`
	Size       int64                  `json:"size"`
	RawData    map[string]interface{} `json:"raw_data,omitempty"`    // For expression evaluation
	DistinctID string                 `json:"distinct_id,omitempty"` // For distinct counting
}

// WindowState represents the state for a single aggregation window.
type WindowState struct {
	WindowStart    time.Time        `json:"window_start"`
	WindowEnd      time.Time        `json:"window_end"`
	Values         []AggregateValue `json:"values"`
	Count          int              `json:"count"`
	TotalSize      int64            `json:"total_size"`
	LastAccess     time.Time        `json:"last_access"`
	Watermark      time.Time        `json:"watermark"`
	DistinctValues map[string]bool  `json:"distinct_values"`
}

// AggregateState represents the complete state for a single aggregation key.
type AggregateState struct {
	Windows     map[string]*WindowState `json:"windows"`
	LastFlush   time.Time               `json:"last_flush"`
	MessageSize int64                   `json:"message_size"`
}

// AggregationResult represents the final aggregation result.
type AggregationResult struct {
	Key         string                              `json:"key"`
	WindowStart time.Time                           `json:"window_start,omitempty"`
	WindowEnd   time.Time                           `json:"window_end,omitempty"`
	Count       int                                 `json:"count"`
	Results     map[AggregationFunction]interface{} `json:"results"`
}

// ExpressionEngine provides simple expression evaluation for aggregation.
type ExpressionEngine struct {
	expressions map[string]string
}

// NewExpressionEngine creates a new expression engine.
func NewExpressionEngine(expressions []ExpressionDef) *ExpressionEngine {
	engine := &ExpressionEngine{
		expressions: make(map[string]string),
	}
	for _, expr := range expressions {
		engine.expressions[expr.Name] = expr.Expression
	}
	return engine
}

// Evaluate evaluates an expression against a set of values.
func (e *ExpressionEngine) Evaluate(name string, values []AggregateValue) (interface{}, error) {
	expr, exists := e.expressions[name]
	if !exists {
		return nil, fmt.Errorf("expression '%s' not found", name)
	}

	// Simple expression parsing - supports basic arithmetic with field references
	// Example: "sum(price * quantity)" or "avg(response_time)"
	result, err := e.parseAndEvaluate(expr, values)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression '%s': %w", expr, err)
	}

	return result, nil
}

// parseAndEvaluate provides basic expression evaluation.
func (e *ExpressionEngine) parseAndEvaluate(expr string, values []AggregateValue) (interface{}, error) {
	// Handle common patterns like "sum(field)", "avg(field1 * field2)", etc.
	expr = strings.TrimSpace(expr)

	// Extract function and arguments
	funcPattern := regexp.MustCompile(`^(\w+)\((.*)\)$`)
	matches := funcPattern.FindStringSubmatch(expr)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid expression format: %s", expr)
	}

	function := strings.ToLower(matches[1])
	args := strings.TrimSpace(matches[2])

	switch function {
	case "sum":
		return e.evaluateSum(args, values)
	case "avg":
		return e.evaluateAverage(args, values)
	case "count":
		return len(values), nil
	case "max":
		return e.evaluateMax(args, values)
	case "min":
		return e.evaluateMin(args, values)
	default:
		return nil, fmt.Errorf("unsupported function: %s", function)
	}
}

// evaluateSum evaluates sum expressions.
func (e *ExpressionEngine) evaluateSum(args string, values []AggregateValue) (float64, error) {
	var total float64
	for _, val := range values {
		result, err := e.evaluateArithmeticExpression(args, val.RawData)
		if err != nil {
			return 0, err
		}
		total += result
	}
	return total, nil
}

// evaluateAverage evaluates average expressions.
func (e *ExpressionEngine) evaluateAverage(args string, values []AggregateValue) (float64, error) {
	if len(values) == 0 {
		return 0, nil
	}
	sum, err := e.evaluateSum(args, values)
	if err != nil {
		return 0, err
	}
	return sum / float64(len(values)), nil
}

// evaluateMax evaluates max expressions.
func (e *ExpressionEngine) evaluateMax(args string, values []AggregateValue) (float64, error) {
	if len(values) == 0 {
		return 0, nil
	}

	max, err := e.evaluateArithmeticExpression(args, values[0].RawData)
	if err != nil {
		return 0, err
	}

	for _, val := range values[1:] {
		result, err := e.evaluateArithmeticExpression(args, val.RawData)
		if err != nil {
			return 0, err
		}
		if result > max {
			max = result
		}
	}
	return max, nil
}

// evaluateMin evaluates min expressions.
func (e *ExpressionEngine) evaluateMin(args string, values []AggregateValue) (float64, error) {
	if len(values) == 0 {
		return 0, nil
	}

	min, err := e.evaluateArithmeticExpression(args, values[0].RawData)
	if err != nil {
		return 0, err
	}

	for _, val := range values[1:] {
		result, err := e.evaluateArithmeticExpression(args, val.RawData)
		if err != nil {
			return 0, err
		}
		if result < min {
			min = result
		}
	}
	return min, nil
}

// evaluateArithmeticExpression evaluates simple arithmetic expressions.
func (e *ExpressionEngine) evaluateArithmeticExpression(expr string, data map[string]interface{}) (float64, error) {
	// Handle simple cases first
	if value, err := e.parseFloat(e.getNestedValue(data, expr)); err == nil {
		return value, nil
	}

	// Handle multiplication (basic case: "field1 * field2")
	if strings.Contains(expr, "*") {
		parts := strings.Split(expr, "*")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			leftVal, err := e.parseFloat(e.getNestedValue(data, left))
			if err != nil {
				return 0, err
			}

			rightVal, err := e.parseFloat(e.getNestedValue(data, right))
			if err != nil {
				return 0, err
			}

			return leftVal * rightVal, nil
		}
	}

	// Handle addition (basic case: "field1 + field2")
	if strings.Contains(expr, "+") {
		parts := strings.Split(expr, "+")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			leftVal, err := e.parseFloat(e.getNestedValue(data, left))
			if err != nil {
				return 0, err
			}

			rightVal, err := e.parseFloat(e.getNestedValue(data, right))
			if err != nil {
				return 0, err
			}

			return leftVal + rightVal, nil
		}
	}

	return 0, fmt.Errorf("unsupported expression: %s", expr)
}

// getNestedValue retrieves a value from nested data using a path.
func (e *ExpressionEngine) getNestedValue(data map[string]interface{}, path string) interface{} {
	return getNestedValue(data, path)
}

// parseFloat attempts to parse various numeric types to float64.
func (e *ExpressionEngine) parseFloat(value interface{}) (float64, error) {
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

// Processor implements the aggregate processor with comprehensive features.
type Processor struct {
	sdk.UnimplementedProcessor // embed the UnimplementedProcessor
	config           Config
	store            Storage
	mutex            sync.RWMutex
	flushTimers      map[string]*time.Timer
	timerMutex       sync.RWMutex
	stopChan         chan struct{}
	windowTicker     *time.Ticker
	expressionEngine *ExpressionEngine
	configMutex      sync.RWMutex
}

// Configure configures the processor with the provided configuration.
func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	p.configMutex.Lock()
	defer p.configMutex.Unlock()

	var processorConfig Config

	// Parse configuration using SDK utility
	err := sdk.ParseConfig(ctx, cfg, &processorConfig, getProcessorSpecification().Parameters)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Validate required fields
	if err := p.validateConfig(&processorConfig, cfg); err != nil {
		return err
	}

	// Parse duration fields
	if err := p.parseDurationFields(&processorConfig, cfg); err != nil {
		return err
	}

	// Parse and validate aggregation functions
	if err := p.parseAggregationFunctions(&processorConfig, cfg); err != nil {
		return err
	}

	// Parse aggregation expressions
	if err := p.parseAggregationExpressions(&processorConfig, cfg); err != nil {
		return err
	}

	// Validate window configurations
	if err := p.validateWindowConfig(&processorConfig); err != nil {
		return err
	}

	// Initialize expression engine
	p.expressionEngine = NewExpressionEngine(processorConfig.AggregationExpressions)

	// Initialize processor state
	p.config = processorConfig
	if p.flushTimers == nil {
		p.flushTimers = make(map[string]*time.Timer)
	}
	if p.stopChan == nil {
		p.stopChan = make(chan struct{})
	}

	return nil
}

// ReloadConfig reloads the configuration if hot reload is enabled.
func (p *Processor) ReloadConfig(ctx context.Context, cfg config.Config) error {
	if !p.config.ConfigReloadEnabled {
		return fmt.Errorf("hot configuration reload is disabled")
	}

	log.Printf("Reloading processor configuration...")
	return p.Configure(ctx, cfg)
}

// validateConfig validates the basic configuration parameters.
func (p *Processor) validateConfig(cfg *Config, rawCfg config.Config) error {
	if cfg.KeyField == "" {
		return fmt.Errorf("key_field cannot be empty")
	}
	if cfg.ValueField == "" {
		return fmt.Errorf("value_field cannot be empty")
	}

	// Validate window type
	switch WindowType(rawCfg["window_type"]) {
	case WindowTypeTumbling, WindowTypeSliding, WindowTypeSession, "":
		cfg.WindowType = WindowType(rawCfg["window_type"])
		if cfg.WindowType == "" {
			cfg.WindowType = WindowTypeTumbling
		}
	default:
		return fmt.Errorf("unsupported window type: %s", rawCfg["window_type"])
	}

	// Validate that at least one flush condition is set
	if cfg.FlushCount <= 0 && cfg.FlushTimeout == 0 && cfg.FlushSize <= 0 && cfg.WindowSize == 0 {
		return fmt.Errorf("at least one flush condition must be set (flush_count, flush_timeout, flush_size, or window_size)")
	}

	return nil
}

// parseDurationFields parses and validates duration configuration fields.
func (p *Processor) parseDurationFields(cfg *Config, rawCfg config.Config) error {
	durationFields := map[string]*time.Duration{
		"flush_timeout":    &cfg.FlushTimeout,
		"window_size":      &cfg.WindowSize,
		"window_offset":    &cfg.WindowOffset,
		"slide_interval":   &cfg.SlideInterval,
		"session_timeout":  &cfg.SessionTimeout,
		"allowed_lateness": &cfg.AllowedLateness,
	}

	for fieldName, target := range durationFields {
		if durationStr := rawCfg[fieldName]; durationStr != "" {
			duration, err := time.ParseDuration(durationStr)
			if err != nil {
				return fmt.Errorf("invalid %s: %w", fieldName, err)
			}
			*target = duration
		}
	}

	// Set default slide interval if not specified
	if cfg.SlideInterval == 0 && cfg.WindowSize > 0 {
		cfg.SlideInterval = cfg.WindowSize / 2
	}

	return nil
}

// parseAggregationFunctions parses and validates aggregation function configuration.
func (p *Processor) parseAggregationFunctions(cfg *Config, rawCfg config.Config) error {
	aggFuncStr := rawCfg["aggregation_functions"]
	if aggFuncStr == "" {
		cfg.AggregationFunctions = []AggregationFunction{AggSum}
		return nil
	}

	functions := strings.Split(aggFuncStr, ",")
	cfg.AggregationFunctions = make([]AggregationFunction, 0, len(functions))

	for _, funcStr := range functions {
		funcStr = strings.TrimSpace(funcStr)
		switch AggregationFunction(funcStr) {
		case AggSum, AggAvg, AggMin, AggMax, AggCount, AggFirst, AggLast, AggDistinct, AggExpr:
			cfg.AggregationFunctions = append(cfg.AggregationFunctions, AggregationFunction(funcStr))
		default:
			return fmt.Errorf("unsupported aggregation function: %s", funcStr)
		}
	}

	return nil
}

// parseAggregationExpressions parses custom aggregation expressions.
func (p *Processor) parseAggregationExpressions(cfg *Config, rawCfg config.Config) error {
	exprStr := rawCfg["aggregation_expressions"]
	if exprStr == "" {
		return nil
	}

	// Parse expressions in format: "name1=expr1,name2=expr2"
	expressions := strings.Split(exprStr, ",")
	cfg.AggregationExpressions = make([]ExpressionDef, 0, len(expressions))

	for _, exprDef := range expressions {
		parts := strings.SplitN(strings.TrimSpace(exprDef), "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid expression format: %s (expected 'name=expression')", exprDef)
		}

		cfg.AggregationExpressions = append(cfg.AggregationExpressions, ExpressionDef{
			Name:       strings.TrimSpace(parts[0]),
			Expression: strings.TrimSpace(parts[1]),
		})
	}

	return nil
}

// validateWindowConfig validates window-specific configuration parameters.
func (p *Processor) validateWindowConfig(cfg *Config) error {
	switch cfg.WindowType {
	case WindowTypeSliding:
		if cfg.WindowSize == 0 {
			return fmt.Errorf("window_size must be set for sliding windows")
		}
		if cfg.SlideInterval >= cfg.WindowSize {
			return fmt.Errorf("slide_interval must be less than window_size")
		}
	case WindowTypeSession:
		if cfg.SessionTimeout == 0 {
			return fmt.Errorf("session_timeout must be set for session windows")
		}
	case WindowTypeTumbling:
		// No additional validation needed for tumbling windows
	}

	return nil
}

// Open initializes the processor and sets up background tasks.
func (p *Processor) Open(ctx context.Context) error {
	// Initialize storage if needed
	p.store = Storage{}

	// Start window cleanup routine if using windowing
	if p.config.WindowSize > 0 {
		p.startWindowCleanup()
	}

	return nil
}

// startWindowCleanup starts a background goroutine to clean up expired windows.
func (p *Processor) startWindowCleanup() {
	cleanupInterval := p.config.WindowSize / 4
	if cleanupInterval < time.Minute {
		cleanupInterval = time.Minute // Minimum cleanup interval
	}

	p.windowTicker = time.NewTicker(cleanupInterval)
	go func() {
		defer p.windowTicker.Stop()
		for {
			select {
			case <-p.windowTicker.C:
				p.cleanupExpiredWindows()
			case <-p.stopChan:
				return
			}
		}
	}()
}

// cleanupExpiredWindows removes expired windows based on watermarks.
func (p *Processor) cleanupExpiredWindows() {
	// In a production implementation, this would iterate through all keys
	// and clean up windows that are beyond the allowed lateness threshold
}

// Process processes a batch of records and returns the results.
func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	results := make([]sdk.ProcessedRecord, 0, len(records))

	for _, record := range records {
		processedRecords := p.processRecord(ctx, record)
		results = append(results, processedRecords...)
	}

	return results
}

// processRecord processes a single record and returns potentially multiple results.
func (p *Processor) processRecord(ctx context.Context, record opencdc.Record) []sdk.ProcessedRecord {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Extract and validate payload
	payload, err := p.extractPayload(record)
	if err != nil {
		return []sdk.ProcessedRecord{sdk.ErrorRecord{Error: err}}
	}

	// Extract key, value, and metadata using nested field support
	key, value, timestamp, messageSize, rawData, err := p.extractRecordData(payload, record)
	if err != nil {
		return []sdk.ProcessedRecord{sdk.ErrorRecord{Error: err}}
	}

	// Apply watermark logic for late data handling
	if !p.isWithinWatermark(timestamp) {
		log.Printf("Dropping late message for key '%s', timestamp: %v", key, timestamp)
		return []sdk.ProcessedRecord{sdk.FilterRecord{}}
	}

	// Get current state for the key
	state, err := p.getState(ctx, key)
	if err != nil {
		return []sdk.ProcessedRecord{sdk.ErrorRecord{Error: fmt.Errorf("failed to get state for key '%s': %w", key, err)}}
	}

	// Create aggregate value with enhanced data
	aggregateValue := AggregateValue{
		Value:      value,
		Timestamp:  timestamp,
		Size:       messageSize,
		RawData:    rawData,
		DistinctID: p.generateDistinctID(rawData),
	}

	// Process the value through appropriate windows
	return p.processValueThroughWindows(ctx, record, key, aggregateValue, state)
}

// extractPayload extracts and validates the JSON payload from a record.
func (p *Processor) extractPayload(record opencdc.Record) (map[string]interface{}, error) {
	payload := make(map[string]interface{})
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return payload, nil
}

// extractRecordData extracts key, value, timestamp, and raw data with nested field support.
func (p *Processor) extractRecordData(payload map[string]interface{}, record opencdc.Record) (string, float64, time.Time, int64, map[string]interface{}, error) {
	// Extract key using nested field support
	keyValue := getNestedValue(payload, p.config.KeyField)
	if keyValue == nil {
		return "", 0, time.Time{}, 0, nil, fmt.Errorf("key field '%s' not found in payload", p.config.KeyField)
	}
	key := fmt.Sprintf("%v", keyValue)

	// Extract value using nested field support
	valueInterface := getNestedValue(payload, p.config.ValueField)
	if valueInterface == nil {
		return "", 0, time.Time{}, 0, nil, fmt.Errorf("value field '%s' not found in payload", p.config.ValueField)
	}

	value, err := p.parseFloat(valueInterface)
	if err != nil {
		return "", 0, time.Time{}, 0, nil, fmt.Errorf("failed to parse value field '%s': %w", p.config.ValueField, err)
	}

	// Extract timestamp using nested field support
	timestamp := time.Now()
	if p.config.TimestampField != "" {
		if timestampValue := getNestedValue(payload, p.config.TimestampField); timestampValue != nil {
			if parsedTime, err := p.parseTimestamp(timestampValue); err == nil {
				timestamp = parsedTime
			}
		}
	}

	// Calculate message size
	messageSize := int64(len(record.Payload.After.Bytes()))

	return key, value, timestamp, messageSize, payload, nil
}

// getNestedValue retrieves a value from nested data using a JSON path.
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
			// Last part - return the value
			return current[part]
		}

		// Navigate deeper
		next, ok := current[part]
		if !ok {
			return nil
		}

		switch v := next.(type) {
		case map[string]interface{}:
			current = v
		default:
			return nil // Cannot navigate further
		}
	}

	return nil
}

// isWithinWatermark checks if a timestamp is within the allowed lateness threshold.
func (p *Processor) isWithinWatermark(eventTime time.Time) bool {
	if p.config.AllowedLateness == 0 {
		return true // No watermark configured
	}

	watermark := time.Now().Add(-p.config.AllowedLateness)
	return eventTime.After(watermark)
}

// generateDistinctID generates a unique identifier for distinct counting.
func (p *Processor) generateDistinctID(data map[string]interface{}) string {
	// Use the entire record for distinct counting
	// In a more sophisticated implementation, this could be configurable
	dataBytes, _ := json.Marshal(data)
	return fmt.Sprintf("%x", dataBytes)
}

// processValueThroughWindows processes a value through all appropriate windows.
func (p *Processor) processValueThroughWindows(ctx context.Context, record opencdc.Record, key string, aggregateValue AggregateValue, state *AggregateState) []sdk.ProcessedRecord {
	windows := p.getWindowsForValue(aggregateValue.Timestamp, state)
	results := make([]sdk.ProcessedRecord, 0)

	for windowKey, window := range windows {
		// Add value to window
		window.Values = append(window.Values, aggregateValue)
		window.Count++
		window.TotalSize += aggregateValue.Size
		window.LastAccess = time.Now()
		window.Watermark = time.Now()

		// Add to distinct values if distinct aggregation is enabled
		if p.hasDistinctAggregation() && aggregateValue.DistinctID != "" {
			if window.DistinctValues == nil {
				window.DistinctValues = make(map[string]bool)
			}
			window.DistinctValues[aggregateValue.DistinctID] = true
		}

		state.Windows[windowKey] = window
		state.MessageSize += aggregateValue.Size

		// Check if window should be flushed
		if p.shouldFlush(key, window, state) {
			aggregatedRecord, err := p.createAggregatedRecord(record, key, window)
			if err != nil {
				results = append(results, sdk.ErrorRecord{Error: fmt.Errorf("failed to create aggregated record: %w", err)})
				continue
			}

			singleRecord := sdk.SingleRecord(aggregatedRecord)
			results = append(results, singleRecord)

			// Remove the window from state after successful flush
			delete(state.Windows, windowKey)
			state.LastFlush = time.Now()
		}
	}

	// Save updated state
	if err := p.setState(ctx, key, *state); err != nil {
		return []sdk.ProcessedRecord{sdk.ErrorRecord{Error: fmt.Errorf("failed to save state for key '%s': %w", key, err)}}
	}

	// Set up flush timer for time-based flushing if no immediate flush occurred
	if len(results) == 0 && p.config.FlushTimeout > 0 {
		p.setupFlushTimer(ctx, key)
	}

	// Return filter record if no flush occurred
	if len(results) == 0 {
		return []sdk.ProcessedRecord{sdk.FilterRecord{}}
	}

	return results
}

// hasDistinctAggregation checks if distinct aggregation is enabled.
func (p *Processor) hasDistinctAggregation() bool {
	for _, fn := range p.config.AggregationFunctions {
		if fn == AggDistinct {
			return true
		}
	}
	return false
}

// getWindowsForValue determines which windows a value belongs to based on window type with offset support.
func (p *Processor) getWindowsForValue(timestamp time.Time, state *AggregateState) map[string]*WindowState {
	windows := make(map[string]*WindowState)

	switch p.config.WindowType {
	case WindowTypeTumbling:
		windowKey := p.getTumblingWindowKey(timestamp)
		windows[windowKey] = p.getOrCreateWindow(state, windowKey, timestamp, WindowTypeTumbling)

	case WindowTypeSliding:
		windowKeys := p.getSlidingWindowKeys(timestamp)
		for _, windowKey := range windowKeys {
			windows[windowKey] = p.getOrCreateWindow(state, windowKey, timestamp, WindowTypeSliding)
		}

	case WindowTypeSession:
		windowKey := p.getSessionWindowKey(timestamp, state)
		windows[windowKey] = p.getOrCreateWindow(state, windowKey, timestamp, WindowTypeSession)
	}

	return windows
}

// getOrCreateWindow gets an existing window or creates a new one.
func (p *Processor) getOrCreateWindow(state *AggregateState, windowKey string, timestamp time.Time, windowType WindowType) *WindowState {
	if window, exists := state.Windows[windowKey]; exists {
		// For session windows, extend the window if within timeout
		if windowType == WindowTypeSession {
			if timestamp.Sub(window.WindowEnd) <= p.config.SessionTimeout {
				window.WindowEnd = timestamp.Add(p.config.SessionTimeout)
			}
		}
		return window
	}
	return p.createWindow(timestamp, windowType, windowKey)
}

// getTumblingWindowKey generates a key for tumbling windows with offset support.
func (p *Processor) getTumblingWindowKey(timestamp time.Time) string {
	if p.config.WindowSize == 0 {
		return "default"
	}

	// Apply window offset for custom alignment
	adjustedTime := timestamp.Add(-p.config.WindowOffset)
	windowStart := adjustedTime.Truncate(p.config.WindowSize).Add(p.config.WindowOffset)
	return fmt.Sprintf("tumbling_%d", windowStart.Unix())
}

// getSlidingWindowKeys generates keys for all sliding windows that should contain this timestamp.
func (p *Processor) getSlidingWindowKeys(timestamp time.Time) []string {
	if p.config.WindowSize == 0 || p.config.SlideInterval == 0 {
		return []string{"default"}
	}

	var keys []string
	windowCount := int(p.config.WindowSize / p.config.SlideInterval)

	for i := 0; i < windowCount; i++ {
		// Apply window offset
		adjustedTime := timestamp.Add(-p.config.WindowOffset)
		windowStart := adjustedTime.Truncate(p.config.SlideInterval).Add(-time.Duration(i) * p.config.SlideInterval).Add(p.config.WindowOffset)
		windowEnd := windowStart.Add(p.config.WindowSize)

		// Check if timestamp falls within this window
		if !timestamp.Before(windowStart) && timestamp.Before(windowEnd) {
			key := fmt.Sprintf("sliding_%d", windowStart.Unix())
			keys = append(keys, key)
		}
	}

	return keys
}

// getSessionWindowKey finds an existing session window or generates a key for a new one.
func (p *Processor) getSessionWindowKey(timestamp time.Time, state *AggregateState) string {
	// Find an existing session window within timeout
	for key, window := range state.Windows {
		if strings.HasPrefix(key, "session_") {
			if timestamp.Sub(window.WindowEnd) <= p.config.SessionTimeout {
				return key
			}
		}
	}
	// Create new session window key
	return fmt.Sprintf("session_%d", timestamp.Unix())
}

// createWindow creates a new window with appropriate start and end times.
func (p *Processor) createWindow(timestamp time.Time, windowType WindowType, windowKey string) *WindowState {
	var windowStart, windowEnd time.Time

	switch windowType {
	case WindowTypeTumbling:
		if p.config.WindowSize > 0 {
			adjustedTime := timestamp.Add(-p.config.WindowOffset)
			windowStart = adjustedTime.Truncate(p.config.WindowSize).Add(p.config.WindowOffset)
			windowEnd = windowStart.Add(p.config.WindowSize)
		}

	case WindowTypeSliding:
		if p.config.WindowSize > 0 {
			// Extract window start from the key for consistency
			if strings.HasPrefix(windowKey, "sliding_") {
				if unixTime, err := strconv.ParseInt(windowKey[8:], 10, 64); err == nil {
					windowStart = time.Unix(unixTime, 0)
					windowEnd = windowStart.Add(p.config.WindowSize)
				}
			}
		}

	case WindowTypeSession:
		windowStart = timestamp
		windowEnd = timestamp.Add(p.config.SessionTimeout)
	}

	return &WindowState{
		WindowStart:    windowStart,
		WindowEnd:      windowEnd,
		Values:         make([]AggregateValue, 0),
		Count:          0,
		TotalSize:      0,
		LastAccess:     time.Now(),
		Watermark:      time.Now(),
		DistinctValues: make(map[string]bool),
	}
}

// shouldFlush determines if a window should be flushed based on configured conditions.
func (p *Processor) shouldFlush(key string, window *WindowState, state *AggregateState) bool {
	now := time.Now()

	// Count-based flush
	if p.config.FlushCount > 0 && window.Count >= p.config.FlushCount {
		return true
	}

	// Size-based flush
	if p.config.FlushSize > 0 && window.TotalSize >= p.config.FlushSize {
		return true
	}

	// Time-based flush for windows
	if p.config.WindowSize > 0 && !window.WindowEnd.IsZero() && now.After(window.WindowEnd) {
		return true
	}

	// Timeout-based flush (for non-windowed aggregation)
	if p.config.FlushTimeout > 0 && p.config.WindowSize == 0 &&
		!state.LastFlush.IsZero() && now.Sub(state.LastFlush) >= p.config.FlushTimeout {
		return true
	}

	return false
}

// setupFlushTimer sets up a timer for time-based flushing.
func (p *Processor) setupFlushTimer(ctx context.Context, key string) {
	if p.config.FlushTimeout <= 0 {
		return
	}

	p.timerMutex.Lock()
	defer p.timerMutex.Unlock()

	// Cancel existing timer if it exists
	if timer, exists := p.flushTimers[key]; exists {
		timer.Stop()
	}

	// Create new timer
	p.flushTimers[key] = time.AfterFunc(p.config.FlushTimeout, func() {
		p.flushByTimer(ctx, key)
	})
}

// flushByTimer handles timer-based flushing (simplified implementation).
func (p *Processor) flushByTimer(ctx context.Context, key string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Clean up timer
	p.timerMutex.Lock()
	delete(p.flushTimers, key)
	p.timerMutex.Unlock()

	// Get state and check for pending data
	state, err := p.getState(ctx, key)
	if err != nil {
		log.Printf("Error getting state for timer flush of key '%s': %v", key, err)
		return
	}

	// Log timer-triggered flush conditions
	for windowKey, window := range state.Windows {
		if window.Count > 0 {
			log.Printf("Timer triggered flush condition for key '%s', window '%s' with %d records",
				key, windowKey, window.Count)
		}
	}
}

// calculateAggregations calculates all configured aggregation functions for the given values.
func (p *Processor) calculateAggregations(values []AggregateValue) map[AggregationFunction]interface{} {
	results := make(map[AggregationFunction]interface{})

	if len(values) == 0 {
		return results
	}

	// Sort values by timestamp for first/last calculations
	sortedValues := make([]AggregateValue, len(values))
	copy(sortedValues, values)
	sort.Slice(sortedValues, func(i, j int) bool {
		return sortedValues[i].Timestamp.Before(sortedValues[j].Timestamp)
	})

	for _, function := range p.config.AggregationFunctions {
		switch function {
		case AggSum:
			results[function] = p.calculateSum(values)
		case AggAvg:
			results[function] = p.calculateAverage(values)
		case AggMin:
			results[function] = p.calculateMin(values)
		case AggMax:
			results[function] = p.calculateMax(values)
		case AggCount:
			results[function] = len(values)
		case AggFirst:
			results[function] = sortedValues[0].Value
		case AggLast:
			results[function] = sortedValues[len(sortedValues)-1].Value
		case AggDistinct:
			results[function] = p.calculateDistinct(values)
		case AggExpr:
			results[function] = p.calculateExpressions(values)
		}
	}

	return results
}

// calculateSum calculates the sum of all values.
func (p *Processor) calculateSum(values []AggregateValue) float64 {
	sum := 0.0
	for _, val := range values {
		sum += val.Value
	}
	return sum
}

// calculateAverage calculates the average of all values.
func (p *Processor) calculateAverage(values []AggregateValue) float64 {
	if len(values) == 0 {
		return 0.0
	}
	return p.calculateSum(values) / float64(len(values))
}

// calculateMin finds the minimum value.
func (p *Processor) calculateMin(values []AggregateValue) float64 {
	min := values[0].Value
	for _, val := range values[1:] {
		if val.Value < min {
			min = val.Value
		}
	}
	return min
}

// calculateMax finds the maximum value.
func (p *Processor) calculateMax(values []AggregateValue) float64 {
	max := values[0].Value
	for _, val := range values[1:] {
		if val.Value > max {
			max = val.Value
		}
	}
	return max
}

// calculateDistinct calculates distinct count.
func (p *Processor) calculateDistinct(values []AggregateValue) int {
	distinct := make(map[string]bool)
	for _, val := range values {
		if val.DistinctID != "" {
			distinct[val.DistinctID] = true
		}
	}
	return len(distinct)
}

// calculateExpressions evaluates custom expressions.
func (p *Processor) calculateExpressions(values []AggregateValue) map[string]interface{} {
	results := make(map[string]interface{})

	for _, expr := range p.config.AggregationExpressions {
		if result, err := p.expressionEngine.Evaluate(expr.Name, values); err == nil {
			results[expr.Name] = result
		} else {
			log.Printf("Error evaluating expression '%s': %v", expr.Name, err)
		}
	}

	return results
}

// parseFloat attempts to parse various numeric types to float64.
func (p *Processor) parseFloat(value interface{}) (float64, error) {
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

// parseTimestamp attempts to parse various timestamp formats.
func (p *Processor) parseTimestamp(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case string:
		// Try common timestamp formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", v)
	case int64:
		return time.Unix(v, 0), nil
	case float64:
		return time.Unix(int64(v), 0), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type: %T", value)
	}
}

// Storage is a simple in-memory storage implementation
type Storage struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// Get retrieves data from storage
func (s *Storage) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.data == nil {
		s.data = make(map[string][]byte)
	}
	
	if val, ok := s.data[key]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("key not found")
}

// Set stores data in storage
func (s *Storage) Set(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.data == nil {
		s.data = make(map[string][]byte)
	}
	
	s.data[key] = value
	return nil
}

// Delete removes data from storage
func (s *Storage) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.data == nil {
		return nil
	}
	
	delete(s.data, key)
	return nil
}

// getState retrieves the aggregation state for a given key.
func (p *Processor) getState(ctx context.Context, key string) (*AggregateState, error) {
	stateKey := fmt.Sprintf("aggregate:%s", key)

	data, err := p.store.Get(ctx, stateKey)
	if err != nil {
		if err.Error() == "key not found" {
			return &AggregateState{
				Windows:     make(map[string]*WindowState),
				LastFlush:   time.Time{},
				MessageSize: 0,
			}, nil
		}
		return nil, err
	}

	var state AggregateState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	// Ensure windows map is initialized
	if state.Windows == nil {
		state.Windows = make(map[string]*WindowState)
	}

	return &state, nil
}

// setState saves the aggregation state for a given key.
func (p *Processor) setState(ctx context.Context, key string, state AggregateState) error {
	stateKey := fmt.Sprintf("aggregate:%s", key)

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	return p.store.Set(ctx, stateKey, data)
}

// clearState removes the aggregation state for a given key.
func (p *Processor) clearState(ctx context.Context, key string) error {
	stateKey := fmt.Sprintf("aggregate:%s", key)
	return p.store.Delete(ctx, stateKey)
}

// createAggregatedRecord creates a new record with aggregated data.
func (p *Processor) createAggregatedRecord(originalRecord opencdc.Record, key string, window *WindowState) (opencdc.Record, error) {
	aggregations := p.calculateAggregations(window.Values)

	aggregatedData := AggregationResult{
		Key:         key,
		WindowStart: window.WindowStart,
		WindowEnd:   window.WindowEnd,
		Count:       window.Count,
		Results:     aggregations,
	}

	payload, err := json.Marshal(aggregatedData)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to marshal aggregated data: %w", err)
	}

	// Create new record based on original
	newRecord := originalRecord
	newRecord.Payload = opencdc.Change{
		After: opencdc.RawData(payload),
	}

	// Add comprehensive metadata
	if newRecord.Metadata == nil {
		newRecord.Metadata = make(opencdc.Metadata)
	}

	p.addMetadata(newRecord.Metadata, key, window)

	return newRecord, nil
}

// addMetadata adds comprehensive metadata to the aggregated record.
func (p *Processor) addMetadata(metadata opencdc.Metadata, key string, window *WindowState) {
	metadata["conduit.aggregate.key"] = key
	metadata["conduit.aggregate.count"] = fmt.Sprintf("%d", window.Count)
	metadata["conduit.aggregate.window.start"] = window.WindowStart.Format(time.RFC3339)
	metadata["conduit.aggregate.window.end"] = window.WindowEnd.Format(time.RFC3339)
	metadata["conduit.aggregate.window.type"] = string(p.config.WindowType)
	metadata["conduit.aggregate.watermark"] = window.Watermark.Format(time.RFC3339)

	// Add function list
	var funcs []string
	for _, f := range p.config.AggregationFunctions {
		funcs = append(funcs, string(f))
	}
	metadata["conduit.aggregate.functions"] = strings.Join(funcs, ",")

	// Add distinct count if applicable
	if len(window.DistinctValues) > 0 {
		metadata["conduit.aggregate.distinct.count"] = fmt.Sprintf("%d", len(window.DistinctValues))
	}
}

// Teardown cleans up the processor resources.
func (p *Processor) Teardown(ctx context.Context) error {
	// Stop window cleanup goroutine
	if p.windowTicker != nil {
		p.windowTicker.Stop()
	}

	// Signal shutdown to background goroutines
	close(p.stopChan)

	// Cancel and clean up all timers
	p.timerMutex.Lock()
	for key, timer := range p.flushTimers {
		timer.Stop()
		delete(p.flushTimers, key)
	}
	p.timerMutex.Unlock()

	return nil
}

// Specification implements the sdk.Processor interface.
func (p *Processor) Specification() (sdk.Specification, error) {
	return getProcessorSpecification(), nil
}

// MiddlewareOptions implements the sdk.Processor interface.
func (p *Processor) MiddlewareOptions() []sdk.ProcessorMiddlewareOption {
	return nil // or return specific middleware options if needed
}

// NewProcessor creates a new aggregate processor instance.
func NewProcessor() sdk.Processor {
	return &Processor{}
}

// main function for standalone execution.
func main() {
	if err := sdk.Run(NewProcessor()); err != nil {
		log.Fatal(err)
	}
}
