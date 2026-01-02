// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquet // import "github.com/hrl20/otel-ducklakeexporter/internal/parquet"

import (
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/hrl20/otel-ducklakeexporter/internal/traceutil"
)

// OTLPLog represents an OpenTelemetry log record in parquet format.
// Field tags define the parquet schema using github.com/parquet-go/parquet-go conventions.
// Timestamps use time.Time with timestamp(millisecond) logical type.
type OTLPLog struct {
	// Core log fields
	Timestamp      time.Time `parquet:"timestamp"`
	TraceID        string    `parquet:"trace_id,optional,dict"`
	SpanID         string    `parquet:"span_id,optional,dict"`
	SeverityNumber int32     `parquet:"severity_number"`
	SeverityText   string    `parquet:"severity_text,optional,dict"`
	Body           string    `parquet:"body"`

	// Scope information
	ScopeName    string `parquet:"scope_name,optional,dict"`
	ScopeVersion string `parquet:"scope_version,optional,dict"`

	// Attributes (stored as JSON strings)
	ResourceAttributes string `parquet:"resource_attributes,optional,json"`
	LogAttributes      string `parquet:"log_attributes,optional,json"`

	// Additional metadata
	Flags        uint32    `parquet:"flags,optional"`
	ObservedTime time.Time `parquet:"observed_timestamp"`
}

// FromLogRecord converts a plog.LogRecord to an OTLPLog.
func FromLogRecord(
	logRecord plog.LogRecord,
	resourceAttrs pcommon.Map,
	scopeName string,
	scopeVersion string,
) (*OTLPLog, error) {
	// Convert attributes to JSON
	resourceJSON, err := attributesToJSON(resourceAttrs)
	if err != nil {
		return nil, fmt.Errorf("failed to convert resource attributes: %w", err)
	}

	logJSON, err := attributesToJSON(logRecord.Attributes())
	if err != nil {
		return nil, fmt.Errorf("failed to convert log attributes: %w", err)
	}

	otlpLog := &OTLPLog{
		Timestamp:          logRecord.Timestamp().AsTime(),
		TraceID:            traceutil.TraceIDToHexOrEmptyString(logRecord.TraceID()),
		SpanID:             traceutil.SpanIDToHexOrEmptyString(logRecord.SpanID()),
		SeverityNumber:     int32(logRecord.SeverityNumber()),
		SeverityText:       logRecord.SeverityText(),
		Body:               logRecord.Body().AsString(),
		ScopeName:          scopeName,
		ScopeVersion:       scopeVersion,
		ResourceAttributes: resourceJSON,
		LogAttributes:      logJSON,
		Flags:              uint32(logRecord.Flags()),
		ObservedTime:       logRecord.ObservedTimestamp().AsTime(),
	}

	return otlpLog, nil
}

// attributesToJSON converts a pcommon.Map to a JSON string.
func attributesToJSON(attrs pcommon.Map) (string, error) {
	if attrs.Len() == 0 {
		return "{}", nil
	}

	// Convert to a Go map
	attrMap := make(map[string]any, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		attrMap[k] = v.AsRaw()
		return true
	})

	// Marshal to JSON
	jsonBytes, err := json.Marshal(attrMap)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

// GetColumnNames returns the names of all columns in the parquet schema.
// This is used for metadata registration.
func GetColumnNames() []string {
	return []string{
		"timestamp",
		"trace_id",
		"span_id",
		"severity_number",
		"severity_text",
		"body",
		"scope_name",
		"scope_version",
		"resource_attributes",
		"log_attributes",
		"flags",
		"observed_timestamp",
	}
}

// GetColumnTypes returns the types of all columns in the parquet schema.
// This is used for metadata registration in DuckLake.
// Uses DuckDB standard primitive type names.
func GetColumnTypes() []string {
	return []string{
		"timestamptz", // timestamp (millisecond precision with timezone)
		"varchar",     // trace_id
		"varchar",     // span_id
		"int32",       // severity_number
		"varchar",     // severity_text
		"varchar",     // body
		"varchar",     // scope_name
		"varchar",     // scope_version
		"json",        // resource_attributes
		"json",        // log_attributes
		"uint32",      // flags
		"timestamptz", // observed_timestamp (millisecond precision with timezone)
	}
}

// OTLPSpan represents an OpenTelemetry span in parquet format.
// Field tags define the parquet schema using github.com/parquet-go/parquet-go conventions.
type OTLPSpan struct {
	// Core span fields
	TraceID      string    `parquet:"trace_id,dict"`
	SpanID       string    `parquet:"span_id,dict"`
	ParentSpanID string    `parquet:"parent_span_id,optional,dict"`
	Name         string    `parquet:"name,dict"`
	Kind         int32     `parquet:"kind"`
	StartTime    time.Time `parquet:"start_time"`
	EndTime      time.Time `parquet:"end_time"`
	DurationNs   int64     `parquet:"duration_ns"`

	// Status
	StatusCode    int32  `parquet:"status_code"`
	StatusMessage string `parquet:"status_message,optional"`

	// Scope information
	ScopeName    string `parquet:"scope_name,optional,dict"`
	ScopeVersion string `parquet:"scope_version,optional,dict"`

	// Attributes (stored as JSON strings)
	ResourceAttributes string `parquet:"resource_attributes,optional,json"`
	SpanAttributes     string `parquet:"span_attributes,optional,json"`

	// Events and Links (stored as JSON arrays)
	Events string `parquet:"events,optional,json"`
	Links  string `parquet:"links,optional,json"`

	// Dropped counts
	DroppedAttributesCount uint32 `parquet:"dropped_attributes_count,optional"`
	DroppedEventsCount     uint32 `parquet:"dropped_events_count,optional"`
	DroppedLinksCount      uint32 `parquet:"dropped_links_count,optional"`

	// Additional metadata
	TraceState string `parquet:"trace_state,optional"`
	Flags      uint32 `parquet:"flags,optional"`
}

// SpanEvent represents a span event for JSON serialization.
type SpanEvent struct {
	Timestamp         string         `json:"timestamp"`
	Name              string         `json:"name"`
	Attributes        map[string]any `json:"attributes,omitempty"`
	DroppedAttributes uint32         `json:"dropped_attributes,omitempty"`
}

// SpanLink represents a span link for JSON serialization.
type SpanLink struct {
	TraceID           string         `json:"trace_id"`
	SpanID            string         `json:"span_id"`
	TraceState        string         `json:"trace_state,omitempty"`
	Attributes        map[string]any `json:"attributes,omitempty"`
	DroppedAttributes uint32         `json:"dropped_attributes,omitempty"`
	Flags             uint32         `json:"flags,omitempty"`
}

// FromSpan converts a ptrace.Span to an OTLPSpan.
func FromSpan(
	span ptrace.Span,
	resourceAttrs pcommon.Map,
	scopeName string,
	scopeVersion string,
) (*OTLPSpan, error) {
	// Convert attributes to JSON
	resourceJSON, err := attributesToJSON(resourceAttrs)
	if err != nil {
		return nil, fmt.Errorf("failed to convert resource attributes: %w", err)
	}

	spanJSON, err := attributesToJSON(span.Attributes())
	if err != nil {
		return nil, fmt.Errorf("failed to convert span attributes: %w", err)
	}

	// Convert events to JSON
	eventsJSON, err := eventsToJSON(span.Events())
	if err != nil {
		return nil, fmt.Errorf("failed to convert events: %w", err)
	}

	// Convert links to JSON
	linksJSON, err := linksToJSON(span.Links())
	if err != nil {
		return nil, fmt.Errorf("failed to convert links: %w", err)
	}

	// Calculate duration
	startTime := span.StartTimestamp().AsTime()
	endTime := span.EndTimestamp().AsTime()
	durationNs := endTime.Sub(startTime).Nanoseconds()

	otlpSpan := &OTLPSpan{
		TraceID:                traceutil.TraceIDToHexOrEmptyString(span.TraceID()),
		SpanID:                 traceutil.SpanIDToHexOrEmptyString(span.SpanID()),
		ParentSpanID:           traceutil.SpanIDToHexOrEmptyString(span.ParentSpanID()),
		Name:                   span.Name(),
		Kind:                   int32(span.Kind()),
		StartTime:              startTime,
		EndTime:                endTime,
		DurationNs:             durationNs,
		StatusCode:             int32(span.Status().Code()),
		StatusMessage:          span.Status().Message(),
		ScopeName:              scopeName,
		ScopeVersion:           scopeVersion,
		ResourceAttributes:     resourceJSON,
		SpanAttributes:         spanJSON,
		Events:                 eventsJSON,
		Links:                  linksJSON,
		DroppedAttributesCount: span.DroppedAttributesCount(),
		DroppedEventsCount:     span.DroppedEventsCount(),
		DroppedLinksCount:      span.DroppedLinksCount(),
		TraceState:             span.TraceState().AsRaw(),
		Flags:                  uint32(span.Flags()),
	}

	return otlpSpan, nil
}

// eventsToJSON converts span events to a JSON array string.
func eventsToJSON(events ptrace.SpanEventSlice) (string, error) {
	if events.Len() == 0 {
		return "[]", nil
	}

	eventsList := make([]SpanEvent, 0, events.Len())
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)

		// Convert attributes to map
		attrMap := make(map[string]any)
		event.Attributes().Range(func(k string, v pcommon.Value) bool {
			attrMap[k] = v.AsRaw()
			return true
		})

		eventsList = append(eventsList, SpanEvent{
			Timestamp:         event.Timestamp().AsTime().Format("2006-01-02 15:04:05.000000-07"),
			Name:              event.Name(),
			Attributes:        attrMap,
			DroppedAttributes: event.DroppedAttributesCount(),
		})
	}

	jsonBytes, err := json.Marshal(eventsList)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

// linksToJSON converts span links to a JSON array string.
func linksToJSON(links ptrace.SpanLinkSlice) (string, error) {
	if links.Len() == 0 {
		return "[]", nil
	}

	linksList := make([]SpanLink, 0, links.Len())
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)

		// Convert attributes to map
		attrMap := make(map[string]any)
		link.Attributes().Range(func(k string, v pcommon.Value) bool {
			attrMap[k] = v.AsRaw()
			return true
		})

		linksList = append(linksList, SpanLink{
			TraceID:           traceutil.TraceIDToHexOrEmptyString(link.TraceID()),
			SpanID:            traceutil.SpanIDToHexOrEmptyString(link.SpanID()),
			TraceState:        link.TraceState().AsRaw(),
			Attributes:        attrMap,
			DroppedAttributes: link.DroppedAttributesCount(),
			Flags:             uint32(link.Flags()),
		})
	}

	jsonBytes, err := json.Marshal(linksList)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

// GetSpanColumnNames returns the names of all columns in the span parquet schema.
func GetSpanColumnNames() []string {
	return []string{
		"trace_id",
		"span_id",
		"parent_span_id",
		"name",
		"kind",
		"start_time",
		"end_time",
		"duration_ns",
		"status_code",
		"status_message",
		"scope_name",
		"scope_version",
		"resource_attributes",
		"span_attributes",
		"events",
		"links",
		"dropped_attributes_count",
		"dropped_events_count",
		"dropped_links_count",
		"trace_state",
		"flags",
	}
}

// GetSpanColumnTypes returns the types of all columns in the span parquet schema.
func GetSpanColumnTypes() []string {
	return []string{
		"varchar",     // trace_id
		"varchar",     // span_id
		"varchar",     // parent_span_id
		"varchar",     // name
		"int32",       // kind
		"timestamptz", // start_time
		"timestamptz", // end_time
		"int64",       // duration_ns
		"int32",       // status_code
		"varchar",     // status_message
		"varchar",     // scope_name
		"varchar",     // scope_version
		"json",        // resource_attributes
		"json",        // span_attributes
		"json",        // events
		"json",        // links
		"uint32",      // dropped_attributes_count
		"uint32",      // dropped_events_count
		"uint32",      // dropped_links_count
		"varchar",     // trace_state
		"uint32",      // flags
	}
}
