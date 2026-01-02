// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package traceutil

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// TraceIDToHexOrEmptyString returns a hex string from TraceID.
// An empty string is returned if the TraceID is invalid.
func TraceIDToHexOrEmptyString(id pcommon.TraceID) string {
	if id.IsEmpty() {
		return ""
	}
	return id.String()
}

// SpanIDToHexOrEmptyString returns a hex string from SpanID.
// An empty string is returned if the SpanID is invalid.
func SpanIDToHexOrEmptyString(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return id.String()
}
