// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package formatter

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestSanitizeFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected interface{}
	}{
		{
			name:     "normal value",
			input:    42.5,
			expected: 42.5,
		},
		{
			name:     "zero",
			input:    0.0,
			expected: 0.0,
		},
		{
			name:     "negative value",
			input:    -15.7,
			expected: -15.7,
		},
		{
			name:     "NaN becomes null",
			input:    math.NaN(),
			expected: nil, // JSON null
		},
		{
			name:     "positive infinity becomes max float64",
			input:    math.Inf(1),
			expected: math.MaxFloat64,
		},
		{
			name:     "negative infinity becomes min float64",
			input:    math.Inf(-1),
			expected: -math.MaxFloat64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFloat64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeFloat64JSONSerialization(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{
			name:     "normal value",
			input:    42.5,
			expected: `42.5`,
		},
		{
			name:     "NaN becomes null",
			input:    math.NaN(),
			expected: `null`,
		},
		{
			name:     "positive infinity becomes max float64",
			input:    math.Inf(1),
			expected: `1.7976931348623157e+308`,
		},
		{
			name:     "negative infinity becomes min float64",
			input:    math.Inf(-1),
			expected: `-1.7976931348623157e+308`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sanitized := sanitizeFloat64(tt.input)

			// Test that the sanitized value can be JSON marshaled
			obj := map[string]interface{}{
				"value": sanitized,
			}

			jsonBytes, err := json.Marshal(obj)
			require.NoError(t, err, "Failed to marshal sanitized value to JSON")

			// Verify the JSON contains the expected value
			assert.Contains(t, string(jsonBytes), tt.expected)
		})
	}
}

func TestGetValueWithNaN(t *testing.T) {
	tests := []struct {
		name       string
		intValue   int64
		floatValue float64
		valueType  pmetric.NumberDataPointValueType
		expected   interface{}
	}{
		{
			name:       "double NaN becomes null",
			intValue:   0,
			floatValue: math.NaN(),
			valueType:  pmetric.NumberDataPointValueTypeDouble,
			expected:   nil, // JSON null
		},
		{
			name:       "double infinity becomes max float64",
			intValue:   0,
			floatValue: math.Inf(1),
			valueType:  pmetric.NumberDataPointValueTypeDouble,
			expected:   math.MaxFloat64,
		},
		{
			name:       "double negative infinity becomes min float64",
			intValue:   0,
			floatValue: math.Inf(-1),
			valueType:  pmetric.NumberDataPointValueTypeDouble,
			expected:   -math.MaxFloat64,
		},
		{
			name:       "normal double value",
			intValue:   0,
			floatValue: 42.5,
			valueType:  pmetric.NumberDataPointValueTypeDouble,
			expected:   42.5,
		},
		{
			name:       "int value unchanged",
			intValue:   123,
			floatValue: 0,
			valueType:  pmetric.NumberDataPointValueTypeInt,
			expected:   123.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getValue(tt.intValue, tt.floatValue, tt.valueType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetExemplarValueWithNaN(t *testing.T) {
	tests := []struct {
		name       string
		intValue   int64
		floatValue float64
		valueType  pmetric.ExemplarValueType
		expected   interface{}
	}{
		{
			name:       "double NaN becomes null",
			intValue:   0,
			floatValue: math.NaN(),
			valueType:  pmetric.ExemplarValueTypeDouble,
			expected:   nil, // JSON null
		},
		{
			name:       "double infinity becomes max float64",
			intValue:   0,
			floatValue: math.Inf(1),
			valueType:  pmetric.ExemplarValueTypeDouble,
			expected:   math.MaxFloat64,
		},
		{
			name:       "normal double value",
			intValue:   0,
			floatValue: 42.5,
			valueType:  pmetric.ExemplarValueTypeDouble,
			expected:   42.5,
		},
		{
			name:       "int value unchanged",
			intValue:   123,
			floatValue: 0,
			valueType:  pmetric.ExemplarValueTypeInt,
			expected:   123.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getExemplarValue(tt.intValue, tt.floatValue, tt.valueType)
			assert.Equal(t, tt.expected, result)
		})
	}
}
