// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package formatter

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNaNImprovements(t *testing.T) {
	t.Run("NaN becomes null in JSON", func(t *testing.T) {
		// Test that NaN values are properly converted to null in JSON
		obj := map[string]interface{}{
			"value": sanitizeFloat64(math.NaN()),
		}

		jsonBytes, err := json.Marshal(obj)
		require.NoError(t, err)

		// Should contain "null" not "NaN"
		assert.Contains(t, string(jsonBytes), `"value":null`)
		assert.NotContains(t, string(jsonBytes), `"value":"NaN"`)
		assert.NotContains(t, string(jsonBytes), `"value":0`)
	})

	t.Run("Infinity values are preserved as numbers", func(t *testing.T) {
		// Test that Infinity values are converted to large numbers
		obj := map[string]interface{}{
			"posInf": sanitizeFloat64(math.Inf(1)),
			"negInf": sanitizeFloat64(math.Inf(-1)),
		}

		jsonBytes, err := json.Marshal(obj)
		require.NoError(t, err)

		// Should contain large numbers, not "Infinity"
		assert.Contains(t, string(jsonBytes), `"posInf":1.7976931348623157e+308`)
		assert.Contains(t, string(jsonBytes), `"negInf":-1.7976931348623157e+308`)
		assert.NotContains(t, string(jsonBytes), `"Infinity"`)
	})

	t.Run("Normal values are unchanged", func(t *testing.T) {
		// Test that normal values are preserved
		obj := map[string]interface{}{
			"normal": sanitizeFloat64(42.5),
			"zero":   sanitizeFloat64(0.0),
			"neg":    sanitizeFloat64(-15.7),
		}

		jsonBytes, err := json.Marshal(obj)
		require.NoError(t, err)

		// Should contain the original values
		assert.Contains(t, string(jsonBytes), `"normal":42.5`)
		assert.Contains(t, string(jsonBytes), `"zero":0`)
		assert.Contains(t, string(jsonBytes), `"neg":-15.7`)
	})
}

func TestJSONSerializationWithNaN(t *testing.T) {
	// Test that a complete JSON object with NaN values can be serialized
	obj := map[string]interface{}{
		"metric_name": "test_metric",
		"value":       sanitizeFloat64(math.NaN()),
		"timestamp":   "2023-01-01T00:00:00Z",
		"attributes": map[string]interface{}{
			"service": "test-service",
			"version": "1.0.0",
		},
	}

	jsonBytes, err := json.Marshal(obj)
	require.NoError(t, err, "Should be able to serialize object with NaN values")

	// Verify the structure
	var result map[string]interface{}
	err = json.Unmarshal(jsonBytes, &result)
	require.NoError(t, err, "Should be able to deserialize the JSON")

	assert.Equal(t, "test_metric", result["metric_name"])
	assert.Nil(t, result["value"], "NaN should become null")
	assert.Equal(t, "2023-01-01T00:00:00Z", result["timestamp"])

	attributes, ok := result["attributes"].(map[string]interface{})
	require.True(t, ok, "Attributes should be a map")
	assert.Equal(t, "test-service", attributes["service"])
	assert.Equal(t, "1.0.0", attributes["version"])
}
