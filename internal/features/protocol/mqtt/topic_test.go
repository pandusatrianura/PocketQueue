package mqtt_test

import (
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/protocol/mqtt"
)

func TestTopic_Match(t *testing.T) {
	testCases := []struct {
		filter   string
		topic    string
		expected bool
	}{
		// Exact matches
		{"sensors/temp", "sensors/temp", true},
		{"sensors/temp", "sensors/pressure", false},
		{"sensors", "sensors", true},
		{"sensors", "sensors/temp", false},
		{"sensors/temp", "sensors", false},
		{"Sensors/Temp", "sensors/temp", false}, // Case-sensitive

		// Multi-level wildcard '#'
		{"#", "sensors", true},
		{"#", "sensors/temp", true},
		{"#", "sensors/temp/room1/celsius", true},
		{"sensors/#", "sensors", true},
		{"sensors/#", "sensors/temp", true},
		{"sensors/#", "sensors/temp/room1", true},
		{"sensors/#", "finance/stocks", false},
		{"sensors/line1/#", "sensors/line1/temp", true},
		{"sensors/line1/#", "sensors/line2/temp", false},

		// Single-level wildcard '+'
		{"+", "sensors", true},
		{"+", "sensors/temp", false}, // '+' only matches 1 level
		{"sensors/+", "sensors/temp", true},
		{"sensors/+", "sensors/pressure", true},
		{"sensors/+", "sensors/temp/room1", false}, // more than 1 level
		{"sensors/+/temp", "sensors/room1/temp", true},
		{"sensors/+/temp", "sensors/room2/temp", true},
		{"sensors/+/temp", "sensors/room1/sub/temp", false},
		{"+/+", "sensors/temp", true},
		{"+/+", "sensors", false},
		{"+/+", "sensors/temp/celsius", false},

		// Combined wildcards
		{"sensors/+/line/#", "sensors/room1/line/temp", true},
		{"sensors/+/line/#", "sensors/room1/line", true},
		{"sensors/+/line/#", "sensors/room1/other/temp", false},
		{"+/+/+", "a/b/c", true},
		{"+/+/+", "a/b", false},

		// Leading / trailing slashes
		{"/sensors", "/sensors", true},
		{"/sensors", "sensors", false},
		{"sensors/", "sensors/", true},
		{"sensors/", "sensors", false},
	}

	for _, tc := range testCases {
		got := mqtt.MatchTopic(tc.filter, tc.topic)
		if got != tc.expected {
			t.Errorf("MatchTopic(%q, %q) = %v, expected %v", tc.filter, tc.topic, got, tc.expected)
		}
	}
}

func TestTopic_Validate(t *testing.T) {
	// Valid topic names
	validNames := []string{"sensors", "sensors/temp", "factory/1/status", "a/b/c/d/e"}
	for _, n := range validNames {
		if err := mqtt.ValidateTopicName(n); err != nil {
			t.Errorf("expected valid topic name %q, got error: %v", n, err)
		}
	}

	// Invalid topic names (cannot contain + or #, or be empty)
	invalidNames := []string{"", "sensors/+", "sensors/#", "+", "#", "sensors/+/temp"}
	for _, n := range invalidNames {
		if err := mqtt.ValidateTopicName(n); err == nil {
			t.Errorf("expected error for invalid topic name %q", n)
		}
	}

	// Valid topic filters
	validFilters := []string{"sensors", "sensors/temp", "+", "#", "sensors/+", "sensors/#", "sensors/+/temp/#"}
	for _, f := range validFilters {
		if err := mqtt.ValidateTopicFilter(f); err != nil {
			t.Errorf("expected valid topic filter %q, got error: %v", f, err)
		}
	}

	// Invalid topic filters (# not at the end or bad syntax)
	invalidFilters := []string{"", "sensors/#/temp", "sensors/a#"}
	for _, f := range invalidFilters {
		if err := mqtt.ValidateTopicFilter(f); err == nil {
			t.Errorf("expected error for invalid topic filter %q", f)
		}
	}
}
