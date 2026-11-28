package mqtt

import (
	"errors"
	"strings"
)

var (
	ErrEmptyTopicName    = errors.New("topic name cannot be empty")
	ErrWildcardInName    = errors.New("topic name cannot contain wildcards (+ or #)")
	ErrNullCharInTopic   = errors.New("topic cannot contain null character")
	ErrEmptyTopicFilter  = errors.New("topic filter cannot be empty")
	ErrInvalidMultiWild  = errors.New("multi-level wildcard '#' must be at the end of topic filter")
	ErrInvalidSingleWild = errors.New("single-level wildcard '+' must occupy an entire level")
)

// ValidateTopicName validates that a topic name is valid for PUBLISH.
// It must not be empty, must not contain wildcards (+ or #), and must not contain null bytes.
func ValidateTopicName(name string) error {
	if name == "" {
		return ErrEmptyTopicName
	}
	if strings.ContainsRune(name, 0) {
		return ErrNullCharInTopic
	}
	if strings.ContainsAny(name, "+#") {
		return ErrWildcardInName
	}
	return nil
}

// ValidateTopicFilter validates an MQTT topic filter for SUBSCRIBE/UNSUBSCRIBE.
func ValidateTopicFilter(filter string) error {
	if filter == "" {
		return ErrEmptyTopicFilter
	}
	if strings.ContainsRune(filter, 0) {
		return ErrNullCharInTopic
	}

	levels := strings.Split(filter, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") {
			// # must be the sole character in the level and must be the last level
			if level != "#" || i != len(levels)-1 {
				return ErrInvalidMultiWild
			}
		}
		if strings.Contains(level, "+") {
			// + must be the sole character in the level
			if level != "+" {
				return ErrInvalidSingleWild
			}
		}
	}

	return nil
}

// MatchTopic checks if an MQTT published topic matches a subscription topic filter.
// Supports exact matching, single-level wildcard '+', and multi-level wildcard '#'.
func MatchTopic(filter, topic string) bool {
	if filter == "#" {
		return true
	}
	if filter == topic {
		return true
	}

	fLevels := strings.Split(filter, "/")
	tLevels := strings.Split(topic, "/")

	for i, f := range fLevels {
		if f == "#" {
			// # matches current and all deeper sub-levels (0 or more levels)
			return true
		}

		if i >= len(tLevels) {
			// Topic has fewer levels than filter
			return false
		}

		if f == "+" {
			// + matches any single topic level
			continue
		}

		if f != tLevels[i] {
			return false
		}
	}

	// If no '#' wildcard was reached, level count must match exactly
	return len(fLevels) == len(tLevels)
}
