package decoder

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"
)

// PayloadKind describes how the payload was classified and decoded.
type PayloadKind string

const (
	KindJSON      PayloadKind = "json"
	KindText      PayloadKind = "text"
	KindProtobuf  PayloadKind = "protobuf"
	KindConfluent PayloadKind = "confluent_protobuf"
	KindHexDump   PayloadKind = "hex_dump"
)

// DecodedPayload contains the human-readable representation and schema metadata.
type DecodedPayload struct {
	Kind        PayloadKind `json:"kind"`
	SchemaID    uint32      `json:"schema_id,omitempty"`
	MessageName string      `json:"message_name,omitempty"`
	Formatted   string      `json:"formatted"`
	IsBinary    bool        `json:"is_binary"`
	RawBytes    int         `json:"raw_bytes"`
}

// PayloadDecoder defines the interface for universal message decoding.
type PayloadDecoder interface {
	Decode(data []byte) DecodedPayload
	Registry() *SchemaRegistry
}

// UniversalDecoder implements the auto-detection decoding cascade.
type UniversalDecoder struct {
	registry *SchemaRegistry
}

// NewUniversalDecoder instantiates a UniversalDecoder.
func NewUniversalDecoder(reg *SchemaRegistry) *UniversalDecoder {
	return &UniversalDecoder{registry: reg}
}

// Registry returns the attached SchemaRegistry (may be nil).
func (u *UniversalDecoder) Registry() *SchemaRegistry {
	return u.registry
}

// Decode evaluates a payload through the auto-detection cascade.
func (u *UniversalDecoder) Decode(data []byte) DecodedPayload {
	if len(data) == 0 {
		return DecodedPayload{
			Kind:      KindText,
			Formatted: "",
			IsBinary:  false,
			RawBytes:  0,
		}
	}

	rawLen := len(data)

	// 1. Valid JSON Check
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) >= 2 {
		first := trimmed[0]
		last := trimmed[len(trimmed)-1]
		if (first == '{' && last == '}') || (first == '[' && last == ']') {
			var js any
			if err := json.Unmarshal(trimmed, &js); err == nil {
				if indented, err := json.MarshalIndent(js, "", "  "); err == nil {
					return DecodedPayload{
						Kind:      KindJSON,
						Formatted: string(indented),
						IsBinary:  false,
						RawBytes:  rawLen,
					}
				}
			}
		}
	}

	// 2. Confluent Wire Format Check (0x00 + 4-byte Schema ID)
	if data[0] == 0x00 && len(data) >= 5 {
		if schemaID, _, payload, ok := ParseConfluentHeader(data); ok {
			// If we have a schema registry, attempt dynamic protobuf deserialization
			if u.registry != nil && len(payload) > 0 {
				if msgName, jsonStr, decodedOk := u.registry.TryDecodeAny(payload); decodedOk {
					return DecodedPayload{
						Kind:        KindConfluent,
						SchemaID:    schemaID,
						MessageName: msgName,
						Formatted:   jsonStr,
						IsBinary:    true,
						RawBytes:    rawLen,
					}
				}
			}
			// Confluent header matched, but schema not found or decode failed:
			// Fallback to formatted hex dump of the payload slice
			return DecodedPayload{
				Kind:      KindConfluent,
				SchemaID:  schemaID,
				Formatted: FormatHexDump(payload),
				IsBinary:  true,
				RawBytes:  rawLen,
			}
		}
	}

	// 3. Raw Protobuf Check (if schema registry loaded)
	if u.registry != nil && len(data) > 0 {
		if msgName, jsonStr, decodedOk := u.registry.TryDecodeAny(data); decodedOk {
			return DecodedPayload{
				Kind:        KindProtobuf,
				MessageName: msgName,
				Formatted:   jsonStr,
				IsBinary:    true,
				RawBytes:    rawLen,
			}
		}
	}

	// 4. Printable UTF-8 Plain Text Check
	if utf8.Valid(data) && isPrintableText(data) {
		return DecodedPayload{
			Kind:      KindText,
			Formatted: string(data),
			IsBinary:  false,
			RawBytes:  rawLen,
		}
	}

	// 5. Fallback: Aligned 16-Byte Hex Dump
	return DecodedPayload{
		Kind:      KindHexDump,
		Formatted: FormatHexDump(data),
		IsBinary:  true,
		RawBytes:  rawLen,
	}
}

// isPrintableText checks if bytes consist predominantly of printable runes and whitespace.
func isPrintableText(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	printable := 0
	total := 0
	for len(data) > 0 {
		r, size := utf8.DecodeRune(data)
		data = data[size:]
		total++
		if (r >= 32 && r <= 126) || r == '\n' || r == '\r' || r == '\t' || r > 127 {
			printable++
		}
	}
	return total > 0 && (float64(printable)/float64(total)) >= 0.95
}
