package decoder

import (
	"encoding/binary"
)

// ParseConfluentHeader inspects data for the Confluent Schema Registry Wire Format:
// Byte 0: 0x00 (magic byte)
// Bytes 1-4: Big-Endian uint32 Schema ID
// Bytes 5+: Varint-encoded message index array (for Protobuf), followed by protobuf binary data.
// Returns schemaID, indexes, remaining payload slice, and true if magic byte matches.
func ParseConfluentHeader(data []byte) (schemaID uint32, indexes []int, payload []byte, ok bool) {
	if len(data) < 5 || data[0] != 0x00 {
		return 0, nil, nil, false
	}

	schemaID = binary.BigEndian.Uint32(data[1:5])
	rest := data[5:]

	if len(rest) == 0 {
		return schemaID, nil, rest, true
	}

	// In Confluent Protobuf Wire Format, bytes 5+ can encode an array of message indexes
	// or varint index count.
	// First varint is the length of the index array.
	// If varint length == 0, it means top-level message (index 0).
	indexCount, n := binary.Uvarint(rest)
	if n > 0 && int(indexCount) >= 0 && int(indexCount) < 32 {
		offset := n
		parsedIndexes := make([]int, 0, int(indexCount))
		valid := true
		for i := 0; i < int(indexCount); i++ {
			if offset >= len(rest) {
				valid = false
				break
			}
			idx, bytesRead := binary.Uvarint(rest[offset:])
			if bytesRead <= 0 {
				valid = false
				break
			}
			offset += bytesRead
			parsedIndexes = append(parsedIndexes, int(idx))
		}
		if valid && offset <= len(rest) {
			return schemaID, parsedIndexes, rest[offset:], true
		}
	}

	// Fallback: if varint index parsing fails (e.g. Avro or raw protobuf without index header),
	// return rest as raw payload.
	return schemaID, nil, rest, true
}
