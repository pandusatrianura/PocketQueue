package decoder

import (
	"fmt"
	"strings"
)

// FormatHexDump returns an aligned 16-byte hex dump with offset and ASCII column.
// Matches standard hexdump -C formatting:
// 00000000  48 65 6c 6c 6f 20 57 6f  72 6c 64 21 00 00 00 01  |Hello World!....|
func FormatHexDump(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	var sb strings.Builder
	const bytesPerLine = 16

	for i := 0; i < len(data); i += bytesPerLine {
		end := i + bytesPerLine
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]

		// Line offset (8 hex chars + 2 spaces)
		fmt.Fprintf(&sb, "%08x  ", i)

		// Hex bytes column (8 bytes, space, 8 bytes, trailing space)
		for j := 0; j < bytesPerLine; j++ {
			if j == 8 {
				sb.WriteByte(' ')
			}
			if j < len(chunk) {
				fmt.Fprintf(&sb, "%02x ", chunk[j])
			} else {
				sb.WriteString("   ")
			}
		}

		// Two spaces before ASCII column
		sb.WriteString(" |")

		// ASCII column: printable ASCII (32-126) preserved, others replaced with '.'
		for _, b := range chunk {
			if b >= 32 && b <= 126 {
				sb.WriteByte(b)
			} else {
				sb.WriteByte('.')
			}
		}
		sb.WriteByte('|')

		if end < len(data) {
			sb.WriteByte('\n')
		}
	}

	return sb.String()
}
