package decoder_test

import (
	"strings"
	"testing"

	"github.com/pandusatrianura/pocketqueue/internal/features/decoder"
)

func TestHexDump_Empty(t *testing.T) {
	result := decoder.FormatHexDump(nil)
	if result != "" {
		t.Errorf("expected empty string for nil data, got: %q", result)
	}
	result = decoder.FormatHexDump([]byte{})
	if result != "" {
		t.Errorf("expected empty string for empty slice, got: %q", result)
	}
}

func TestHexDump_SingleRowExact16(t *testing.T) {
	data := []byte("Hello, World! 42") // exactly 16 bytes
	dump := decoder.FormatHexDump(data)

	if !strings.HasPrefix(dump, "00000000  ") {
		t.Errorf("expected dump to start with 00000000 offset, got: %s", dump)
	}
	if !strings.HasSuffix(dump, "|Hello, World! 42|") {
		t.Errorf("expected dump to end with ASCII sidebar, got: %s", dump)
	}
}

func TestHexDump_PartialRowPadding(t *testing.T) {
	data := []byte("Hello") // 5 bytes
	dump := decoder.FormatHexDump(data)

	expectedPrefix := "00000000  48 65 6c 6c 6f"
	if !strings.HasPrefix(dump, expectedPrefix) {
		t.Errorf("expected dump prefix %q, got: %s", expectedPrefix, dump)
	}
	if !strings.HasSuffix(dump, "|Hello|") {
		t.Errorf("expected ASCII sidebar |Hello|, got: %s", dump)
	}
}

func TestHexDump_NonPrintableCharacters(t *testing.T) {
	data := []byte{0x00, 0x01, 0x02, 0x41, 0x0a, 0x7f, 0xff}
	dump := decoder.FormatHexDump(data)

	// Non-printables should be dots '.', 'A' (0x41) should be 'A'
	if !strings.HasSuffix(dump, "|...A...|") {
		t.Errorf("expected non-printables replaced with dots, got: %s", dump)
	}
}

func TestHexDump_MultiRow(t *testing.T) {
	data := make([]byte, 35)
	for i := range data {
		data[i] = byte(i + 1)
	}
	dump := decoder.FormatHexDump(data)
	lines := strings.Split(strings.TrimRight(dump, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 rows for 35 bytes, got %d rows", len(lines))
	}
	if !strings.HasPrefix(lines[0], "00000000  ") {
		t.Errorf("row 0 offset mismatch: %s", lines[0])
	}
	if !strings.HasPrefix(lines[1], "00000010  ") {
		t.Errorf("row 1 offset mismatch: %s", lines[1])
	}
	if !strings.HasPrefix(lines[2], "00000020  ") {
		t.Errorf("row 2 offset mismatch: %s", lines[2])
	}
}
