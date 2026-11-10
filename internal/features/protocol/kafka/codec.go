package kafka

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

var (
	ErrUnexpectedEOF = errors.New("kafka protocol: unexpected end of buffer")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)
)

// ChecksumCRC32 calculates the CRC32-C (Castagnoli) checksum of the input data.
func ChecksumCRC32(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// Encoder writes binary Kafka wire protocol values.
type Encoder struct {
	w   io.Writer
	buf [8]byte
}

// NewEncoder creates a new Kafka binary encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

func (e *Encoder) PutInt8(v int8) {
	e.buf[0] = byte(v)
	_, _ = e.w.Write(e.buf[:1])
}

func (e *Encoder) PutInt16(v int16) {
	binary.BigEndian.PutUint16(e.buf[:2], uint16(v))
	_, _ = e.w.Write(e.buf[:2])
}

func (e *Encoder) PutInt32(v int32) {
	binary.BigEndian.PutUint32(e.buf[:4], uint32(v))
	_, _ = e.w.Write(e.buf[:4])
}

func (e *Encoder) PutInt64(v int64) {
	binary.BigEndian.PutUint64(e.buf[:8], uint64(v))
	_, _ = e.w.Write(e.buf[:8])
}

func (e *Encoder) PutString(s string) {
	e.PutInt16(int16(len(s)))
	if len(s) > 0 {
		_, _ = e.w.Write([]byte(s))
	}
}

func (e *Encoder) PutNullableString(s *string) {
	if s == nil {
		e.PutInt16(-1)
		return
	}
	e.PutString(*s)
}

func (e *Encoder) PutBytes(b []byte) {
	if b == nil {
		e.PutInt32(-1)
		return
	}
	e.PutInt32(int32(len(b)))
	if len(b) > 0 {
		_, _ = e.w.Write(b)
	}
}

func (e *Encoder) PutNullableBytes(b []byte) {
	e.PutBytes(b)
}

func (e *Encoder) PutRaw(b []byte) {
	_, _ = e.w.Write(b)
}

// PutVarint writes a signed 64-bit integer using zig-zag varint encoding.
func (e *Encoder) PutVarint(v int64) {
	ux := uint64((v << 1) ^ (v >> 63))
	e.PutUVarint(ux)
}

// PutUVarint writes an unsigned 64-bit integer using variable-length encoding.
func (e *Encoder) PutUVarint(v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, _ = e.w.Write(buf[:n])
}

// Decoder reads binary Kafka wire protocol values from a byte slice.
type Decoder struct {
	data []byte
	pos  int
}

// NewDecoder creates a new Kafka binary decoder.
func NewDecoder(data []byte) *Decoder {
	return &Decoder{data: data, pos: 0}
}

func (d *Decoder) Remaining() int {
	return len(d.data) - d.pos
}

func (d *Decoder) GetInt8() (int8, error) {
	if d.Remaining() < 1 {
		return 0, ErrUnexpectedEOF
	}
	v := int8(d.data[d.pos])
	d.pos++
	return v, nil
}

func (d *Decoder) GetInt16() (int16, error) {
	if d.Remaining() < 2 {
		return 0, ErrUnexpectedEOF
	}
	v := int16(binary.BigEndian.Uint16(d.data[d.pos : d.pos+2]))
	d.pos += 2
	return v, nil
}

func (d *Decoder) GetInt32() (int32, error) {
	if d.Remaining() < 4 {
		return 0, ErrUnexpectedEOF
	}
	v := int32(binary.BigEndian.Uint32(d.data[d.pos : d.pos+4]))
	d.pos += 4
	return v, nil
}

func (d *Decoder) GetInt64() (int64, error) {
	if d.Remaining() < 8 {
		return 0, ErrUnexpectedEOF
	}
	v := int64(binary.BigEndian.Uint64(d.data[d.pos : d.pos+8]))
	d.pos += 8
	return v, nil
}

func (d *Decoder) GetString() (string, error) {
	length, err := d.GetInt16()
	if err != nil {
		return "", err
	}
	if length <= 0 {
		return "", nil
	}
	if d.Remaining() < int(length) {
		return "", ErrUnexpectedEOF
	}
	s := string(d.data[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return s, nil
}

func (d *Decoder) GetNullableString() (*string, error) {
	length, err := d.GetInt16()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if length == 0 {
		empty := ""
		return &empty, nil
	}
	if d.Remaining() < int(length) {
		return nil, ErrUnexpectedEOF
	}
	s := string(d.data[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return &s, nil
}

func (d *Decoder) GetBytes() ([]byte, error) {
	length, err := d.GetInt32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if length == 0 {
		return []byte{}, nil
	}
	if d.Remaining() < int(length) {
		return nil, ErrUnexpectedEOF
	}
	b := make([]byte, length)
	copy(b, d.data[d.pos:d.pos+int(length)])
	d.pos += int(length)
	return b, nil
}

func (d *Decoder) GetNullableBytes() ([]byte, error) {
	return d.GetBytes()
}

func (d *Decoder) GetVarint() (int64, error) {
	ux, err := d.GetUVarint()
	if err != nil {
		return 0, err
	}
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, nil
}

func (d *Decoder) GetUVarint() (uint64, error) {
	ux, n := binary.Uvarint(d.data[d.pos:])
	if n <= 0 {
		return 0, ErrUnexpectedEOF
	}
	d.pos += n
	return ux, nil
}

func (d *Decoder) Slice(n int) ([]byte, error) {
	if d.Remaining() < n {
		return nil, ErrUnexpectedEOF
	}
	res := d.data[d.pos : d.pos+n]
	d.pos += n
	return res, nil
}

