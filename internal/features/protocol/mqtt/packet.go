package mqtt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MQTT Control Packet Types (MQTT v3.1.1 Section 2.2.1)
const (
	TypeConnect     byte = 1
	TypeConnack     byte = 2
	TypePublish     byte = 3
	TypePuback      byte = 4
	TypeSubscribe   byte = 8
	TypeSuback      byte = 9
	TypeUnsubscribe byte = 10
	TypeUnsuback    byte = 11
	TypePingreq     byte = 12
	TypePingresp    byte = 13
	TypeDisconnect  byte = 14
)

// Quality of Service Levels
const (
	QoS0 byte = 0
	QoS1 byte = 1
	QoS2 byte = 2
)

// CONNACK Return Codes (MQTT v3.1.1 Section 3.2.2.3)
const (
	ConnAccepted                      byte = 0x00
	ConnUnacceptableProtocolVersion   byte = 0x01
	ConnIdentifierRejected            byte = 0x02
	ConnServerUnavailable             byte = 0x03
	ConnBadUsernameOrPassword         byte = 0x04
	ConnNotAuthorized                 byte = 0x05
)

// SUBACK Return Codes (MQTT v3.1.1 Section 3.9.3)
const (
	SubackQoS0    byte = 0x00
	SubackQoS1    byte = 0x01
	SubackQoS2    byte = 0x02
	SubackFailure byte = 0x80
)

const (
	// MaxPacketSize limits memory allocation to 16 MB per packet.
	MaxPacketSize = 16 * 1024 * 1024
)

var (
	ErrMalformedRemainingLength = errors.New("malformed remaining length")
	ErrPacketTooLarge           = errors.New("packet exceeds maximum allowed size (16 MB)")
	ErrUnsupportedPacketType    = errors.New("unsupported or reserved mqtt packet type")
	ErrInvalidPacket            = errors.New("invalid or truncated mqtt packet")
)

// Packet represents an MQTT control packet.
type Packet interface {
	Type() byte
}

// EncodeRemainingLength encodes an integer into MQTT variable-length representation (1-4 bytes).
func EncodeRemainingLength(length int) []byte {
	var encoded []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		encoded = append(encoded, digit)
		if length == 0 {
			break
		}
	}
	return encoded
}

// DecodeRemainingLength decodes MQTT variable-length remaining length from reader.
// Returns (value, bytesRead, error).
func DecodeRemainingLength(r io.Reader) (int, int, error) {
	var (
		multiplier = 1
		value      = 0
		bytesRead  = 0
		buf        [1]byte
	)

	for {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, bytesRead, err
		}
		bytesRead++
		digit := buf[0]
		value += int(digit&0x7F) * multiplier
		if multiplier > 128*128*128 {
			return 0, bytesRead, ErrMalformedRemainingLength
		}
		multiplier *= 128
		if (digit & 0x80) == 0 {
			break
		}
	}

	return value, bytesRead, nil
}

func readString(buf *bytes.Buffer) (string, error) {
	if buf.Len() < 2 {
		return "", ErrInvalidPacket
	}
	var strLen uint16
	if err := binary.Read(buf, binary.BigEndian, &strLen); err != nil {
		return "", err
	}
	if buf.Len() < int(strLen) {
		return "", ErrInvalidPacket
	}
	data := make([]byte, strLen)
	if _, err := io.ReadFull(buf, data); err != nil {
		return "", err
	}
	return string(data), nil
}

func writeString(buf *bytes.Buffer, s string) {
	_ = binary.Write(buf, binary.BigEndian, uint16(len(s)))
	buf.WriteString(s)
}

// ----------------------------------------------------------------------------
// Packet Implementations
// ----------------------------------------------------------------------------

// ConnectPacket represents an MQTT CONNECT control packet.
type ConnectPacket struct {
	ProtocolName  string
	ProtocolLevel byte
	CleanSession  bool
	WillFlag      bool
	WillQoS       byte
	WillRetain    bool
	UsernameFlag  bool
	PasswordFlag  bool
	KeepAlive     uint16
	ClientID      string
	WillTopic     string
	WillMessage   []byte
	Username      string
	Password      string
}

func (p *ConnectPacket) Type() byte { return TypeConnect }

// ConnackPacket represents an MQTT CONNACK control packet.
type ConnackPacket struct {
	SessionPresent bool
	ReturnCode     byte
}

func (p *ConnackPacket) Type() byte { return TypeConnack }

// PublishPacket represents an MQTT PUBLISH control packet.
type PublishPacket struct {
	Dup      bool
	QoS      byte
	Retain   bool
	Topic    string
	PacketID uint16
	Payload  []byte
}

func (p *PublishPacket) Type() byte { return TypePublish }

// PubackPacket represents an MQTT PUBACK control packet.
type PubackPacket struct {
	PacketID uint16
}

func (p *PubackPacket) Type() byte { return TypePuback }

// TopicSubscription represents a topic filter with requested QoS.
type TopicSubscription struct {
	Topic string
	QoS   byte
}

// SubscribePacket represents an MQTT SUBSCRIBE control packet.
type SubscribePacket struct {
	PacketID      uint16
	Subscriptions []TopicSubscription
}

func (p *SubscribePacket) Type() byte { return TypeSubscribe }

// SubackPacket represents an MQTT SUBACK control packet.
type SubackPacket struct {
	PacketID    uint16
	ReturnCodes []byte
}

func (p *SubackPacket) Type() byte { return TypeSuback }

// UnsubscribePacket represents an MQTT UNSUBSCRIBE control packet.
type UnsubscribePacket struct {
	PacketID uint16
	Topics   []string
}

func (p *UnsubscribePacket) Type() byte { return TypeUnsubscribe }

// UnsubackPacket represents an MQTT UNSUBACK control packet.
type UnsubackPacket struct {
	PacketID uint16
}

func (p *UnsubackPacket) Type() byte { return TypeUnsuback }

// PingreqPacket represents an MQTT PINGREQ control packet.
type PingreqPacket struct{}

func (p *PingreqPacket) Type() byte { return TypePingreq }

// PingrespPacket represents an MQTT PINGRESP control packet.
type PingrespPacket struct{}

func (p *PingrespPacket) Type() byte { return TypePingresp }

// DisconnectPacket represents an MQTT DISCONNECT control packet.
type DisconnectPacket struct{}

func (p *DisconnectPacket) Type() byte { return TypeDisconnect }

// ----------------------------------------------------------------------------
// Read & Write Routing
// ----------------------------------------------------------------------------

// ReadPacket reads and deserializes a single complete MQTT control packet from reader.
func ReadPacket(r io.Reader) (Packet, error) {
	var header [1]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	pktType := header[0] >> 4
	flags := header[0] & 0x0F

	remLen, _, err := DecodeRemainingLength(r)
	if err != nil {
		return nil, err
	}
	if remLen > MaxPacketSize {
		return nil, ErrPacketTooLarge
	}

	body := make([]byte, remLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(body)

	switch pktType {
	case TypeConnect:
		protoName, err := readString(buf)
		if err != nil {
			return nil, err
		}
		if buf.Len() < 4 {
			return nil, ErrInvalidPacket
		}
		protoLevel, _ := buf.ReadByte()
		connFlags, _ := buf.ReadByte()
		var keepAlive uint16
		_ = binary.Read(buf, binary.BigEndian, &keepAlive)

		cleanSession := (connFlags & 0x02) != 0
		willFlag := (connFlags & 0x04) != 0
		willQoS := (connFlags >> 3) & 0x03
		willRetain := (connFlags & 0x20) != 0
		passwordFlag := (connFlags & 0x40) != 0
		usernameFlag := (connFlags & 0x80) != 0

		clientID, err := readString(buf)
		if err != nil {
			return nil, err
		}

		var willTopic string
		var willMsg []byte
		if willFlag {
			willTopic, err = readString(buf)
			if err != nil {
				return nil, err
			}
			msgStr, err := readString(buf)
			if err != nil {
				return nil, err
			}
			willMsg = []byte(msgStr)
		}

		var username, password string
		if usernameFlag {
			username, err = readString(buf)
			if err != nil {
				return nil, err
			}
		}
		if passwordFlag {
			password, err = readString(buf)
			if err != nil {
				return nil, err
			}
		}

		return &ConnectPacket{
			ProtocolName:  protoName,
			ProtocolLevel: protoLevel,
			CleanSession:  cleanSession,
			WillFlag:      willFlag,
			WillQoS:       willQoS,
			WillRetain:    willRetain,
			UsernameFlag:  usernameFlag,
			PasswordFlag:  passwordFlag,
			KeepAlive:     keepAlive,
			ClientID:      clientID,
			WillTopic:     willTopic,
			WillMessage:   willMsg,
			Username:      username,
			Password:      password,
		}, nil

	case TypeConnack:
		if buf.Len() < 2 {
			return nil, ErrInvalidPacket
		}
		flagsByte, _ := buf.ReadByte()
		rcByte, _ := buf.ReadByte()
		return &ConnackPacket{
			SessionPresent: (flagsByte & 0x01) != 0,
			ReturnCode:     rcByte,
		}, nil

	case TypePublish:
		topic, err := readString(buf)
		if err != nil {
			return nil, err
		}
		dup := (flags & 0x08) != 0
		qos := (flags >> 1) & 0x03
		retain := (flags & 0x01) != 0

		var packetID uint16
		if qos > QoS0 {
			if buf.Len() < 2 {
				return nil, ErrInvalidPacket
			}
			if err := binary.Read(buf, binary.BigEndian, &packetID); err != nil {
				return nil, err
			}
		}

		payload := buf.Bytes()
		return &PublishPacket{
			Dup:      dup,
			QoS:      qos,
			Retain:   retain,
			Topic:    topic,
			PacketID: packetID,
			Payload:  payload,
		}, nil

	case TypePuback:
		if buf.Len() < 2 {
			return nil, ErrInvalidPacket
		}
		var packetID uint16
		_ = binary.Read(buf, binary.BigEndian, &packetID)
		return &PubackPacket{PacketID: packetID}, nil

	case TypeSubscribe:
		if buf.Len() < 2 {
			return nil, ErrInvalidPacket
		}
		var packetID uint16
		_ = binary.Read(buf, binary.BigEndian, &packetID)

		var subs []TopicSubscription
		for buf.Len() > 0 {
			t, err := readString(buf)
			if err != nil {
				return nil, err
			}
			if buf.Len() < 1 {
				return nil, ErrInvalidPacket
			}
			q, _ := buf.ReadByte()
			subs = append(subs, TopicSubscription{Topic: t, QoS: q})
		}
		return &SubscribePacket{
			PacketID:      packetID,
			Subscriptions: subs,
		}, nil

	case TypeSuback:
		if buf.Len() < 2 {
			return nil, ErrInvalidPacket
		}
		var packetID uint16
		_ = binary.Read(buf, binary.BigEndian, &packetID)
		rcs := buf.Bytes()
		return &SubackPacket{
			PacketID:    packetID,
			ReturnCodes: rcs,
		}, nil

	case TypeUnsubscribe:
		if buf.Len() < 2 {
			return nil, ErrInvalidPacket
		}
		var packetID uint16
		_ = binary.Read(buf, binary.BigEndian, &packetID)

		var topics []string
		for buf.Len() > 0 {
			t, err := readString(buf)
			if err != nil {
				return nil, err
			}
			topics = append(topics, t)
		}
		return &UnsubscribePacket{
			PacketID: packetID,
			Topics:   topics,
		}, nil

	case TypeUnsuback:
		if buf.Len() < 2 {
			return nil, ErrInvalidPacket
		}
		var packetID uint16
		_ = binary.Read(buf, binary.BigEndian, &packetID)
		return &UnsubackPacket{PacketID: packetID}, nil

	case TypePingreq:
		return &PingreqPacket{}, nil

	case TypePingresp:
		return &PingrespPacket{}, nil

	case TypeDisconnect:
		return &DisconnectPacket{}, nil

	default:
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedPacketType, pktType)
	}
}

// WritePacket serializes and writes an MQTT control packet to writer.
func WritePacket(w io.Writer, p Packet) error {
	var (
		pktType byte = p.Type()
		flags   byte = 0
		body    = new(bytes.Buffer)
	)

	switch pkt := p.(type) {
	case *ConnectPacket:
		writeString(body, pkt.ProtocolName)
		body.WriteByte(pkt.ProtocolLevel)

		var connFlags byte
		if pkt.CleanSession {
			connFlags |= 0x02
		}
		if pkt.WillFlag {
			connFlags |= 0x04
			connFlags |= (pkt.WillQoS & 0x03) << 3
			if pkt.WillRetain {
				connFlags |= 0x20
			}
		}
		if pkt.Password != "" || pkt.PasswordFlag {
			connFlags |= 0x40
		}
		if pkt.Username != "" || pkt.UsernameFlag {
			connFlags |= 0x80
		}
		body.WriteByte(connFlags)
		_ = binary.Write(body, binary.BigEndian, pkt.KeepAlive)

		writeString(body, pkt.ClientID)
		if pkt.WillFlag {
			writeString(body, pkt.WillTopic)
			writeString(body, string(pkt.WillMessage))
		}
		if pkt.Username != "" || pkt.UsernameFlag {
			writeString(body, pkt.Username)
		}
		if pkt.Password != "" || pkt.PasswordFlag {
			writeString(body, pkt.Password)
		}

	case *ConnackPacket:
		var flagsByte byte
		if pkt.SessionPresent {
			flagsByte |= 0x01
		}
		body.WriteByte(flagsByte)
		body.WriteByte(pkt.ReturnCode)

	case *PublishPacket:
		if pkt.Dup {
			flags |= 0x08
		}
		flags |= (pkt.QoS & 0x03) << 1
		if pkt.Retain {
			flags |= 0x01
		}
		writeString(body, pkt.Topic)
		if pkt.QoS > QoS0 {
			_ = binary.Write(body, binary.BigEndian, pkt.PacketID)
		}
		body.Write(pkt.Payload)

	case *PubackPacket:
		_ = binary.Write(body, binary.BigEndian, pkt.PacketID)

	case *SubscribePacket:
		flags = 0x02 // MQTT 3.1.1 Section 3.8.1: Bits 3,2,1,0 are reserved and MUST be 0,0,1,0
		_ = binary.Write(body, binary.BigEndian, pkt.PacketID)
		for _, s := range pkt.Subscriptions {
			writeString(body, s.Topic)
			body.WriteByte(s.QoS & 0x03)
		}

	case *SubackPacket:
		_ = binary.Write(body, binary.BigEndian, pkt.PacketID)
		body.Write(pkt.ReturnCodes)

	case *UnsubscribePacket:
		flags = 0x02 // MQTT 3.1.1 Section 3.10.1: Bits 3,2,1,0 are reserved and MUST be 0,0,1,0
		_ = binary.Write(body, binary.BigEndian, pkt.PacketID)
		for _, t := range pkt.Topics {
			writeString(body, t)
		}

	case *UnsubackPacket:
		_ = binary.Write(body, binary.BigEndian, pkt.PacketID)

	case *PingreqPacket, *PingrespPacket, *DisconnectPacket:
		// No variable header or payload

	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedPacketType, p)
	}

	// First byte: (Type << 4) | Flags
	firstByte := (pktType << 4) | (flags & 0x0F)
	if _, err := w.Write([]byte{firstByte}); err != nil {
		return err
	}

	remLenBytes := EncodeRemainingLength(body.Len())
	if _, err := w.Write(remLenBytes); err != nil {
		return err
	}

	if body.Len() > 0 {
		if _, err := w.Write(body.Bytes()); err != nil {
			return err
		}
	}

	return nil
}
