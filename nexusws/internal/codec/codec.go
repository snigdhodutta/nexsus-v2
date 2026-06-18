// Package codec provides codec implementations for NexusWS.
package codec

import (
	"encoding/binary"
	"fmt"
)

// RawCodec is a zero-copy codec that treats payloads as raw bytes.
// This is the default and most efficient codec for binary data.
type RawCodec struct{}

// Ensure RawCodec implements nexusws.Codec.
var _ nexusws.Codec = (*RawCodec)(nil)

// NewRawCodec creates a new RawCodec instance.
func NewRawCodec() *RawCodec {
	return &RawCodec{}
}

// Encode returns the data as-is if it's already []byte, otherwise returns an error.
// For maximum efficiency, pass []byte directly to avoid allocation.
func (c *RawCodec) Encode(v any) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	// Support string as well without allocation
	if s, ok := v.(string); ok {
		return []byte(s), nil
	}
	return nil, fmt.Errorf("raw codec only supports []byte or string, got %T", v)
}

// Decode returns the data as-is. Caller is responsible for type assertion.
func (c *RawCodec) Decode(data []byte, v any) error {
	if ptr, ok := v.(*[]byte); ok {
		*ptr = data
		return nil
	}
	if ptr, ok := v.(*string); ok {
		*ptr = string(data)
		return nil
	}
	return fmt.Errorf("raw codec only decodes to *[]byte or *string, got %T", v)
}

// FrameEncoder encodes messages into the binary wire protocol.
// Format: [FrameType(1) | SubjectLen(2) | Subject | CorrelationID(8) | PayloadLen(4) | Payload]
type FrameEncoder struct{}

// NewFrameEncoder creates a new FrameEncoder.
func NewFrameEncoder() *FrameEncoder {
	return &FrameEncoder{}
}

// Encode encodes a Message into a byte slice using the binary wire protocol.
// The returned slice can be written directly to a WebSocket connection.
func (e *FrameEncoder) Encode(msg *nexusws.Message) ([]byte, error) {
	// Calculate total size
	subjectLen := len(msg.Subject)
	replyLen := len(msg.Reply)
	payloadLen := len(msg.Payload)

	// Total: FrameType(1) + SubjectLen(2) + Subject + ReplyLen(1) + Reply + CorrelationID(8) + PayloadLen(4) + Payload
	totalSize := 1 + 2 + subjectLen + 1 + replyLen + 8 + 4 + payloadLen

	buf := make([]byte, 0, totalSize)

	// FrameType (1 byte)
	buf = append(buf, byte(msg.Type))

	// SubjectLen (2 bytes, big-endian)
	buf = binary.BigEndian.AppendUint16(buf, uint16(subjectLen))

	// Subject (variable)
	buf = append(buf, msg.Subject...)

	// ReplyLen (1 byte) - included for flexibility
	buf = append(buf, byte(replyLen))

	// Reply (variable)
	buf = append(buf, msg.Reply...)

	// CorrelationID (8 bytes, big-endian)
	buf = binary.BigEndian.AppendUint64(buf, msg.CorrelationID)

	// PayloadLen (4 bytes, big-endian)
	buf = binary.BigEndian.AppendUint32(buf, uint32(payloadLen))

	// Payload (variable)
	buf = append(buf, msg.Payload...)

	return buf, nil
}

// FrameDecoder decodes messages from the binary wire protocol.
type FrameDecoder struct{}

// NewFrameDecoder creates a new FrameDecoder.
func NewFrameDecoder() *FrameDecoder {
	return &FrameDecoder{}
}

// Decode decodes a byte slice into a Message.
// Returns the number of bytes consumed and any error.
func (d *FrameDecoder) Decode(data []byte) (*nexusws.Message, int, error) {
	if len(data) < 1 {
		return nil, 0, fmt.Errorf("insufficient data for frame type")
	}

	idx := 0

	// FrameType (1 byte)
	frameType := nexusws.FrameType(data[idx])
	idx++

	// SubjectLen (2 bytes)
	if len(data) < idx+2 {
		return nil, 0, fmt.Errorf("insufficient data for subject length")
	}
	subjectLen := int(binary.BigEndian.Uint16(data[idx : idx+2]))
	idx += 2

	// Subject (variable)
	if len(data) < idx+subjectLen {
		return nil, 0, fmt.Errorf("insufficient data for subject")
	}
	subject := string(data[idx : idx+subjectLen])
	idx += subjectLen

	// ReplyLen (1 byte)
	if len(data) < idx+1 {
		return nil, 0, fmt.Errorf("insufficient data for reply length")
	}
	replyLen := int(data[idx])
	idx++

	// Reply (variable)
	if len(data) < idx+replyLen {
		return nil, 0, fmt.Errorf("insufficient data for reply")
	}
	reply := string(data[idx : idx+replyLen])
	idx += replyLen

	// CorrelationID (8 bytes)
	if len(data) < idx+8 {
		return nil, 0, fmt.Errorf("insufficient data for correlation ID")
	}
	correlationID := binary.BigEndian.Uint64(data[idx : idx+8])
	idx += 8

	// PayloadLen (4 bytes)
	if len(data) < idx+4 {
		return nil, 0, fmt.Errorf("insufficient data for payload length")
	}
	payloadLen := int(binary.BigEndian.Uint32(data[idx : idx+4]))
	idx += 4

	// Payload (variable)
	if len(data) < idx+payloadLen {
		return nil, 0, fmt.Errorf("insufficient data for payload")
	}
	payload := make([]byte, payloadLen)
	copy(payload, data[idx:idx+payloadLen])
	idx += payloadLen

	msg := &nexusws.Message{
		Type:          frameType,
		Subject:       subject,
		CorrelationID: correlationID,
		Payload:       payload,
		Reply:         reply,
		Timestamp:     0, // Set by caller if needed
	}

	return msg, idx, nil
}

// JSONCodec provides JSON serialization (for compatibility).
// Not recommended for performance-critical paths.
type JSONCodec struct{}

var _ nexusws.Codec = (*JSONCodec)(nil)

// NewJSONCodec creates a new JSONCodec.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Encode serializes v to JSON.
func (c *JSONCodec) Encode(v any) ([]byte, error) {
	// Will be implemented with encoding/json in full version
	return nil, fmt.Errorf("JSON codec not yet implemented")
}

// Decode deserializes JSON data into v.
func (c *JSONCodec) Decode(data []byte, v any) error {
	// Will be implemented with encoding/json in full version
	return fmt.Errorf("JSON codec not yet implemented")
}
