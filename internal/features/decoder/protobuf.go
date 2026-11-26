package decoder

import (
	"fmt"
	"os"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// SchemaRegistry manages dynamically loaded protobuf message descriptors.
type SchemaRegistry struct {
	mu          sync.RWMutex
	descriptors []protoreflect.MessageDescriptor
	byFullName  map[string]protoreflect.MessageDescriptor
	byShortName map[string]protoreflect.MessageDescriptor
}

// NewSchemaRegistry instantiates an empty SchemaRegistry.
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		descriptors: make([]protoreflect.MessageDescriptor, 0),
		byFullName:  make(map[string]protoreflect.MessageDescriptor),
		byShortName: make(map[string]protoreflect.MessageDescriptor),
	}
}

// LoadDescriptorSet reads a compiled FileDescriptorSet (.desc) file from disk.
func (r *SchemaRegistry) LoadDescriptorSet(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read descriptor file %s: %w", path, err)
	}
	return r.LoadDescriptorSetBytes(raw)
}

// LoadDescriptorSetBytes parses binary FileDescriptorSet bytes and registers message descriptors.
func (r *SchemaRegistry) LoadDescriptorSetBytes(raw []byte) error {
	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &fds); err != nil {
		return fmt.Errorf("unmarshal FileDescriptorSet: %w", err)
	}

	files, err := protodesc.NewFiles(&fds)
	if err != nil {
		return fmt.Errorf("build protodesc files: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		r.registerMessages(fd.Messages())
		return true
	})

	return nil
}

func (r *SchemaRegistry) registerMessages(messages protoreflect.MessageDescriptors) {
	for i := 0; i < messages.Len(); i++ {
		desc := messages.Get(i)
		fullName := string(desc.FullName())
		shortName := string(desc.Name())

		r.descriptors = append(r.descriptors, desc)
		r.byFullName[fullName] = desc
		r.byShortName[shortName] = desc

		// Register nested messages recursively
		if desc.Messages().Len() > 0 {
			r.registerMessages(desc.Messages())
		}
	}
}

// FindDescriptor looks up a MessageDescriptor by full name or short name.
func (r *SchemaRegistry) FindDescriptor(name string) protoreflect.MessageDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if desc, ok := r.byFullName[name]; ok {
		return desc
	}
	if desc, ok := r.byShortName[name]; ok {
		return desc
	}
	return nil
}

// Descriptors returns all registered message descriptors.
func (r *SchemaRegistry) Descriptors() []protoreflect.MessageDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]protoreflect.MessageDescriptor, len(r.descriptors))
	copy(res, r.descriptors)
	return res
}

// DecodeMessage deserializes raw bytes into a dynamic message using the given descriptor,
// returning formatted JSON.
func (r *SchemaRegistry) DecodeMessage(desc protoreflect.MessageDescriptor, data []byte) (string, error) {
	if desc == nil {
		return "", fmt.Errorf("nil message descriptor")
	}
	msg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(data, msg); err != nil {
		return "", err
	}

	marshaler := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: false,
	}
	formatted, err := marshaler.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("protojson format: %w", err)
	}

	return string(formatted), nil
}

// TryDecodeAny attempts to decode data against all registered message descriptors.
// It ranks candidates to pick the best match (minimum unknown fields, maximum populated fields).
// Returns message name, formatted JSON string, and true on successful match.
func (r *SchemaRegistry) TryDecodeAny(data []byte) (messageName string, jsonStr string, ok bool) {
	r.mu.RLock()
	descs := r.descriptors
	r.mu.RUnlock()

	if len(descs) == 0 || len(data) == 0 {
		return "", "", false
	}

	type matchCandidate struct {
		desc      protoreflect.MessageDescriptor
		fields    int
		unknowns  int
		jsonBytes []byte
	}

	var candidates []matchCandidate

	for _, desc := range descs {
		msg := dynamicpb.NewMessage(desc)
		if err := proto.Unmarshal(data, msg); err == nil {
			var fieldCount int
			msg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
				fieldCount++
				return true
			})

			unknownCount := len(msg.ProtoReflect().GetUnknown())

			if fieldCount > 0 {
				marshaler := protojson.MarshalOptions{
					Multiline:       true,
					Indent:          "  ",
					EmitUnpopulated: false,
				}
				if jsonBytes, err := marshaler.Marshal(msg); err == nil {
					candidates = append(candidates, matchCandidate{
						desc:      desc,
						fields:    fieldCount,
						unknowns:  unknownCount,
						jsonBytes: jsonBytes,
					})
				}
			}
		}
	}

	if len(candidates) == 0 {
		return "", "", false
	}

	// Rank candidates: minimum unknowns, then maximum known fields
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.unknowns < best.unknowns {
			best = c
		} else if c.unknowns == best.unknowns && c.fields > best.fields {
			best = c
		}
	}

	return string(best.desc.Name()), string(best.jsonBytes), true
}
