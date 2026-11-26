//go:build ignore

package main

import (
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func main() {
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("order.proto"),
		Package: proto.String("ecommerce"),
		Syntax:  proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{
			{
				Name: proto.String("OrderStatus"),
				Value: []*descriptorpb.EnumValueDescriptorProto{
					{Name: proto.String("PENDING"), Number: proto.Int32(0)},
					{Name: proto.String("PROCESSING"), Number: proto.Int32(1)},
					{Name: proto.String("COMPLETED"), Number: proto.Int32(2)},
					{Name: proto.String("CANCELLED"), Number: proto.Int32(3)},
				},
			},
		},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("OrderItem"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("item_id"),
						Number: proto.Int32(1),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   proto.String("quantity"),
						Number: proto.Int32(2),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
					},
					{
						Name:   proto.String("price"),
						Number: proto.Int32(3),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum(),
					},
				},
			},
			{
				Name: proto.String("OrderEvent"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("order_id"),
						Number: proto.Int32(1),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   proto.String("customer_id"),
						Number: proto.Int32(2),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   proto.String("total_amount"),
						Number: proto.Int32(3),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum(),
					},
					{
						Name:     proto.String("status"),
						Number:   proto.Int32(4),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
						TypeName: proto.String(".ecommerce.OrderStatus"),
					},
					{
						Name:     proto.String("items"),
						Number:   proto.Int32(5),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						TypeName: proto.String(".ecommerce.OrderItem"),
					},
					{
						Name:   proto.String("timestamp_ms"),
						Number: proto.Int32(6),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
				},
			},
		},
	}

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fd},
	}

	raw, err := proto.Marshal(fds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to marshal FileDescriptorSet: %v\n", err)
		os.Exit(1)
	}

	outFile := "test/fixtures/schemas.desc"
	if err := os.WriteFile(outFile, raw, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", outFile, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully generated %s (%d bytes)\n", outFile, len(raw))
}
