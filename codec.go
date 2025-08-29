package fsm

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

type Codec interface {
	// Marshal marshals the given message.
	//
	// Marshal may expect a specific type of message, and will error if this type
	// is not given.
	Marshal(any) ([]byte, error)
	// Unmarshal unmarshals the given message.
	//
	// Unmarshal may expect a specific type of message, and will error if this
	// type is not given.
	Unmarshal([]byte, any) error
}

type protoBinaryCodec struct{}

var _ Codec = (*protoBinaryCodec)(nil)

func (c *protoBinaryCodec) Marshal(message any) ([]byte, error) {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T doesn't implement proto.Message", message)
	}
	return proto.Marshal(protoMessage)
}

func (c *protoBinaryCodec) Unmarshal(data []byte, message any) error {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return fmt.Errorf("%T doesn't implement proto.Message", message)
	}

	err := proto.Unmarshal(data, protoMessage)
	if err != nil {
		return fmt.Errorf("unmarshal into %T: %w", message, err)
	}
	return nil
}

type jsonCodec struct{}

var _ Codec = (*jsonCodec)(nil)

func (c *jsonCodec) Marshal(message any) ([]byte, error) {
	return json.Marshal(message)
}

func (c *jsonCodec) Unmarshal(data []byte, message any) error {
	return json.Unmarshal(data, message)
}
