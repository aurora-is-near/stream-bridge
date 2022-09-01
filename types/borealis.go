package types

import "github.com/aurora-is-near/borealis.go"

func DecodeBorealisEvent[T any](data []byte) (*borealis.TypedEvent[T], error) {
	raw := new(borealis.BusMessage)
	raw.OverrideEventFactory(func(eventType borealis.MessageType) any {
		return new(T)
	})
	if err := raw.DecodeCBOR(data); err != nil {
		return nil, err
	}

	return borealis.CheckMessageTyped[T](raw)
}
