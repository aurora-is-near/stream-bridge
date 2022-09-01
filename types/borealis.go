package types

import (
	"bytes"
	"fmt"

	"github.com/aurora-is-near/borealis.go"
	"github.com/fxamacker/cbor/v2"
)

var cborDecMode cbor.DecMode

func getCborDecMode() (cbor.DecMode, error) {
	if cborDecMode != nil {
		return cborDecMode, nil
	}

	var err error
	cborDecMode, err = cbor.DecOptions{
		MaxArrayElements: 2147483647,
	}.DecMode()
	if err != nil {
		return nil, err
	}

	return cborDecMode, nil
}

func DecodeBorealisPayload[T any](data []byte) (*T, error) {
	reader := bytes.NewReader(data)

	var err error
	var version byte
	if version, err = reader.ReadByte(); err != nil {
		return nil, err
	}

	switch version {
	case borealis.V1MessageVersion:
		decMode, err := getCborDecMode()
		if err != nil {
			return nil, err
		}
		decoder := decMode.NewDecoder(reader)
		envelope := &borealis.Envelope{}
		if err := decoder.Decode(envelope); err != nil {
			return nil, err
		}
		payload := new(T)
		if err := decoder.Decode(payload); err != nil {
			return nil, err
		}
		return payload, nil
	default:
		return nil, fmt.Errorf("unknown version of borealis-message: %v", version)
	}
}
