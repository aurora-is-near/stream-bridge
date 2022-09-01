package types

type AuroraBlock struct {
	Hash       string `cbor:"hash" json:"hash"`
	ParentHash string `cbor:"parent_hash" json:"parent_hash"`
	Height     uint64 `cbor:"height" json:"height"`
}

func DecodeAuroraBlock(data []byte) (*AuroraBlock, error) {
	return DecodeBorealisPayload[AuroraBlock](data)
}
