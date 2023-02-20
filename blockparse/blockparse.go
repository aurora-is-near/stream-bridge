package blockparse

import (
	"fmt"
	"strconv"

	"github.com/aurora-is-near/stream-bridge/types"
	"github.com/nats-io/nats.go"
)

type ParseBlockFn func(data []byte, header nats.Header) (*types.AbstractBlock, error)

func parseNearBlock(data []byte, header nats.Header) (*types.AbstractBlock, error) {
	block, err := types.DecodeNearBlock(data)
	if err != nil {
		return nil, err
	}
	return block.ToAbstractBlock(), nil
}

func ParseAuroraBlock(data []byte, header nats.Header) (*types.AbstractBlock, error) {
	block, err := types.DecodeAuroraBlock(data)
	if err != nil {
		return nil, err
	}
	return block.ToAbstractBlock(), nil
}

func parseUnverifiedBlock(data []byte, header nats.Header) (*types.AbstractBlock, error) {
	msgIdStr := header.Get(nats.MsgIdHdr)
	if len(msgIdStr) == 0 {
		return nil, fmt.Errorf("missing msg-id header")
	}
	msgId, err := strconv.ParseUint(msgIdStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse msg-id: %v", err)
	}
	return &types.AbstractBlock{Height: msgId}, nil
}

func GetParseBlockFn(mode string) (ParseBlockFn, error) {
	modes := map[string]ParseBlockFn{
		"near":       parseNearBlock,
		"aurora":     ParseAuroraBlock,
		"unverified": parseUnverifiedBlock,
	}
	if fn, ok := modes[mode]; ok {
		return fn, nil
	}

	keys := make([]string, 0, len(modes))
	for k := range modes {
		keys = append(keys, k)
	}
	return nil, fmt.Errorf("mode should be one of %v", keys)
}
