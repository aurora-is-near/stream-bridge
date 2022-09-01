package types

type AbstractBlock struct {
	Hash     string
	PrevHash string
	Height   uint64
}

func (ab *AuroraBlock) ToAbstractBlock() *AbstractBlock {
	return &AbstractBlock{
		Hash:     ab.Hash,
		PrevHash: ab.ParentHash,
		Height:   ab.Height,
	}
}

func (ab *NearBlock) ToAbstractBlock() *AbstractBlock {
	return &AbstractBlock{
		Hash:     ab.Hash,
		PrevHash: ab.PrevHash,
		Height:   ab.Height,
	}
}
