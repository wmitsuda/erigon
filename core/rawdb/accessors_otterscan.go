package rawdb

import "github.com/ledgerwatch/erigon/common"

type Spender struct {
	Spender common.Address `gencodec:"required"`
	Blocks  []uint64       `gencodec:"required"`
}

func NewSpender(spender common.Address) *Spender {
	return &Spender{
		Spender: spender,
		Blocks:  make([]uint64, 0, 1),
	}
}

type Spenders struct {
	Spenders []Spender `gencodec:"required"`
}
